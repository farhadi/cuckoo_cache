%%%-------------------------------------------------------------------
%% @doc High-performance concurrent in-memory cache with the ability
%% to detect one-hit-wonders using a Cuckoo Filter.
%% @end
%%%-------------------------------------------------------------------

-module(cuckoo_cache).

-export([
    new/1, new/2,
    get/2,
    put/3, put/4,
    fetch/3, fetch/4,
    delete/2,
    capacity/1,
    filter_size/1,
    size/1
]).

-include("cuckoo_cache.hrl").

-type cuckoo_cache() :: #cuckoo_cache{}.

-export_type([cuckoo_cache/0]).

-type hash() :: non_neg_integer().
-type fingerprint() :: pos_integer().
-type fingerprint_size() :: 4 | 8 | 16 | 32 | 64.
-type hash_function() :: fun((binary()) -> hash()).
-type key() :: term().
-type value() :: term().
-type fallback_fun() :: fun((key()) -> value()).
-type cache_name() :: term().
-type options() :: [option()].
-type option() ::
    {name, term()}
    | {ttl, timeout()}
    | {fingerprint_size, fingerprint_size()}
    | {bucket_size, pos_integer()}
    | {hash_function, hash_function()}.

%% Default configurations
-define(DEFAULT_FINGERPRINT_SIZE, 32).
-define(DEFAULT_BUCKET_SIZE, 4).
-define(DEFAULT_TTL, infinity).

-define(CACHE(Name), persistent_term:get({?MODULE, Name})).
-define(NOW, erlang:monotonic_time(millisecond)).

%% @equiv new(Capacity, [])
-spec new(pos_integer()) -> cuckoo_cache().
new(Capacity) ->
    new(Capacity, []).

%% @doc Creates a new cuckoo cache with the given capacity and options.
%%
%% A cuckoo cache has a built-in cuckoo filter to detect one-hit-wonders
%% and an ets table to keep cached entries.
%%
%% Given capacity is used as the capacity for the built-in cuckoo filter.
%% Note that the actual capacity might be higher than the given capacity,
%% because internally number of buckets in a cuckoo filter must be a power of 2.
%%
%% The capacity of the ets table depends on the ratio of one-hit-wonders to
%% the capacity of the filter, and is limited by the fingerprint key space.
%% The size of ets table can not exceed the capacity of the cuckoo filter.
%%
%% Possible options are:
%% <ul>
%% <li>`{name, Name}'
%% <p>If you give it a name, created cache instance will be stored in
%% persistent_term, and later you can access the cache by its name.</p>
%% </li>
%% <li>`{ttl, TTL}'
%% <p>Default TTL for cache entries in millisecond. If not specified,
%% `infinity' is used. When storing an entry, TTL can be given per entry,
%% but if it is not, this default TTL is used.</p>
%% </li>
%% <li>`{fingerprint_size, FingerprintSize}'
%% <p>This is the fingerprint size of the built-in cuckoo filter and can be
%% one of 4, 8, 16, 32, and 64 bits. Default fingerprint size is 16 bits.</p>
%% </li>
%% <li>`{bucket_size, BucketSize}'
%% <p>BucketSize of the built-in cuckoo filter must be a non negative integer,
%% and the default value is 4. Higher bucket sizes increase false positive rate.</p>
%% </li>
%% <li>`{hash_function, HashFunction}'
%% <p> You can specify a custom hash function for the built-in cuckoo filter
%% that accepts a binary as argument and returns hash value as an integer.
%% By default xxh3 hash functions are used, and you need to manually add `xxh3'
%% to the list of your project dependencies.</p>
%% </li>
%% </ul>
-spec new(pos_integer(), options()) -> cuckoo_cache().
new(Capacity, Opts) ->
    is_integer(Capacity) andalso Capacity > 0 orelse error(badarg),
    BucketSize = proplists:get_value(bucket_size, Opts, ?DEFAULT_BUCKET_SIZE),
    is_integer(BucketSize) andalso BucketSize > 0 orelse error(badarg),
    FingerprintSize = proplists:get_value(fingerprint_size, Opts, ?DEFAULT_FINGERPRINT_SIZE),
    lists:member(FingerprintSize, [4, 8, 16, 32, 64]) orelse error(badarg),
    TTL = proplists:get_value(ttl, Opts, ?DEFAULT_TTL),
    (is_integer(TTL) andalso TTL > 0 orelse TTL == infinity) orelse error(badarg),
    HashFunction = proplists:get_value(
        hash_function,
        Opts,
        default_hash_function(BucketSize + FingerprintSize)
    ),
    NumBuckets = 1 bsl ceil(math:log2(ceil(Capacity / BucketSize))),
    MaxHash = NumBuckets bsl FingerprintSize - 1,
    AtomicsSize = ceil(NumBuckets * BucketSize * FingerprintSize / 64) + 1,

    Table = ets:new(?MODULE, [
        ordered_set,
        public,
        {write_concurrency, true},
        {read_concurrency, true}
    ]),

    CuckooCache = #cuckoo_cache{
        table = Table,
        ttl = TTL,
        buckets = atomics:new(AtomicsSize, [{signed, false}]),
        num_buckets = NumBuckets,
        max_hash = MaxHash,
        bucket_size = BucketSize,
        fingerprint_size = FingerprintSize,
        hash_function = HashFunction
    },

    case proplists:get_value(name, Opts) of
        undefined ->
            CuckooCache;
        Name ->
            persistent_term:put({?MODULE, Name}, CuckooCache),
            CuckooCache
    end.

%% @doc Retrieves an entry from a cache.
%%
%% If entry exists in the cache `{ok, Value}' is returned, otherwise
%% `{error, not_found}' is returned.
-spec get(cuckoo_cache() | cache_name(), key()) -> {ok, value()} | {error, not_found}.
get(Cache = #cuckoo_cache{table = Table, fingerprint_size = FingerprintSize}, Key) ->
    Hash = hash(Cache, Key),
    Fingerprint = fingerprint(Hash, FingerprintSize),
    case contains_hash(Cache, Hash) of
        true -> lookup_cache(Table, Fingerprint, Key);
        false -> {error, not_found}
    end;
get(Name, Key) ->
    get(?CACHE(Name), Key).

%% @equiv put(Cache, Key, Value, Cache#cuckoo_cache.ttl)
-spec put(cuckoo_cache() | cache_name(), key(), value()) -> ok.
put(Cache = #cuckoo_cache{ttl = TTL}, Key, Value) ->
    put(Cache, Key, Value, TTL);
put(Name, Key, Value) ->
    put(?CACHE(Name), Key, Value).

%% @doc Places an entry in a cache.
%%
%% This will add the entry to both the cuckoo filter and the ets table.
%%
%% If there is not enough space in the filter a random element gets removed.
-spec put(cuckoo_cache() | cache_name(), key(), value(), timeout()) -> ok.
put(Cache = #cuckoo_cache{}, Key, Value, infinity) ->
    do_put(Cache, Key, Value, infinity);
put(Cache = #cuckoo_cache{}, Key, Value, TTL) ->
    do_put(Cache, Key, Value, ?NOW + TTL);
put(Name, Key, Value, TTL) ->
    put(?CACHE(Name), Key, Value, TTL).

%% @equiv fetch(Cache, Key, Fallback, Cache#cuckoo_cache.ttl)
-spec fetch(cuckoo_cache() | cache_name(), key(), fallback_fun()) -> value().
fetch(Cache = #cuckoo_cache{ttl = TTL}, Key, Fallback) ->
    fetch(Cache, Key, Fallback, TTL);
fetch(Name, Key, Fallback) ->
    fetch(?CACHE(Name), Key, Fallback).

%% @doc Fetches an entry from a cache, generating a value on cache miss.
%%
%% If the requested entry is found in the cache, cached value is returned.
%% If the entry is not contained in the cache, the provided fallback function
%% will be executed and its result is returned.
%%
%% If the entry does not exist in the cuckoo filter, the returned value from
%% fallback function will not be cached in the ets table, but the key will be
%% added to the cuckoo filter.
%%
%% If the entry exists in the cuckoo filter, the returned value from fallback
%% function will be cached in the ets table.
%%
%% When a new entry is added to the cuckoo filter, if cuckoo filter does not
%% have enough space, a random element gets removed from both the filter and
%% the ets table.
-spec fetch(cuckoo_cache() | cache_name(), key(), fallback_fun(), timeout()) -> value().
fetch(Cache = #cuckoo_cache{}, Key, Fallback, infinity) ->
    do_fetch(Cache, Key, Fallback, infinity);
fetch(Cache = #cuckoo_cache{}, Key, Fallback, TTL) ->
    do_fetch(Cache, Key, Fallback, ?NOW + TTL);
fetch(Name, Key, Fallback, TTL) ->
    fetch(?CACHE(Name), Key, Fallback, TTL).

%% @doc Removes an entry from a cache.
%%
%% This removes the entry from both the cuckoo filter and the ets table.
%%
%% Returns `ok' if the deletion was successful, and returns {error, not_found}
%% if the element could not be found in the cuckoo filter.
-spec delete(cuckoo_cache() | cache_name(), key()) -> ok | {error, not_found}.
delete(Cache = #cuckoo_cache{table = Table, fingerprint_size = FingerprintSize}, Key) ->
    Hash = hash(Cache, Key),
    Fingerprint = fingerprint(Hash, FingerprintSize),
    ets:delete(Table, Fingerprint),
    delete_hash(Cache, Hash);
delete(Name, Key) ->
    delete(?CACHE(Name), Key).

%% @doc Returns the maximum capacity of the cuckoo filter in a cache.
-spec capacity(cuckoo_cache() | cache_name()) -> pos_integer().
capacity(#cuckoo_cache{bucket_size = BucketSize, num_buckets = NumBuckets}) ->
    NumBuckets * BucketSize;
capacity(Name) ->
    capacity(?CACHE(Name)).

%% @doc Returns number of items in the cuckoo filter of a cache.
-spec filter_size(cuckoo_cache() | cache_name()) -> non_neg_integer().
filter_size(#cuckoo_cache{buckets = Buckets}) ->
    atomics:get(Buckets, 1);
filter_size(Name) ->
    filter_size(?CACHE(Name)).

%% @doc Returns number of items in the ets table of a cache.
-spec size(cuckoo_cache() | cache_name()) -> non_neg_integer().
size(#cuckoo_cache{table = Table}) ->
    ets:info(Table, size);
size(Name) ->
    cuckoo_cache:size(?CACHE(Name)).

%%%-------------------------------------------------------------------
%% Internal functions
%%%-------------------------------------------------------------------

-spec add_hash(cuckoo_cache(), hash()) -> ok.
add_hash(
    Cache = #cuckoo_cache{
        fingerprint_size = FingerprintSize,
        num_buckets = NumBuckets,
        hash_function = HashFunction
    },
    Hash
) ->
    {Index, Fingerprint} = index_and_fingerprint(Hash, FingerprintSize),
    case insert_at_index(Cache, Index, Fingerprint) of
        ok ->
            ok;
        {error, full} ->
            AltIndex = alt_index(Index, Fingerprint, NumBuckets, HashFunction),
            case insert_at_index(Cache, AltIndex, Fingerprint) of
                ok ->
                    ok;
                {error, full} ->
                    RandIndex = element(rand:uniform(2), {Index, AltIndex}),
                    force_insert(Cache, RandIndex, Fingerprint)
            end
    end.

-spec contains_hash(cuckoo_cache(), hash()) -> boolean().
contains_hash(Cache = #cuckoo_cache{fingerprint_size = FingerprintSize}, Hash) ->
    {Index, Fingerprint} = index_and_fingerprint(Hash, FingerprintSize),
    lookup_index(Cache, Index, Fingerprint).

-spec delete_hash(cuckoo_cache(), hash()) -> ok | {error, not_found}.
delete_hash(
    Cache = #cuckoo_cache{
        fingerprint_size = FingerprintSize,
        num_buckets = NumBuckets,
        hash_function = HashFunction
    },
    Hash
) ->
    {Index, Fingerprint} = index_and_fingerprint(Hash, FingerprintSize),
    case delete_fingerprint(Cache, Fingerprint, Index) of
        ok ->
            ok;
        {error, not_found} ->
            AltIndex = alt_index(Index, Fingerprint, NumBuckets, HashFunction),
            delete_fingerprint(Cache, Fingerprint, AltIndex)
    end.

-spec hash(cuckoo_cache(), key()) -> hash().
hash(#cuckoo_cache{max_hash = MaxHash, hash_function = HashFunction}, Key) when is_binary(Key) ->
    HashFunction(Key) band MaxHash;
hash(Cache, Key) ->
    hash(Cache, term_to_binary(Key)).

-spec lookup_cache(ets:tid(), fingerprint(), key()) -> {ok, value()} | {error, not_found}.
lookup_cache(Table, Fingerprint, Key) ->
    case ets:lookup(Table, Fingerprint) of
        [{Fingerprint, Key, Value, infinity}] ->
            {ok, Value};
        [{Fingerprint, Key, Value, Expiry}] ->
            case Expiry > ?NOW of
                true -> {ok, Value};
                false -> {error, not_found}
            end;
        _ ->
            {error, not_found}
    end.

-spec do_put(cuckoo_cache(), key(), value(), timeout()) -> ok.
do_put(
    Cache = #cuckoo_cache{table = Table, fingerprint_size = FingerprintSize},
    Key,
    Value,
    Expiry
) ->
    Hash = hash(Cache, Key),
    Fingerprint = fingerprint(Hash, FingerprintSize),
    add_hash(Cache, Hash),
    ets:insert(Table, {Fingerprint, Key, Value, Expiry}),
    ok.

-spec do_fetch(cuckoo_cache(), key(), fallback_fun(), timeout()) -> value().
do_fetch(
    Cache = #cuckoo_cache{table = Table, fingerprint_size = FingerprintSize},
    Key,
    Fallback,
    Expiry
) ->
    Hash = hash(Cache, Key),
    case contains_hash(Cache, Hash) of
        true ->
            add_hash(Cache, Hash),
            Fingerprint = fingerprint(Hash, FingerprintSize),
            case lookup_cache(Table, Fingerprint, Key) of
                {ok, Value} ->
                    Value;
                {error, not_found} ->
                    Value = Fallback(Key),
                    ets:insert(Table, {Fingerprint, Key, Value, Expiry}),
                    Value
            end;
        false ->
            add_hash(Cache, Hash),
            Fallback(Key)
    end.

-spec default_hash_function(fingerprint_size()) -> hash_function().
default_hash_function(Size) when Size > 64 ->
    fun xxh3:hash128/1;
default_hash_function(_Size) ->
    fun xxh3:hash64/1.

-spec lookup_index(cuckoo_cache(), non_neg_integer(), fingerprint()) -> boolean().
lookup_index(
    Cache = #cuckoo_cache{num_buckets = NumBuckets, hash_function = HashFunction},
    Index,
    Fingerprint
) ->
    Bucket = read_bucket(Index, Cache),
    case lists:member(Fingerprint, Bucket) of
        true ->
            true;
        false ->
            AltIndex = alt_index(Index, Fingerprint, NumBuckets, HashFunction),
            AltBucket = read_bucket(AltIndex, Cache),
            lists:member(Fingerprint, AltBucket)
    end.

-spec delete_fingerprint(cuckoo_cache(), fingerprint(), non_neg_integer()) ->
    ok | {error, not_found}.
delete_fingerprint(Cache, Fingerprint, Index) ->
    Bucket = read_bucket(Index, Cache),
    case find_in_bucket(Bucket, Fingerprint) of
        {ok, SubIndex} ->
            case update_in_bucket(Cache, Index, SubIndex, Fingerprint, 0) of
                ok ->
                    ok;
                {error, outdated} ->
                    delete_fingerprint(Cache, Fingerprint, Index)
            end;
        {error, not_found} ->
            {error, not_found}
    end.

-spec fingerprint(hash(), fingerprint()) -> fingerprint().
fingerprint(Hash, FingerprintSize) ->
    case Hash rem (1 bsl FingerprintSize) of
        0 ->
            1;
        Fingerprint ->
            Fingerprint
    end.

-spec index_and_fingerprint(hash(), fingerprint_size()) -> {non_neg_integer(), fingerprint()}.
index_and_fingerprint(Hash, FingerprintSize) ->
    Fingerprint = fingerprint(Hash, FingerprintSize),
    Index = Hash bsr FingerprintSize,
    {Index, Fingerprint}.

-spec alt_index(non_neg_integer(), fingerprint(), pos_integer(), hash_function()) ->
    non_neg_integer().
alt_index(Index, Fingerprint, NumBuckets, HashFunction) ->
    Index bxor HashFunction(binary:encode_unsigned(Fingerprint)) rem NumBuckets.

atomic_index(BitIndex) ->
    BitIndex div 64 + 2.

-spec insert_at_index(cuckoo_cache(), non_neg_integer(), fingerprint()) -> ok | {error, full}.
insert_at_index(Cache, Index, Fingerprint) ->
    Bucket = read_bucket(Index, Cache),
    case find_in_bucket(Bucket, 0) of
        {ok, SubIndex} ->
            case update_in_bucket(Cache, Index, SubIndex, 0, Fingerprint) of
                ok ->
                    ok;
                {error, outdated} ->
                    insert_at_index(Cache, Index, Fingerprint)
            end;
        {error, not_found} ->
            {error, full}
    end.

-spec force_insert(cuckoo_cache(), non_neg_integer(), fingerprint()) -> ok.
force_insert(Cache = #cuckoo_cache{table = Table, bucket_size = BucketSize}, Index, Fingerprint) ->
    Bucket = read_bucket(Index, Cache),
    SubIndex = rand:uniform(BucketSize) - 1,
    case lists:nth(SubIndex + 1, Bucket) of
        0 ->
            case insert_at_index(Cache, Index, Fingerprint) of
                ok ->
                    ok;
                {error, full} ->
                    force_insert(Cache, Index, Fingerprint)
            end;
        Fingerprint ->
            ok;
        Evicted ->
            case update_in_bucket(Cache, Index, SubIndex, Evicted, Fingerprint) of
                ok ->
                    case lookup_index(Cache, Index, Evicted) of
                        true ->
                            ok;
                        false ->
                            ets:delete(Table, Evicted),
                            ok
                    end;
                {error, outdated} ->
                    force_insert(Cache, Index, Fingerprint)
            end
    end.

-spec find_in_bucket([fingerprint()], fingerprint()) -> non_neg_integer().
find_in_bucket(Bucket, Fingerprint) ->
    find_in_bucket(Bucket, Fingerprint, 0).

find_in_bucket([], _Fingerprint, _Index) ->
    {error, not_found};
find_in_bucket([Fingerprint | _Bucket], Fingerprint, Index) ->
    {ok, Index};
find_in_bucket([_ | Bucket], Fingerprint, Index) ->
    find_in_bucket(Bucket, Fingerprint, Index + 1).

-spec read_bucket(non_neg_integer(), cuckoo_cache()) -> [fingerprint()].
read_bucket(
    Index,
    #cuckoo_cache{
        buckets = Buckets,
        bucket_size = BucketSize,
        fingerprint_size = FingerprintSize
    }
) ->
    BucketBitSize = BucketSize * FingerprintSize,
    BitIndex = Index * BucketBitSize,
    AtomicIndex = atomic_index(BitIndex),
    SkipBits = BitIndex rem 64,
    EndIndex = atomic_index(BitIndex + BucketBitSize - 1),
    <<_:SkipBits, Bucket:BucketBitSize/bitstring, _/bitstring>> = <<
        <<(atomics:get(Buckets, I)):64/big-unsigned-integer>>
     || I <- lists:seq(AtomicIndex, EndIndex)
    >>,
    [F || <<F:FingerprintSize/big-unsigned-integer>> <= Bucket].

-spec update_in_bucket(
    cuckoo_cache(),
    non_neg_integer(),
    non_neg_integer(),
    fingerprint(),
    fingerprint()
) -> ok | {error, outdated}.
update_in_bucket(
    Cache = #cuckoo_cache{
        buckets = Buckets,
        bucket_size = BucketSize,
        fingerprint_size = FingerprintSize
    },
    Index,
    SubIndex,
    OldValue,
    Value
) ->
    BitIndex = Index * BucketSize * FingerprintSize + SubIndex * FingerprintSize,
    AtomicIndex = atomic_index(BitIndex),
    SkipBits = BitIndex rem 64,
    AtomicValue = atomics:get(Buckets, AtomicIndex),
    case <<AtomicValue:64/big-unsigned-integer>> of
        <<Prefix:SkipBits/bitstring, OldValue:FingerprintSize/big-unsigned-integer,
            Suffix/bitstring>> ->
            <<UpdatedAtomic:64/big-unsigned-integer>> =
                <<Prefix/bitstring, Value:FingerprintSize/big-unsigned-integer, Suffix/bitstring>>,
            case atomics:compare_exchange(Buckets, AtomicIndex, AtomicValue, UpdatedAtomic) of
                ok ->
                    case {OldValue, Value} of
                        {0, _} -> atomics:add(Buckets, 1, 1);
                        {_, 0} -> atomics:sub(Buckets, 1, 1);
                        {_, _} -> ok
                    end;
                _ ->
                    update_in_bucket(Cache, Index, SubIndex, OldValue, Value)
            end;
        _ ->
            {error, outdated}
    end.
