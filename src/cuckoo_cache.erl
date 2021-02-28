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

-include_lib("cuckoo_filter/src/cuckoo_filter.hrl").
-include("cuckoo_cache.hrl").

-type cuckoo_cache() :: #cuckoo_cache{}.

-export_type([cuckoo_cache/0]).

-type hash() :: non_neg_integer().
-type fingerprint() :: pos_integer().
-type fingerprint_size() :: 4 | 8 | 16 | 32 | 64.
-type hash_function() :: fun((binary()) -> hash()).
-type key() :: term().
-type value() :: term().
-type fallback_fun() :: fun((key()) -> {ok, value()} | {error, term()}).
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
-define(DEFAULT_TTL, infinity).

-define(CACHE(Name), persistent_term:get({?MODULE, Name})).
-define(FINGERPRINT(Hash, FingerprintSize), Hash rem (1 bsl FingerprintSize - 1) + 1).
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
%% one of 4, 8, 16, 32, and 64 bits. Default fingerprint size is 32 bits.</p>
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
    FingerprintSize = proplists:get_value(fingerprint_size, Opts, ?DEFAULT_FINGERPRINT_SIZE),
    TTL = proplists:get_value(ttl, Opts, ?DEFAULT_TTL),
    (is_integer(TTL) andalso TTL > 0 orelse TTL == infinity) orelse error(badarg),

    Opts1 = lists:keystore(fingerprint_size, 1, Opts, {fingerprint_size, FingerprintSize}),
    Opts2 = lists:keystore(max_evictions, 1, Opts1, {max_evictions, 0}),
    Opts3 = lists:keydelete(name, 1, Opts2),
    Filter = cuckoo_filter:new(Capacity, Opts3),

    Table = ets:new(?MODULE, [
        ordered_set,
        public,
        {write_concurrency, true},
        {read_concurrency, true}
    ]),

    CuckooCache = #cuckoo_cache{
        table = Table,
        ttl = TTL,
        filter = Filter
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
get(
    #cuckoo_cache{
        table = Table,
        filter = Filter = #cuckoo_filter{fingerprint_size = FingerprintSize}
    },
    Key
) ->
    Hash = cuckoo_filter:hash(Filter, Key),
    Fingerprint = ?FINGERPRINT(Hash, FingerprintSize),
    case cuckoo_filter:contains_hash(Filter, Hash) of
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
delete(
    #cuckoo_cache{
        table = Table,
        filter = Filter = #cuckoo_filter{fingerprint_size = FingerprintSize}
    },
    Key
) ->
    Hash = cuckoo_filter:hash(Filter, Key),
    Fingerprint = ?FINGERPRINT(Hash, FingerprintSize),
    ets:delete(Table, Fingerprint),
    cuckoo_filter:delete_hash(Filter, Hash);
delete(Name, Key) ->
    delete(?CACHE(Name), Key).

%% @doc Returns the maximum capacity of the cuckoo filter in a cache.
-spec capacity(cuckoo_cache() | cache_name()) -> pos_integer().
capacity(#cuckoo_cache{filter = Filter}) ->
    cuckoo_filter:capacity(Filter);
capacity(Name) ->
    capacity(?CACHE(Name)).

%% @doc Returns number of items in the cuckoo filter of a cache.
-spec filter_size(cuckoo_cache() | cache_name()) -> non_neg_integer().
filter_size(#cuckoo_cache{filter = Filter}) ->
    cuckoo_filter:size(Filter);
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

-spec add_to_filter(cuckoo_cache(), hash()) -> ok.
add_to_filter(#cuckoo_cache{table = Table, filter = Filter}, Hash) ->
    case cuckoo_filter:add_hash(Filter, Hash, force) of
        ok ->
            ok;
        {ok, {Index, Evicted}} ->
            case cuckoo_filter:contains_fingerprint(Filter, Index, Evicted) of
                true ->
                    ok;
                false ->
                    ets:delete(Table, Evicted),
                    ok
            end
    end.

-spec do_put(cuckoo_cache(), key(), value(), timeout()) -> ok.
do_put(
    Cache = #cuckoo_cache{
        table = Table,
        filter = Filter = #cuckoo_filter{fingerprint_size = FingerprintSize}
    },
    Key,
    Value,
    Expiry
) ->
    Hash = cuckoo_filter:hash(Filter, Key),
    Fingerprint = ?FINGERPRINT(Hash, FingerprintSize),
    add_to_filter(Cache, Hash),
    ets:insert(Table, {Fingerprint, Key, Value, Expiry}),
    ok.

-spec do_fetch(cuckoo_cache(), key(), fallback_fun(), timeout()) -> value().
do_fetch(
    Cache = #cuckoo_cache{
        table = Table,
        filter = Filter = #cuckoo_filter{fingerprint_size = FingerprintSize}
    },
    Key,
    Fallback,
    Expiry
) ->
    Hash = cuckoo_filter:hash(Filter, Key),
    case cuckoo_filter:contains_hash(Filter, Hash) of
        true ->
            add_to_filter(Cache, Hash),
            Fingerprint = ?FINGERPRINT(Hash, FingerprintSize),
            case lookup_cache(Table, Fingerprint, Key) of
                {ok, Value} ->
                    {ok, Value};
                {error, not_found} ->
                    case Fallback(Key) of
                        {ok, Value} ->
                            ets:insert(Table, {Fingerprint, Key, Value, Expiry}),
                            {ok, Value};
                        Error ->
                            Error
                    end
            end;
        false ->
            add_to_filter(Cache, Hash),
            Fallback(Key)
    end.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

new_badargs_test() ->
    ?assertError(badarg, new(0)),
    ?assertError(badarg, new(8, [{bucket_size, 0}])),
    ?assertError(badarg, new(8, [{fingerprint_size, 5}])),
    ?assertError(badarg, new(8, [{ttl, 0}])).

new_test() ->
    Capacity = rand:uniform(1000),
    Cache = new(Capacity),
    RealCapacity = capacity(Cache),
    ?assert(RealCapacity >= Capacity),
    ?assertMatch(
        #cuckoo_cache{
            ttl = infinity,
            filter = #cuckoo_filter{}
        },
        Cache
    ).

put_get_delete_test() ->
    Cache = new(rand:uniform(1000)),
    Capacity = capacity(Cache),
    Key = <<"my_key">>,
    Value = 123,
    ok = put(Cache, Key, Value),
    ?assertMatch({ok, Value}, get(Cache, Key)),
    ok = delete(Cache, Key),
    Keys = lists:seq(1, Capacity),
    [put(Cache, K, K * 2) || K <- Keys],
    CachedKeys = [K || K <- Keys, {ok, K * 2} == get(Cache, K)],
    ?assertEqual(length(CachedKeys), cuckoo_cache:size(Cache)),
    ?assertEqual(length(CachedKeys), filter_size(Cache)),
    [delete(Cache, K) || K <- Keys],
    ?assertEqual(0, cuckoo_cache:size(Cache)),
    ?assertEqual(0, filter_size(Cache)).

fetch_test() ->
    Cache = new(rand:uniform(1000)),
    Capacity = capacity(Cache),
    Keys = lists:seq(1, Capacity),
    Fallback = fun
        (K) when K rem 2 == 0 -> {ok, K * 2};
        (K) -> {error, no_cache}
    end,
    FetchedKeys = [K || K <- Keys, {ok, K * 2} == fetch(Cache, K, Fallback)],
    ?assertEqual(0, cuckoo_cache:size(Cache)),
    ?assert(filter_size(Cache) =< Capacity),
    FetchedKeys = [K || K <- Keys, {ok, K * 2} == fetch(Cache, K, Fallback)],
    CachedKeys = [K || K <- Keys, {ok, K * 2} == get(Cache, K)],
    ?assert(cuckoo_cache:size(Cache) > 0),
    ?assertEqual(length(CachedKeys), cuckoo_cache:size(Cache)),
    ?assert(cuckoo_cache:size(Cache) < filter_size(Cache)).

-endif.
