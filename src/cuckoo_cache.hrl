-record(cuckoo_cache, {
    table :: ets:tid(),
    ttl :: pos_integer(),
    buckets :: atomics:atomics_ref(),
    num_buckets :: pos_integer(),
    max_hash :: pos_integer(),
    bucket_size :: pos_integer(),
    fingerprint_size :: 4 | 8 | 16 | 32 | 64,
    hash_function :: fun((binary()) -> non_neg_integer())
}).
