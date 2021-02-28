-record(cuckoo_cache, {
    table :: ets:tid(),
    ttl :: pos_integer(),
    filter :: cuckoo_filter:cuckoo_filter()
}).
