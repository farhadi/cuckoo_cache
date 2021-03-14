# Cuckoo Cache

[![CI build status](https://github.com/farhadi/cuckoo_cache/workflows/CI/badge.svg)](https://github.com/farhadi/cuckoo_cache/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/farhadi/cuckoo_cache/branch/main/graph/badge.svg)](https://codecov.io/gh/farhadi/cuckoo_cache)
[![Hex docs](http://img.shields.io/badge/hex.pm-docs-green.svg?style=flat)](https://hexdocs.pm/cuckoo_cache)
[![Hex Version](http://img.shields.io/hexpm/v/cuckoo_cache.svg?style=flat)](https://hex.pm/packages/cuckoo_cache)
[![License](http://img.shields.io/hexpm/l/cuckoo_cache.svg?style=flat)](https://github.com/farhadi/cuckoo_cache/blob/main/LICENSE)

A high-performance probabilistic LRFU in-memory cache with the ability to detect
one-hit-wonders using a built-in [Cuckoo Filter](https://github.com/farhadi/cuckoo_filter)
for Erlang and Elixir.

# Introduction

I call it a probabilistic cache because it relys on probablistic features of
its underlying Cuckoo Filter. There is a high chance that most recently/frequently
used items will remain in the cache and least recently/frequently used items are
either not cached at all or removed lazily to give space to recently/frequently
accessed items.

Using a Cuckoo Filter gives us these benefits:

  - Avoid caching one-hit-wonders in the cache
  - Lazily removing least recently/frequently used items
  - Concurrent process-less architecture

# How it works

CuckooCache uses a cuckoo filter to keep track of accessed/cached items, and
uses an ets table to store cached items.

Every time an item is accessed, its key is inserted in the cuckoo filter. An item
is cached in the ets table only if it already exists in the cuckoo filter. In
other words items that are accessed once and never used again in a sliding
window of requests (one-hit-wonders), will not be cached.

When an item is added to the cuckoo filter, and filter capacity is full, another
random item gets removed from the filter, and removed item is also removed from
the ets cache if it exists in the cache and no longer exists in the filter.

One interesting feature of a cuckoo filter is that it can contain duplicate
elements (up to bucket_size * 2 times). This feature gives frequently accessed
elements more chance to stay in the filter as long as they are accessed
frequently enough.

## License

Copyright 2021, Ali Farhadi <a.farhadi@gmail.com>.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.