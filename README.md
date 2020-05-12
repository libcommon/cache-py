# cache-py

## Overview

Caches are useful as temporary, fast-access datastores for API responses, data computed with complex calculations, and
any other kind of data that's relatively expensive to retrieve/compute. `cache-py` is a Python library consisting of
a simple cache interface (base class), implementations of common caches using this interface, and a general purpose
hashing function for Python objects. All implementations are currently local and in-memory, though one could easily use the interface
to implement file or API-based caches.

## Installation

### Install from PyPi (preferred method)

```bash
pip install lc-cache
```

### Install from GitHub with Pip

```bash
pip install git+https://github.com/libcommon/cache-py.git@vx.x.x#egg=lc_cache
```

where `x.x.x` is the version you want to download.

## Install by Manual Download

To download the source distribution and/or wheel files, navigate to
`https://github.com/libcommon/cache-py/tree/releases/vx.x.x/dist`, where `x.x.x` is the version you want to install,
and download either via the UI or with a tool like wget. Then to install run:

```bash
pip install <downloaded file>
```

Do _not_ change the name of the file after downloading, as Pip requires a specific naming convention for installation files.

## Dependencies

`cache-py` does not have external dependencies. Only Python versions >= 3.6 are officially supported.

## Getting Started

If you simply need to store values and later check for their presence, but don't necessarily care about retrieving the values themselves, use the `HashsetCache`.

```python
import random

from lc_cache import HashsetCache

def main() -> int:
    # Create the cache
    cache = HashsetCache()

    # Insert some random values into the cache
    for _ in range(10):
        cache.insert(random.randrange(0, 10))
        
    # Check for their presence
    for i in range(10):
        if cache.check(i):
            print("Encountered value", i)

    return 0
```

If you need to retrieve values later on, and have a key to associate them with, use the `HashmapCache`. Below is an implementation
of `functools.lru_cache`, though it is simply a reference. Do not use this implementation as it is not thread-safe, and
the `functools` version has some optimizations that likely make it faster.

```python
from functools import wraps
from typing import Any, Callable

from lc_cache import HashmapCache, gen_python_hash

_CACHE = HashmapCache()

def lru_cache(f: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(f)
    def wrapper(*args, **kwargs) -> Any:
        args_hash = hash((gen_python_hash(args), gen_python_hash(kwargs)))
        # NOTE: Have to use check here because a cached value may be None
        if _CACHE.check(args_hash):
            return _CACHE.get(args_hash)
        return f(*args, **kwargs)
    return wrapper
```

The `SizedHashmapCache` and `SizedLRUCache` classes may be useful in long-running processes where you want to cap the
number of items in the cache.

## Use Case

The cache interface class, `Cache`, defines a simple API for manipulating the underlying storage object(s):

    * `check`: check if a value is present in the cache
    * `insert`: add a value to the cache, or update an existing one
    * `get`: get a value from the cache, if it exists
    * `remove`: remove a value from the cache, if it exists
    * `clear`: clear the cache
    * `iter`: returns a [Generator](https://wiki.python.org/moin/Generators) over the values in the dictionary

Note that the word "value" here is used loosely, and does not refer to a value in a Python dictionary.  That being said,
Python dictionaries are commonly used to cache values. The `HashmapCache` has a regular Python dictionary as the underlying
storage object, and thus can be used just like a dictionary. Similarly, The `SizedHashmapCache` and `SizedLRUCache` cache
classes are also backed by dictionaries, but with some extra constraints on cache size and logic for removing items.

If your use case isn't complicated, and a simple dictionary will suffice, then there's no need to use classes from this library.
However, if you need something like an LRU cache, or if you're developing an MVP but will need a more complex cache implementation
later on (and thus want a consistent API), this library can be helpful.

## Contributing/Suggestions

Contributions and suggestions are welcome! To make a feature request, report a bug, or otherwise comment on existing
functionality, please [file an issue](https://github.com/libcommon/cache-py/issues/new/choose). For contributions please
[submit a PR](https://github.com/libcommon/cache-py/compare), but make sure to lint, type-check, and test your code
before doing so. Thanks in advance!
