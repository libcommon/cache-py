## -*- coding: UTF-8 -*-
## cache.py
##
## Copyright (c) 2019 libcommon
##
## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:
##
## The above copyright notice and this permission notice shall be included in all
## copies or substantial portions of the Software.
##
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
## SOFTWARE.


from collections import OrderedDict
import os
from typing import Any, Dict, Generator, Optional, Set, Tuple, TypeVar


__author__ = "libcommon"
T = TypeVar("T")
U = TypeVar("U")


def gen_python_hash(obj: Any) -> int:
    """Generate Python hash for given object, if hashable.
    NOTE: Python hash output is an int and does not have the same
    uniqueness guarantees as a cryptographic hash function.  Use
    with caution.
    """
    if hasattr(obj, "__hash__") and callable(obj.__hash__):
        return hash(obj)
    if isinstance(obj, (list, tuple)):
        return hash(tuple(gen_python_hash(item) for item in obj))
    if isinstance(obj, dict):
        return hash(tuple((gen_python_hash(key), gen_python_hash(value)) for key, value in obj.items()))
    raise TypeError("cannot generate hash for type {}".format(type(obj).__name__))


class Cache:
    """Interface for a simple cache that implements:
        1) Checking for the presence of a value
        2) Inserting a value
        3) Retreiving a value
        4) Removing a value
        5) Clearing the cache
        6) Iterating over items in the cache
    """
    __slots__ = ("_store",)

    @staticmethod
    def _gen_store() -> Any:
        """Generate instance of backing store."""
        raise NotImplementedError

    def __init__(self, store: Optional[Any] = None) -> None:
        self._store = self._gen_store() if store is None else store

    def check(self, key: Any) -> bool:
        """Check if value in set."""
        raise NotImplementedError

    def insert(self, key: Any, value: Optional[Any]) -> None:
        """Insert value in set."""
        raise NotImplementedError

    def get(self, key: Any) -> Optional[Any]:
        """Retrieve value from cache."""
        raise NotImplementedError

    def remove(self, key: Any) -> Optional[Any]:
        """Remove value from cache."""
        raise NotImplementedError

    def clear(self) -> None:
        """Clear the cache."""
        raise NotImplementedError

    def iter(self) -> Generator[Any, None, None]:
        """Generator over items in the cache."""
        raise NotImplementedError


class HashsetCache(Cache):
    """Cache backed by hashset store."""
    __slots__ = ()

    @staticmethod
    def _gen_store() -> Set[T]:
        return set()

    def check(self, key: T) -> bool:
        return key in self._store

    def insert(self, key: T, value: None = None) -> None:
        self._store.add(key)

    def get(self, key: T) -> Optional[T]:
        if self.check(key):
            return key
        return None

    def remove(self, key: T) -> Optional[T]:
        if self.check(key):
            self._store.discard(key)
            return key
        return None

    def clear(self) -> None:
        self._store.clear()

    def iter(self) -> Generator[T, None, None]:
        for item in self._store:
            yield item


class HashmapCache(Cache):
    """Cache backed by hashmap store."""
    __slots__ = ()

    @staticmethod
    def _gen_store() -> Dict[T, U]:
        return dict()

    def check(self, key: T) -> bool:
        return key in self._store

    def insert(self, key: T, value: Optional[U]) -> None:
        self._store[key] = value

    def get(self, key: T) -> Optional[U]:
        return self._store.get(key)

    def remove(self, key: T) -> Optional[U]:
        if self.check(key):
            value = self.get(key)
            del self._store[key]
            return value
        return None

    def clear(self) -> None:
        self._store.clear()

    def iter(self) -> Generator[Tuple[T, U], None, None]:
        for item in self._store.items():
            yield item


class SizedHashmapCache(HashmapCache):
    """Hashmap cache with a maximum size limit."""
    __slots__ = ("limit",)

    @staticmethod
    def _gen_store() -> Dict[T, U]:
        return OrderedDict()

    def __init__(self, limit: int, store: Optional[Dict[T, U]] = None) -> None:
        super().__init__(store)
        if limit <= 0:
            raise ValueError("Limit must be greater than 0")
        self.limit = limit

    def insert(self, key: T, value: Optional[U]) -> None:
        if len(self._store) == self.limit:
            self._store.popitem(last=False)
        self._store[key] = value


class SizedLRUCache(SizedHashmapCache):
    """LRU cache with a maximum size limit."""
    __slots__ = ()

    def get(self, key: T) -> Optional[U]:
        if self.check(key):
            value = self._store.get(key)
            self._store.move_to_end(key)
            return value
        return None
