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
    if isinstance(obj, list):
        return hash(tuple(obj))
    if isinstance(obj, dict):
        return hash(tuple(obj.items()))
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


if os.environ.get("ENVIRONMENT") == "TEST":
    import unittest


    class TestHashsetCache(unittest.TestCase):
        """Tests for HashsetCache."""

        def setUp(self):
            self.cache = HashsetCache({1, 2, 3, 4, 5})

        def test_check(self):
            for value, expected in ((1, True), (10, False)):
                with self.subTest(value=value):
                    self.assertEqual(expected, self.cache.check(value))

        def test_insert(self):
            cache = HashsetCache(self.cache._store.copy())
            cache.insert(6)
            self.assertTrue(cache.check(6))

        def test_get(self):
            for value, expected in ((1, 1), (8, None)):
                with self.subTest(value=value):
                    self.assertEqual(expected, self.cache.get(value))

        def test_remove(self):
            cache = HashsetCache(self.cache._store.copy())
            for value, expected in ((1, 1), (9, None)):
                with self.subTest(value=value):
                    self.assertEqual(expected, cache.remove(value))
                    if expected is not None:
                        self.assertFalse(cache.check(value))

        def test_clear(self):
            cache = HashsetCache(self.cache._store.copy())
            self.assertEqual({1, 2, 3, 4, 5}, cache._store)
            cache.clear()
            self.assertEqual(set(), cache._store)

        def test_iter(self):
            self.assertEqual({1, 2, 3, 4, 5}, set(self.cache.iter()))


    class TestHashmapCache(unittest.TestCase):
        """Tests for HashmapCache."""

        def setUp(self):
            self.cache = HashmapCache(dict(a=1, b=2, c=3, d=4, e=5))

        def test_check(self):
            for key, expected in (("a", True), ("c", True), ("z", False)):
                with self.subTest(key=key):
                    self.assertEqual(expected, self.cache.check(key))

        def test_insert(self):
            cache = HashmapCache(self.cache._store.copy())
            cache.insert("y", 25)
            self.assertEqual(25, cache.get("y"))

        def test_get(self):
            for key, expected_value in (("a", 1), ("e", 5), ("z", None)):
                with self.subTest(key=key):
                    self.assertEqual(expected_value, self.cache.get(key))

        def test_remove(self):
            cache = HashmapCache(self.cache._store.copy())
            for key, expected_value in (("a", 1), ("d", 4), ("z", None)):
                with self.subTest(key=key):
                    self.assertEqual(expected_value, cache.remove(key))
                    if expected_value is not None:
                        self.assertFalse(cache.check(key))

        def test_clear(self):
            cache = HashmapCache(self.cache._store.copy())
            self.assertEqual(dict(a=1, b=2, c=3, d=4, e=5), cache._store)
            cache.clear()
            self.assertEqual(dict(), cache._store)

        def test_iter(self):
            self.assertEqual(dict(a=1, b=2, c=3, d=4, e=5), dict(self.cache.iter()))


    class TestSizedHashmapCache(unittest.TestCase):
        """Tests for SizedHashmapCache."""

        def test_insert_after_limit(self):
            limit = 10
            cache = SizedHashmapCache(limit)
            # Insert 10 items (at boundary of limit)
            for idx in range(65, 65+limit):
                cache.insert(chr(idx), idx)
            # Assert last item added in cache
            self.assertEqual(idx, cache.get(chr(idx)))
            # Add another item
            cache.insert(chr(idx+1), idx+1)
            # Assert first item no longer in cache
            self.assertEqual(None, cache.get(chr(65)))
            # Assert second item in cache
            self.assertEqual(66, cache.get(chr(66)))

        def test_insert_from_existing_store(self):
            limit = 10
            store = OrderedDict([(chr(idx), idx) for idx in range(65, 65+limit)])
            cache = SizedHashmapCache(limit, store=store)
            self.assertEqual(65, cache.get(chr(65)))
            next_idx = 65+limit+1
            cache.insert(chr(next_idx), next_idx)
            self.assertEqual(next_idx, cache.get(chr(next_idx)))
            self.assertEqual(None, cache.get(chr(65)))


    class TestSizedLRUCache(unittest.TestCase):
        """Tests for SizedLRUCache."""

        def test_get_cache_order(self):
            limit = 15
            cache = SizedLRUCache(limit, store=OrderedDict([(chr(idx), idx) for idx in range(65, 65+limit)]))
            # Get first item added to cache, moving it to "back"
            self.assertEqual(65, cache.get(chr(65)))
            next_idx = 65+limit+1
            # Insert new item to cache, removing item at front
            # Due to call to get above, first item should be second item added to cache
            cache.insert(chr(next_idx), next_idx)
            self.assertEqual(None, cache.get(chr(66)))
