"""Internal-only module with immutable data structures.

Please, do not use it outside of Luigi codebase itself.
"""

from collections import OrderedDict

try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping  # type: ignore


class FrozenOrderedDict(Mapping):
    """
    It is an immutable wrapper around ordered dictionaries that implements the complete :py:class:`collections.Mapping`
    interface. It can be used as a drop-in replacement for dictionaries where immutability and ordering are desired.
    """

    def __init__(self, *args, **kwargs):
        self.__dict = OrderedDict(*args, **kwargs)
        self.__hash = None

    def __getitem__(self, key):
        return self.__dict[key]

    def __iter__(self):
        return iter(self.__dict)

    def __len__(self):
        return len(self.__dict)

    def __repr__(self):
        # We should use short representation for beautiful console output
        return repr(dict(self.__dict))

    def __hash__(self):
        if self.__hash is None:
            # Two requirements have to hold at the same time:
            #   * equality is order independent, so equal mappings must keep
            #     equal hashes (Python's hash/eq invariant);
            #   * the previous XOR-over-items implementation collapsed
            #     unrelated mappings such as {"a": 1, "b": 2} and
            #     {"a": 2, "b": 1} into the same hash, which made the task
            #     instance cache return a task built from different arguments.
            # Hash a canonical, key-sorted tuple of item hashes instead.
            self.__hash = hash(tuple(sorted((hash(key), hash(value)) for key, value in self.items())))

        return self.__hash

    def get_wrapped(self):
        return self.__dict


def recursively_freeze(value):
    """
    Recursively walks ``Mapping``s and ``list``s and converts them to ``FrozenOrderedDict`` and ``tuples``, respectively.
    """
    if isinstance(value, Mapping):
        return FrozenOrderedDict(((k, recursively_freeze(v)) for k, v in value.items()))
    elif isinstance(value, list) or isinstance(value, tuple):
        return tuple(recursively_freeze(v) for v in value)
    return value


def recursively_unfreeze(value):
    """
    Recursively walks ``FrozenOrderedDict``s and ``tuple``s and converts them to ``dict`` and ``list``, respectively.
    """
    if isinstance(value, Mapping):
        return dict(((k, recursively_unfreeze(v)) for k, v in value.items()))
    elif isinstance(value, list) or isinstance(value, tuple):
        return list(recursively_unfreeze(v) for v in value)
    return value
