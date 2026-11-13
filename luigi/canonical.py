# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Stable, type-preserving canonical encoding used by the canonical task
identity mode.

The legacy task id (see :func:`luigi.task.task_id_str`) is derived from the
string-serialized parameter values:

* dictionary parameters keep their *insertion order*, so two mappings holding
  the same key/value pairs can produce different task ids;
* values are flattened to strings first, so distinct values such as ``1``,
  ``True`` and ``"1"`` collide into the same task id when passed to an
  untyped :class:`~luigi.parameter.Parameter`.

The canonical identity mode addresses both problems by hashing the actual
(normalized) parameter values through :func:`canonical_identity_hash`.  The
encoding is deterministic across processes:

* mappings are sorted by their keys, so insertion order is irrelevant;
* every supported value carries an explicit type tag, so values of different
  Python types never share an identity;
* only a closed set of registered types is accepted -- arbitrary objects are
  rejected instead of being coerced through :func:`repr`;
* circular references raise :class:`CanonicalEncodingError` instead of
  recursing forever;
* the hash input never depends on process specific state such as
  :func:`hash` randomization or :func:`id`.

Supported value types (and their identity conventions):

* ``None``, ``bool``, ``int``, ``str`` (scalars; note that ``bool`` is kept
  distinct from ``int`` even though it is an ``int`` subclass);
* ``float`` (``NaN``/infinity use the fixed lexical forms ``NaN``,
  ``Infinity`` and ``-Infinity``);
* :class:`decimal.Decimal` (exact, lexical decimal form -- never float);
* :class:`datetime.date` and :class:`datetime.datetime` (ISO-8601; naive
  datetimes are encoded as-is and treated as local wall-clock values while
  timezone-aware datetimes are converted to UTC and carry an explicit ``Z``
  suffix, so wall-clock and absolute instants never collide);
* :class:`datetime.timedelta` (exact normalized form
  ``[D days, ]HH:MM:SS[.ffffff]``);
* ``list`` and ``tuple`` (ordered, recursive; the two container kinds keep
  distinct tags);
* mappings with ``str`` keys (order independent, recursive);
* :class:`enum.Enum` members (encoded from their declarative ``name``).
"""

from __future__ import annotations

import datetime
import enum
import hashlib
import warnings
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Iterable

from luigi import configuration

CANONICAL_IDENTITY_VERSION = "v2"
"""Suffix appended to canonical task ids so that they can never be confused
with a legacy task id referencing the same task family."""

_CONFIG_SECTION = "core"
_CONFIG_OPTION = "canonical_task_identity"


def canonical_identity_enabled() -> bool:
    """Whether task ids should be generated with the canonical identity mode.

    Defaults to ``False`` so existing deployments keep their legacy task ids.
    Enable it through the ``[core] canonical_task_identity`` config option or
    the ``--canonical-task-identity`` command line flag.
    """
    # Command line flags are stored on the active CmdlineParser rather than on
    # the config file singleton, so both sources have to be consulted.
    try:
        from luigi.cmdline_parser import CmdlineParser

        parser = CmdlineParser.get_instance()
        if parser is not None:
            cmdline_value = getattr(parser.known_args, "core_canonical_task_identity", None)
            if cmdline_value:
                return True
    except (AttributeError, ImportError):
        # Reading the parser must never break task instantiation (e.g. when
        # tasks are built directly through the Python API).
        pass
    try:
        return configuration.get_config().getboolean(_CONFIG_SECTION, _CONFIG_OPTION, False)
    except ValueError as exc:
        warnings.warn(
            "Invalid value for [%s] %s, falling back to legacy task ids: %s" % (_CONFIG_SECTION, _CONFIG_OPTION, exc),
            stacklevel=2,
        )
        return False


class CanonicalEncodingError(TypeError):
    """Raised when a parameter value cannot be represented by the canonical
    encoding (unsupported type) or contains a circular reference."""


def _encode_scalar(tag: str, payload: str) -> str:
    return tag + ":" + payload


def _encode_datetime(value: datetime.datetime) -> str:
    if value.tzinfo is not None and value.utcoffset() is not None:
        utc_value = value.astimezone(datetime.timezone.utc)
        return _encode_scalar("datetime_utc", utc_value.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z")
    return _encode_scalar("datetime", value.strftime("%Y-%m-%dT%H:%M:%S.%f"))


def _encode_mapping(value: Mapping[str, Any], seen: set[int]) -> str:
    marker = id(value)
    if marker in seen:
        raise CanonicalEncodingError("Circular reference detected while canonically encoding a mapping")
    seen.add(marker)
    try:
        items = []
        # Validate every key before sorting: a mixed-type key set would raise
        # a raw TypeError from sorted() instead of our explicit boundary error.
        for key in value.keys():
            if not isinstance(key, str):
                raise CanonicalEncodingError(
                    "Canonical mapping keys must be strings, got %s (%s)" % (key, type(key).__name__)
                )
        for key in sorted(value.keys()):
            items.append("k:" + key + "=" + _canonical_encoding(value[key], seen))
        return "map:{" + ",".join(items) + "}"
    finally:
        seen.remove(marker)


def _encode_sequence(values: Iterable[Any], tag: str, seen: set[int], container_id: int) -> str:
    if container_id in seen:
        raise CanonicalEncodingError("Circular reference detected while canonically encoding a sequence")
    seen.add(container_id)
    try:
        return tag + ":[" + ",".join(_canonical_encoding(item, seen) for item in values) + "]"
    finally:
        seen.remove(container_id)


def _canonical_encoding(value: Any, seen: set[int]) -> str:
    # bool must be checked before int: bool is an int subclass but True and 1
    # must have distinct canonical identities.
    if isinstance(value, bool):
        return _encode_scalar("bool", "true" if value else "false")
    if isinstance(value, int):
        return _encode_scalar("int", str(value))
    if isinstance(value, float):
        if value != value:  # NaN, which cannot be compared with == even to itself
            payload = "NaN"
        elif value == float("inf"):
            payload = "Infinity"
        elif value == float("-inf"):
            payload = "-Infinity"
        else:
            payload = repr(value)
        return _encode_scalar("float", payload)
    if isinstance(value, Decimal):
        return _encode_scalar("decimal", format(value, "f"))
    if isinstance(value, datetime.datetime):
        return _encode_datetime(value)
    if isinstance(value, datetime.date):
        return _encode_scalar("date", value.isoformat())
    if isinstance(value, datetime.timedelta):
        return _encode_scalar("timedelta", str(value))
    if isinstance(value, enum.Enum):
        return _encode_scalar("enum", type(value).__name__ + "." + value.name)
    if isinstance(value, Mapping):
        return _encode_mapping(value, seen)
    if isinstance(value, tuple):
        return _encode_sequence(value, "tuple", seen, id(value))
    if isinstance(value, list):
        return _encode_sequence(value, "list", seen, id(value))
    if value is None:
        return "none:"
    if isinstance(value, str):
        return _encode_scalar("str", value)
    raise CanonicalEncodingError(
        "Values of type %s cannot be part of a canonical task identity; "
        "use one of the registered scalar/container types instead" % type(value).__name__
    )


def canonical_encoding(value: Any) -> str:
    """Return the stable, type-tagged encoding of a single parameter value.

    The encoding is a plain ``str`` and is only meant to be fed back into
    :func:`canonical_hash`; its exact layout must not be relied upon by user
    code.
    """
    return _canonical_encoding(value, set())


def canonical_hash(parts: Iterable[tuple[str, Any]]) -> str:
    """Hash a mapping of parameter name -> normalized value.

    ``parts`` may be supplied in any order: parameter names are sorted before
    hashing, so the result is independent of the dictionary's insertion order.
    Non-significant parameters must be filtered out by the caller.
    """
    digest = hashlib.sha256()
    for param_name, param_value in sorted(parts, key=lambda item: item[0]):
        if not isinstance(param_name, str):
            raise CanonicalEncodingError("Parameter names must be strings, got %r" % (param_name,))
        digest.update(b"param:")
        digest.update(param_name.encode("utf-8"))
        digest.update(b"=")
        digest.update(canonical_encoding(param_value).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def canonical_identity_hash(task_family: str, parts: Iterable[tuple[str, Any]]) -> str:
    """Return the full canonical identity hash for a task instance.

    The task family participates in the hash (two different task classes can
    never share an identity) and the hash is prefixed with the canonical
    identity version so legacy and canonical ids are visibly distinct.
    """
    digest = hashlib.sha256()
    digest.update(b"family:")
    digest.update(task_family.encode("utf-8"))
    digest.update(b"\n")
    digest.update(canonical_hash(parts).encode("ascii"))
    digest.update(b"\n")
    digest.update(CANONICAL_IDENTITY_VERSION.encode("ascii"))
    return digest.hexdigest()
