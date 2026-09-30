# -*- coding: utf-8 -*-
#
# Copyright 2026 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Secret references and resolvers.

This module implements the machinery behind
:class:`~luigi.parameter.SecretParameter`. Tasks never carry secret
plaintext: they only carry a :class:`SecretRef` (a name plus a version).
The plaintext is fetched lazily, on the worker, at the moment the task
actually asks for it, by calling :meth:`SecretRef.resolve`. The resolved
value is never stored on the task, never serialized and never sent to the
scheduler, so it cannot end up in task ids, scheduler state, ``repr``
output, error pages or RPC payloads.
"""

import threading
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Union


class SecretResolutionError(Exception):
    """
    Raised when a secret reference cannot be resolved to plaintext.

    The message only ever contains the secret *reference* (name and
    version), never any resolved plaintext.
    """

    pass


class SecretResolver:
    """
    Base class for secret resolvers.

    Subclasses implement :py:meth:`resolve` to fetch the plaintext for a
    given :class:`SecretRef`, e.g. from a vault, a KMS or a metadata
    service. Implementations must not include resolved plaintext in
    exception messages.
    """

    def resolve(self, ref: "SecretRef") -> str:
        """
        Return the plaintext for ``ref``.

        :param ref: the secret reference to resolve.
        :raises SecretResolutionError: if the secret cannot be resolved.
        """
        raise NotImplementedError


class MappingSecretResolver(SecretResolver):
    """
    Simple resolver backed by an in-memory mapping.

    Keys may be either ``(name, version)`` tuples or plain ``name``
    strings (which act as a fallback for any version). Mostly useful for
    tests and small deployments.
    """

    def __init__(self, secrets: Dict[Union[str, Tuple[str, str]], str]):
        self._secrets = dict(secrets)

    def resolve(self, ref: "SecretRef") -> str:
        key = (ref.name, ref.version)
        if key in self._secrets:
            return self._secrets[key]
        if ref.name in self._secrets:
            return self._secrets[ref.name]
        raise SecretResolutionError("No secret stored for reference '%s'" % ref)


_resolver_lock = threading.Lock()
_resolver: Optional[SecretResolver] = None


def set_secret_resolver(resolver: Optional[SecretResolver]) -> None:
    """
    Register the process-wide secret resolver used by
    :meth:`SecretRef.resolve` when no explicit resolver is passed.

    :param resolver: a :class:`SecretResolver` instance, or ``None`` to
                     clear the current resolver.
    """
    global _resolver
    if resolver is not None and not isinstance(resolver, SecretResolver):
        raise TypeError("resolver must be a SecretResolver instance")
    with _resolver_lock:
        _resolver = resolver


def get_secret_resolver() -> Optional[SecretResolver]:
    """
    Return the currently registered process-wide secret resolver, or
    ``None`` if none has been registered.
    """
    with _resolver_lock:
        return _resolver


@dataclass(frozen=True)
class SecretRef:
    """
    A reference to a secret: a ``name`` plus a ``version``.

    This object never holds plaintext. Its string form (``name@version``)
    is what appears in task ids, parameter manifests sent to the
    scheduler, ``repr`` output and error pages. The plaintext is only
    obtained by explicitly calling :meth:`resolve` on the worker.
    """

    name: str
    version: str = "latest"

    SEPARATOR = "@"

    def __post_init__(self):
        if not self.name or not isinstance(self.name, str):
            raise ValueError("SecretRef name must be a non-empty string")
        if not self.version or not isinstance(self.version, str):
            raise ValueError("SecretRef version must be a non-empty string")

    @classmethod
    def from_str(cls, value: str) -> "SecretRef":
        """
        Parse a ``name@version`` string. If no version is given, it
        defaults to ``"latest"``.
        """
        if not isinstance(value, str):
            raise ValueError("Cannot parse a SecretRef from %r" % (value,))
        name, separator, version = value.rpartition(cls.SEPARATOR)
        if not separator:
            return cls(name=value)
        if not name or not version:
            raise ValueError("Invalid secret reference %r, expected 'name%sversion'" % (value, cls.SEPARATOR))
        return cls(name=name, version=version)

    def __str__(self) -> str:
        return "%s%s%s" % (self.name, self.SEPARATOR, self.version)

    def resolve(self, resolver: Optional[SecretResolver] = None) -> str:
        """
        Resolve this reference to its plaintext value.

        The value is fetched from the resolver on every call (nothing is
        cached on the task), so a retried task always re-fetches. The
        plaintext is returned to the caller only; it is never stored on
        this object.

        :param resolver: an explicit resolver to use. If omitted, the
                         process-wide resolver registered with
                         :func:`set_secret_resolver` is used.
        :raises SecretResolutionError: if no resolver is available or the
                                       resolver fails.
        """
        resolver = resolver if resolver is not None else get_secret_resolver()
        if resolver is None:
            raise SecretResolutionError(
                "Cannot resolve secret reference '%s': no secret resolver configured. "
                "Register one with luigi.secret.set_secret_resolver() or pass one explicitly." % self
            )
        try:
            value = resolver.resolve(self)
        except SecretResolutionError:
            raise
        except Exception as exc:
            raise SecretResolutionError("Failed to resolve secret reference '%s'" % self) from exc
        if not isinstance(value, str):
            raise SecretResolutionError("Resolver returned a non-string value for secret reference '%s'" % self)
        return value
