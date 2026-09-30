# -*- coding: utf-8 -*-
#
# Copyright 2026 Spotify AB
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

"""
Secret reference parameters.

Some parameters (access tokens, passwords, API keys) must never leave the
worker machine.  A :class:`~luigi.parameter.SecretParameter` does not carry
the plaintext value around: it carries a :class:`SecretReference` consisting
of a secret *name* and a *version*.  Only that reference is serialized into
task ids, sent to the scheduler, written into scheduler state and shown on
error pages.

The plaintext is obtained lazily, right before a task starts running, by
calling a :class:`SecretResolver` that the application explicitly registers
on the worker (or on the parameter).  Every (re)try re-resolves the secret,
so a previously failing resolver does not permanently block the task.  If
resolution fails, the task is not started and is reported as failed with a
message that only mentions the reference -- the resolved value, if any, is
never put into repr output, exception messages, scheduler RPC payloads or
logs.
"""

from __future__ import annotations

import abc
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

SECRET_REF_SEPARATOR = "@"
#: Key in the worker ``secret_resolvers`` mapping used for parameters that do
#: not name a specific resolver.
DEFAULT_RESOLVER_NAME = "default"


class SecretException(Exception):
    """Base exception for secret reference handling."""


class SecretReferenceParseException(SecretException):
    """Raised when a value cannot be interpreted as a secret reference."""


class SecretResolutionError(SecretException):
    """
    Raised when a secret cannot be resolved on the worker.

    The message intentionally contains only the secret reference (name and
    version) and never the resolved value, so it is safe to surface in task
    failure explanations and error emails.
    """

    def __init__(self, reference: "SecretReference", reason: Optional[str] = None):
        self.reference = reference
        message = "Could not resolve secret {!r}".format(reference)
        if reason:
            message += ": {}".format(reason)
        super().__init__(message)


class SecretReference:
    """
    An immutable pointer to a secret: ``"<name>@<version>"``.

    This is the only thing that travels with the task manifest.  It never
    holds the plaintext and its representations are therefore safe to log,
    serialize into task ids and send over the scheduler RPC.
    """

    __slots__ = ("name", "version")

    def __init__(self, name: str, version: str):
        if not isinstance(name, str) or not name:
            raise SecretReferenceParseException("Secret name must be a non-empty string")
        if not isinstance(version, str) or not version:
            raise SecretReferenceParseException("Secret version must be a non-empty string")
        if SECRET_REF_SEPARATOR in name:
            raise SecretReferenceParseException(
                "Secret name must not contain {!r}: {!r}".format(SECRET_REF_SEPARATOR, name)
            )
        self.name = name
        self.version = version

    @classmethod
    def parse(cls, value: Any) -> "SecretReference":
        """Parse ``"<name>@<version>"`` or normalize a reference-like value.

        A :class:`ResolvedSecret` is reduced to its plain reference, so
        parsing/normalization never accidentally preserves a plaintext value.
        """
        if isinstance(value, SecretReference):
            return value.reference if isinstance(value, ResolvedSecret) else value
        if isinstance(value, dict):
            try:
                return cls(name=value["name"], version=str(value["version"]))
            except (KeyError, TypeError):
                raise SecretReferenceParseException(
                    "Secret reference mapping must contain 'name' and 'version' keys, got {!r}".format(value)
                )
        if not isinstance(value, str):
            raise SecretReferenceParseException(
                "Expected a secret reference of the form "
                "'<name>{sep}<version>', got {!r}".format(value, sep=SECRET_REF_SEPARATOR)
            )
        if SECRET_REF_SEPARATOR not in value:
            raise SecretReferenceParseException(
                "Secret parameter needs a reference of the form "
                "'<name>{sep}<version>' rather than a raw value".format(sep=SECRET_REF_SEPARATOR)
            )
        name, version = value.split(SECRET_REF_SEPARATOR, 1)
        return cls(name=name, version=version)

    def serialize(self) -> str:
        return "{}{}{}".format(self.name, SECRET_REF_SEPARATOR, self.version)

    def __str__(self) -> str:
        return self.serialize()

    def __repr__(self) -> str:
        return "SecretReference({})".format(self.serialize())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SecretReference):
            return NotImplemented
        return self.name == other.name and self.version == other.version

    def __hash__(self) -> int:
        return hash((SecretReference, self.name, self.version))


class ResolvedSecret(SecretReference):
    """
    A secret reference together with the plaintext obtained from the resolver.

    Exists only on the worker for the duration of a task run.  ``str()``
    yields the plaintext (so ``self.token`` can be used like any other
    string), while ``repr()`` and serialization still show only the
    reference.  Equality and hashing follow the reference, so a resolved
    secret never becomes part of task identity.
    """

    __slots__ = ("_value",)

    def __init__(self, reference: SecretReference, value: str):
        if not isinstance(value, str):
            raise SecretException("Resolved secret value must be a string")
        super().__init__(reference.name, reference.version)
        self._value = value

    @property
    def reference(self) -> SecretReference:
        return SecretReference(self.name, self.version)

    @property
    def value(self) -> str:
        return self._value

    def serialize(self) -> str:
        return self.reference.serialize()

    def __str__(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "ResolvedSecret({})".format(self.reference.serialize())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SecretReference):
            return self.reference == (other.reference if isinstance(other, ResolvedSecret) else other)
        return NotImplemented

    def __hash__(self) -> int:
        return hash((SecretReference, self.name, self.version))


class SecretResolver(abc.ABC):
    """
    Resolves a :class:`SecretReference` to its plaintext value on a worker.

    Applications provide a concrete implementation (talking to a vault, KMS,
    environment variable store, ...) and register it explicitly with the
    :class:`~luigi.worker.Worker` (``secret_resolvers``) or on the
    :class:`~luigi.parameter.SecretParameter` itself.
    """

    @abc.abstractmethod
    def resolve(self, reference: SecretReference) -> str:
        """Return the plaintext for *reference* or raise on failure."""


@contextmanager
def resolved_task_secrets(task: Any, resolvers: Dict[str, SecretResolver]) -> Iterator[None]:
    """
    Resolve every :class:`~luigi.parameter.SecretParameter` of *task*.

    The plaintext is injected onto the task only while the context is active
    and the original :class:`SecretReference` values are restored afterwards,
    so the resolved values never linger on the task object (which is kept in
    worker memory and serialized into scheduler updates).

    Resolution happens *before* ``task.run()`` is entered; any failure raises
    :class:`SecretResolutionError` (which carries no plaintext).
    """
    from luigi import parameter as parameter_module

    originals: Dict[str, SecretReference] = {}
    try:
        for param_name, param_obj in task.get_params():
            if not isinstance(param_obj, parameter_module.SecretParameter):
                continue
            value = getattr(task, param_name)
            reference = SecretReference.parse(value)
            resolver = _select_resolver(param_obj, resolvers)
            if resolver is None:
                raise SecretResolutionError(reference, "no secret resolver is configured on the worker")
            try:
                plaintext = resolver.resolve(reference)
            except Exception as exc:
                # Never trust any string produced by the resolver -- even a
                # SecretResolutionError it constructs itself could embed the
                # plaintext in its message or reason. Rebuild a strictly
                # reference-only error so nothing leaks into scheduler state,
                # failure pages, emails or logs.
                raise SecretResolutionError(reference, type(exc).__name__) from None
            resolved = ResolvedSecret(reference, plaintext)
            originals[param_name] = value
            setattr(task, param_name, resolved)
        yield
    finally:
        # Restore in reverse resolution order so that a failure while
        # resolving a later secret still resets every earlier one.
        for param_name in reversed(list(originals)):
            setattr(task, param_name, SecretReference.parse(originals[param_name]))


def _select_resolver(param_obj: Any, resolvers: Dict[str, SecretResolver]) -> Optional[SecretResolver]:
    """Return the parameter-level resolver, else the worker resolver keyed by name."""
    parameter_resolver = getattr(param_obj, "resolver", None)
    if parameter_resolver is not None:
        return parameter_resolver
    resolver_name = getattr(param_obj, "resolver_name", None)
    if resolver_name is not None:
        return resolvers.get(resolver_name)
    # Unnamed secret: use the worker-wide default resolver if one exists.
    return resolvers.get(DEFAULT_RESOLVER_NAME)
