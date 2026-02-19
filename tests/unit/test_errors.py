"""Tests that TransientError and FatalError are proper Exception subclasses
and can be instantiated, raised, and caught independently."""

import pytest

from rag_indexer.errors import TransientError, FatalError


def test_transient_error_is_exception_subclass():
    assert issubclass(TransientError, Exception)


def test_fatal_error_is_exception_subclass():
    assert issubclass(FatalError, Exception)


def test_transient_error_is_not_fatal_error():
    assert not issubclass(TransientError, FatalError)


def test_fatal_error_is_not_transient_error():
    assert not issubclass(FatalError, TransientError)


def test_transient_error_carries_message():
    err = TransientError("retry me")
    assert str(err) == "retry me"


def test_fatal_error_carries_message():
    err = FatalError("dead letter")
    assert str(err) == "dead letter"


def test_bare_except_catches_transient():
    """A bare `except Exception` catches TransientError (confirms it doesn't bypass hierarchy)."""
    with pytest.raises(Exception):
        raise TransientError("caught")


def test_bare_except_catches_fatal():
    """A bare `except Exception` catches FatalError (confirms it doesn't bypass hierarchy)."""
    with pytest.raises(Exception):
        raise FatalError("caught")
