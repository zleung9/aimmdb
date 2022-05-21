import contextlib
import getpass

import pytest

@pytest.fixture
def enter_password(monkeypatch):
    """
    Return a context manager that overrides getpass, used like:

    >>> with enter_password(...):
    ...     # Run code that calls getpass.getpass().
    """

    @contextlib.contextmanager
    def f(password):
        original = getpass.getpass
        monkeypatch.setattr("getpass.getpass", lambda: password)
        yield
        monkeypatch.setattr("getpass.getpass", original)

    return f
