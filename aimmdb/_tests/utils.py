import contextlib
import getpass

import pytest
from tiled.client.utils import ClientError


@contextlib.contextmanager
def fail_with_status_code(status_code):
    with pytest.raises(ClientError) as info:
        yield
    assert info.value.response.status_code == status_code
