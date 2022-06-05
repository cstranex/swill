import os
import pytest


@pytest.fixture(scope='function')
def osenv():
    old_environ = os.environ
    os.environ = {}
    yield os.environ
    os.environ = old_environ
