# pylint: disable=redefined-outer-name

import pathlib
import tempfile

import pytest

from lazarus.storage.local import LocalStorage


@pytest.fixture
def storage():
    with tempfile.TemporaryDirectory() as tdir:
        yield LocalStorage(pathlib.Path(tdir))


def test_add_element(storage):
    storage.put("k", "v")
    assert storage.get("k") == "v"


def test_add_element_wtopic(storage):
    storage.put("k", "v", topic="topic")
    assert storage.get("k", topic="topic") == "v"
