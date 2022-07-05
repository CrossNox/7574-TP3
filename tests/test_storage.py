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


def test_load_from_disk(storage):
    storage.put("k", "v", topic="topic")
    new_storage = LocalStorage.load(storage.data_dir)
    assert new_storage.get("k", topic="topic") == "v"


def test_add_elements(storage):
    storage.put("k1", "v1")
    storage.put("k2", "v2")
    assert storage.get("k1") == "v1"
    assert storage.get("k2") == "v2"


def test_add_elements_wtopic(storage):
    storage.put("k", "v1", topic="topic1")
    storage.put("k", "v2", topic="topic2")
    assert storage.get("k", topic="topic1") == "v1"
    assert storage.get("k", topic="topic2") == "v2"


def test_load_from_disk_single_topic(storage):
    storage.put("k1", "v1", topic="topic")
    storage.put("k2", "v2", topic="topic")
    new_storage = LocalStorage.load(storage.data_dir)
    assert new_storage.get("k1", topic="topic") == "v1"
    assert new_storage.get("k2", topic="topic") == "v2"


def test_step_on_element(storage):
    storage.put("k1", "v1")
    storage.put("k1", "v2")
    assert storage.get("k1") == "v2"


def test_load_from_disk_step_on_keys(storage):
    storage.put("k1", "v1", topic="topic")
    storage.put("k1", "v2", topic="topic")
    new_storage = LocalStorage.load(storage.data_dir)
    assert new_storage.get("k1", topic="topic") == "v2"


def test_add_int(storage):
    storage.put("k1", 1)
    new_storage = LocalStorage.load(storage.data_dir)
    assert storage.get("k1") == 1
    assert new_storage.get("k1") == 1


def test_add_dict(storage):
    storage.put("k1", {"v1": 1, "v2": "2"})
    new_storage = LocalStorage.load(storage.data_dir)
    assert storage.get("k1") == {"v1": 1, "v2": "2"}
    assert new_storage.get("k1") == {"v1": 1, "v2": "2"}
