#!/usr/bin/env python

"""Persistence demo.

This demo shows how NexusPy uses Pydantic + SQLAlchemy to persist to
SQLite (in memory for testing) and PostgreSQL (for production). It
also shows how we do bulk import for data migration. Imports are mixed
with functions and running code for tutorial purposes."""

import json
import sys

# ----------------------------------------------------------------------

# We need to handle complex types like datetimes, which means providing
# hooks for JSON persistence.

JSON_TIMESTAMP_KEY = '_timestamp_'

def _json_encoder(obj):
    """Custom JSON encoder."""
    if isinstance(obj, datetime):
        return {JSON_TIMESTAMP_KEY: obj.isoformat()}
    return obj

def _json_decoder(obj):
    """Custom JSON decoder."""
    if JSON_TIMESTAMP_KEY not in obj:
        return obj
    assert len(obj) == 1, \
        f"Timestamp entry in JSON can only have one element {obj}"
    return datetime.fromisoformat(obj[JSON_TIMESTAMP_KEY])

def json_dump(obj):
    """Convert object to JSON string, handling timestamps etc.

    - Sort keys so that tests are deterministic."""
    return json.dumps(obj, default=_json_encoder, sort_keys=True)

def json_load(text):
    """Convert JSON string to object, handling timestamps etc."""
    return json.loads(text, object_hook=_json_decoder)

# Create a base class for domain entities that:
# - has a unique ID field
# - can persist itself as a dictionary and as a JSON string
# - can restore itself from a dictionary or from a JSON string
#
# Persistence to and from JSON uses the hooks defined above.
# Persistence to JSON string takes an extra argument `replace`
# which is ignored here, but which will be used to trigger
# replacement of contained objects with their UIDs in parent
# classes. Reconstruction from JSON takes a list of objects
# (which must have `uid` fields) and matches them to the UID
# values in the sub-object list.

from abc import ABC
from dataclasses import asdict, dataclass

@dataclass
class DomainEntity(ABC):
    uid: str

    @classmethod
    def from_dict(cls, src: dict):
        return cls(**src)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_json(cls, src: str, contained=None):
        assert contained is None, \
            f"Child object replacement not implemented in {cls.__name__}"
        return cls.from_dict(json_load(src))

    def to_json(self, replace=False) -> str:
        assert not replace, \
            f"Child object replacement not implemented in {self.__class__.__name__}"
        return json_dump(self.to_dict())

# A flat record does not contain sub-objects that need to be
# persisted separately (e.g., `CellLine`). It has a name and a
# count so that we can test database uniqueness constraints.
# Note that we have to repeat the `@dataclass` decorator.

@dataclass
class FlatRecord(DomainEntity):
    name: str
    count: int

def test_flat_record_dict_json():
    rec = FlatRecord(uid="flat01", name="flat", count=3)
    as_dict = rec.to_dict()
    assert as_dict == {"uid": "flat01", "name": "flat", "count": 3}

    restored = FlatRecord.from_dict(as_dict)
    assert restored == rec

    as_json = rec.to_json()
    assert as_json == '{"count": 3, "name": "flat", "uid": "flat01"}'
    restored = FlatRecord.from_json(as_json)
    assert restored == rec

# Create a child record that we can later include in a parent record
# (like a sample is part of a luciferase experiment). We give the
# child record a `datetime` field to test JSON persistence.
    
from datetime import datetime

@dataclass
class ChildRecord(DomainEntity):
    when: datetime

def test_child_record_dict_json():
    then = datetime(2019, 1, 2, 3, 4, 5)
    rec = ChildRecord(uid="child01", when=then)
    as_dict = rec.to_dict()
    assert as_dict == {"uid": "child01", "when": then}

    restored = ChildRecord.from_dict(as_dict)
    assert restored == rec

    as_json = rec.to_json()
    assert as_json == '{"uid": "child01", "when": {"_timestamp_": "2019-01-02T03:04:05"}}'
    restored = ChildRecord.from_json(as_json)
    assert restored == rec

# A parent record contains a list of child records that need
# to be persisted separately (e.g., `LuciferaseExperiment` and
# its samples).
# - We have to reconstitute the children in `from_dict` ourselves.
# - We implement the `replace` parameter of `to_json`, so `children`
#   can hold either `ChildRecord` or `str` (UIDs).

from typing import List, Union

@dataclass
class ParentRecord(DomainEntity):
    name: str
    children: List[Union[ChildRecord, str]]

    @classmethod
    def from_dict(cls, src: dict):
        result = cls(**src)
        result.children = [c if isinstance(c, ChildRecord) else ChildRecord(**c)
                           for c in result.children]
        return result

    def to_json(self, replace=False) -> str:
        d = self.to_dict()
        if replace:
            d["children"] = [c["uid"] for c in d["children"]]
        return json_dump(d)

    @classmethod
    def from_json(cls, src: str, contained=None):
        temp = json_load(src)
        if contained is not None:
            contained = {c.uid: c for c in contained}
            temp["children"] = [contained[c] for c in temp["children"]]
        return cls.from_dict(temp)

def test_parent_record_dict_json():
    first_time = datetime(2019, 1, 2, 3, 4, 5)
    first_child = ChildRecord(uid="child01", when=first_time)
    second_time = datetime(2020, 6, 7, 8, 9, 0)
    second_child = ChildRecord(uid="child02", when=second_time)
    parent = ParentRecord(uid="parent01", name="parent", children=[first_child, second_child])

    as_dict = parent.to_dict()
    assert as_dict == {"uid": "parent01", "name": "parent", "children": [
        {"uid": "child01", "when": first_time},
        {"uid": "child02", "when": second_time}
    ]}
    restored = ParentRecord.from_dict(as_dict)
    assert restored == parent

    as_json_no_replace = parent.to_json(False)
    assert as_json_no_replace == '{"children": [{"uid": "child01", "when": {"_timestamp_": "2019-01-02T03:04:05"}}, {"uid": "child02", "when": {"_timestamp_": "2020-06-07T08:09:00"}}], "name": "parent", "uid": "parent01"}'
    restored = ParentRecord.from_json(as_json_no_replace)
    assert restored == parent

    as_json_with_replace = parent.to_json(True)
    assert as_json_with_replace == '{"children": ["child01", "child02"], "name": "parent", "uid": "parent01"}'
    restored = ParentRecord.from_json(as_json_with_replace, [first_child, second_child])
    assert restored == parent
