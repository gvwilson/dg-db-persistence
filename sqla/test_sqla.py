#!/usr/bin/env python

"""Persistence demo.

This demo shows how NexusPy uses Pydantic + SQLAlchemy to persist to
SQLite (in memory for testing) and PostgreSQL (for production). It
also shows how we do bulk import for data migration. Imports are mixed
with functions and running code for tutorial purposes."""

import sys
import pytest

# ----------------------------------------------------------------------

# We need to handle complex types like datetimes, which means providing
# hooks for JSON persistence.

import json

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

# Create a base class for domain entities that has a unique ID
# field and persist to/from dictionaries and JSON strings.
# - Persistence to and from JSON uses the hooks defined above.
# - Persistence to JSON string takes an extra argument `replace`
#   which is ignored here, but which will be used to trigger
#   replacement of contained objects with their UIDs in parent
#   classes.
# - Reconstruction from JSON takes a list of objects (which must
#   have `uid` fields) and matches them to the UID values in the
#   sub-object list.
# - We make it an abstract base class to be consistent with NexusPy.

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
    when = datetime(2019, 1, 2, 3, 4, 5)
    rec = ChildRecord(uid="child01", when=when)
    as_dict = rec.to_dict()
    assert as_dict == {"uid": "child01", "when": when}

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

# ----------------------------------------------------------------------

# Persist a plain JSON blob to and from a SQLite database.

from sqlalchemy import create_engine, select, Column, String, TIMESTAMP
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.types import JSON

SqlBase = declarative_base()

@pytest.fixture
def engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    SqlBase.metadata.create_all(engine)
    return engine

class PlainJsonDB(SqlBase):
    __tablename__ = "plainjson"
    uid = Column(String(255), primary_key=True, nullable=False)
    data = Column(JSON)

    def __eq__(self, other):
        return (self.uid == other.uid) and (self.data == other.data)

    def __str__(self):
        return f"<PlainJsonDB/{self.uid}/{self.data}>"

def test_plain_json_db(engine):
    data = {"key": "value"}

    with Session(engine) as session:
        rec = PlainJsonDB(uid="plain01", data=data)
        session.add(rec)
        restored = session.query(PlainJsonDB).all()
        assert len(restored) == 1
        assert restored[0] == rec
        session.close()

# It's a little tricky to test SQLAlchemy because we have to compare
# objects within the scope of the session to avoid
# https://docs.sqlalchemy.org/en/14/errors.html#error-bhk3.  That's OK
# because `data` is all we really need, so let's simulate the
# functions we'd actually use.

def plainCreate(engine, data):
    with Session(engine) as session:
        rec = PlainJsonDB(uid=data["uid"], data=data)
        session.add(rec)
        session.commit()

def plainGet(engine, uid):
    with Session(engine) as session:
        results = session.query(PlainJsonDB)\
                         .filter(PlainJsonDB.uid == uid)\
                         .all()
        assert len(results) == 1
        return results[0].data

def plainExists(engine, uid):
    with Session(engine) as session:
        results = session.query(PlainJsonDB)\
                         .filter(PlainJsonDB.uid == uid)\
                         .all()
        return len(results) == 1

def test_plain_json_session_scoping(engine):
    original = {"uid": "plain01", "key": "value"}
    plainCreate(engine, original)

    restored = plainGet(engine, "plain01")
    assert restored == original

    assert plainExists(engine, "plain01")
    assert not plainExists(engine, "something else")

# Flat record persistence is a fairly straightforward extension of
# plain JSON.
# - We add a `created_at` field to the database, which is part of the
#   primary key.
# - We also add an `archived_at` field, which is `NULL` for the "active"
#   version of the record (at most one).
# - We have to do the object-to-dict conversion to handle complex types
#   like timestamps in the JSON blobs.

class FlatRecordDB(SqlBase):
    __tablename__ = "flatrecord"
    uid = Column(String(255), primary_key=True, nullable=False)
    data = Column(JSON)
    created_at = Column(TIMESTAMP, primary_key=True, nullable=False)
    archived_at = Column(TIMESTAMP)

    def __eq__(self, other):
        return (self.uid == other.uid) \
            and (self.data == other.data) \
            and (self.created_at == other.created_at) \
            and (self.archived_at == other.archived_at)

    def __str__(self):
        return f"<FlatRecordDB/{self.uid}/{self.data}/{self.created_at}/{self.archived_at}>"

def flatRecordCreate(engine, obj, when=None):
    when = when if (when is not None) else datetime.now() # to support testing
    as_dict = obj.to_dict()
    with Session(engine) as session:
        rec = FlatRecordDB(uid=as_dict["uid"], data=as_dict, created_at=when, archived_at=None)
        session.add(rec)
        session.commit()

def flatRecordGet(engine, uid, cls, archived=False):
    with Session(engine) as session:
        query = session.query(FlatRecordDB)\
                       .filter(FlatRecordDB.uid == uid)
        if not archived:
            query = query.filter(FlatRecordDB.archived_at == None)
        results = query.all()
        assert len(results) == 1
        return cls.from_dict(results[0].data)

def flatRecordGetAll(engine, cls):
    with Session(engine) as session:
        query = session.query(FlatRecordDB)
        results = query.all()
        return [cls.from_dict(r.data) for r in results]

def flatRecordExists(engine, uid, archived=False):
    with Session(engine) as session:
        query = session.query(FlatRecordDB)\
                       .filter(FlatRecordDB.uid == uid)
        if not archived:
            query = query.filter(FlatRecordDB.archived_at == None)
        results = query.all()
        return len(results) > 0

def flatRecordArchive(engine, uid, when=None):
    when = when if (when is not None) else datetime.now() # to support testing
    with Session(engine) as session:
        check = session.query(FlatRecordDB)\
                       .filter(FlatRecordDB.uid == uid)\
                       .filter(FlatRecordDB.archived_at is not None)
        results = check.all()
        assert len(results) == 1
        query = session.query(FlatRecordDB)\
                       .filter(FlatRecordDB.uid == uid)\
                       .filter(FlatRecordDB.archived_at is not None)\
                       .update({"archived_at": when})
        session.commit()
        return when

def test_flat_record_persistence(engine):
    original = FlatRecord(uid="flat01", name="flat", count=3)
    flatRecordCreate(engine, original)

    restored = flatRecordGet(engine, "flat01", FlatRecord)
    assert restored == original

    assert flatRecordExists(engine, "flat01")
    assert not flatRecordExists(engine, "something else")

def test_flat_record_archiving(engine):
    first = FlatRecord(uid="flat01", name="flat", count=3)
    flatRecordCreate(engine, first)
    assert flatRecordExists(engine, "flat01")

    now = datetime.now()
    assert flatRecordArchive(engine, "flat01", when=now) == now

    assert not flatRecordExists(engine, "flat01")
    assert flatRecordExists(engine, "flat01", True)

    second = FlatRecord(uid="flat01", name="flat", count=5)
    flatRecordCreate(engine, second)
    assert flatRecordExists(engine, "flat01")
    assert flatRecordGet(engine, "flat01", FlatRecord) == second
