#!/usr/bin/env python

"""Persistence demo.

This demo shows how NexusPy uses Pydantic + SQLAlchemy to persist to
SQLite (in memory for testing) and PostgreSQL (for production). It
also shows how we do bulk import for data migration. Imports are mixed
with functions and running code for tutorial purposes."""

import sys
import pytest

# ----------------------------------------------------------------------

# We need to handle timestamps in objects (and in future may need to handle
# other types as well). Doing this with `dataclasses.asdict` proved difficult
# (the `dict_factory` that `asdict` takes doesn't do what we need). Luckily,
# there is a package called `dataclasses-json` that does. If we stack the
# `@dataclass_json` decorator on top of the `@dataclass` decorator and
# declare `datetime` fields using `field` and `config` as shown below, we get
# the conversion machinery we need.

from datetime import datetime
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config

@dataclass_json
@dataclass
class ConversionStringOnly():
    uid: str

def test_conversion_string_only():
    obj = ConversionStringOnly(uid="abc123")
    expected_dict = {"uid": "abc123"}
    expected_str = '{"uid": "abc123"}'

    assert obj.to_dict() == expected_dict
    assert ConversionStringOnly.from_dict(expected_dict) == obj

    assert obj.to_json() == expected_str
    assert ConversionStringOnly.from_json(expected_str) == obj

@dataclass_json
@dataclass
class ConversionStringAndDatetime():
    uid: str
    when: datetime = field(
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat
        ))

def test_conversion_with_datetime():
    now = datetime(2021, 12, 5, 1, 2, 3)
    obj = ConversionStringAndDatetime(uid="abc123", when=now)
    expected_dict = {'uid': 'abc123', 'when': '2021-12-05T01:02:03'}

    assert obj.to_dict() == expected_dict
    assert ConversionStringAndDatetime.from_dict(expected_dict) == obj

@dataclass_json
@dataclass
class ConversionNested():
    uid: str
    child: ConversionStringAndDatetime

def test_conversion_nested():
    now = datetime(2021, 12, 6, 7, 8, 9)
    obj = ConversionNested(uid="abc123", child=ConversionStringAndDatetime(uid="pqr789", when=now))
    expected_dict = {'uid': 'abc123', 'child': {'uid': 'pqr789', 'when': '2021-12-06T07:08:09'}}

    assert obj.to_dict() == expected_dict
    assert ConversionNested.from_dict(expected_dict) == obj

# ----------------------------------------------------------------------
    
# Create a base class for domain entities that has a unique ID
# field and persist to/from dictionaries and JSON strings.
# - Persistence takes an extra argument `replace_uid` which is
#   ignored here, but which will be used to trigger replacement
#   of contained objects with their UIDs in some derived classes.
# - Reconstruction from JSON takes a list of objects (which must
#   have `uid` fields) and matches them to the UID values in the
#   sub-object list.
# - We use names like `to_nexus_dict` because `@dataclass_json`
#   creates `to_dict` on this class, and we can't upcall to it.
# - So we use `_nexus_` in the names of all the other methods to
#   be clear.

@dataclass_json
@dataclass
class DomainEntity:
    uid: str

    @classmethod
    def from_nexus_dict(cls, src: dict):
        return cls.from_dict(src)

    def to_nexus_dict(self, replace_uid=False) -> dict:
        return self.to_dict()

# A flat record does not contain sub-objects that need to be persisted
# separately (e.g., `CellLine`). It has a name and a count so that we
# can test uniqueness constraints.  Note that we have to repeat the
# decorators even though it derives from `DomainEntity`..

@dataclass_json
@dataclass
class FlatEntity(DomainEntity):
    name: str
    count: int

def test_entity_nexus_dict():
    rec = FlatEntity(uid="flat01", name="flat", count=3)
    as_dict = rec.to_nexus_dict()
    assert as_dict == {"uid": "flat01", "name": "flat", "count": 3}

    restored = FlatEntity.from_nexus_dict(as_dict)
    assert restored == rec

# ----------------------------------------------------------------------

# Persist a plain JSON blob to and from a SQLite database.  No
# DomainEntity stuff is involved here: we'll connect the two later.

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
    uid = Column(String(16), primary_key=True, nullable=False)
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

# ----------------------------------------------------------------------
    
# Flat entity database persistence is a fairly straightforward extension of
# plain JSON.
# - We add a `created_at` field to the database, which is part of the
#   primary key.
# - We also add an `archived_at` field, which is `NULL` for the "active"
#   version of the record (at most one).
# - We do the object-dict conversion to handle complex types like timestamps.
# - We *don't* check alignment between the DomainEntity class and the
#   SQLAlchemy class, but we could (and should?).

class FlatEntityDB(SqlBase):
    __tablename__ = "flat_table"
    uid = Column(String(16), primary_key=True, nullable=False)
    data = Column(JSON)
    created_at = Column(TIMESTAMP, primary_key=True, nullable=False)
    archived_at = Column(TIMESTAMP)

    def __eq__(self, other):
        return (self.uid == other.uid) \
            and (self.data == other.data) \
            and (self.created_at == other.created_at) \
            and (self.archived_at == other.archived_at)

    def __str__(self):
        return f"<FlatEntityDB/{self.uid}/{self.data}/{self.created_at}/{self.archived_at}>"

def flatEntityCreate(engine, obj, when=None):
    when = when if (when is not None) else datetime.now() # to support testing
    as_dict = obj.to_dict()
    with Session(engine) as session:
        rec = FlatEntityDB(uid=as_dict["uid"], data=as_dict, created_at=when, archived_at=None)
        session.add(rec)
        session.commit()

def flatEntityGet(engine, uid, cls, archived=False):
    with Session(engine) as session:
        query = session.query(FlatEntityDB)\
                       .filter(FlatEntityDB.uid == uid)
        if not archived:
            query = query.filter(FlatEntityDB.archived_at == None)
        results = query.all()
        assert len(results) == 1
        return cls.from_dict(results[0].data)

def flatEntityGetAll(engine, cls):
    with Session(engine) as session:
        query = session.query(FlatEntityDB)
        results = query.all()
        return [cls.from_dict(r.data) for r in results]

def flatEntityExists(engine, uid, archived=False):
    with Session(engine) as session:
        query = session.query(FlatEntityDB)\
                       .filter(FlatEntityDB.uid == uid)
        if not archived:
            query = query.filter(FlatEntityDB.archived_at == None)
        results = query.all()
        return len(results) > 0

def flatEntityArchive(engine, uid, when=None):
    when = when if (when is not None) else datetime.now() # to support testing
    with Session(engine) as session:
        check = session.query(FlatEntityDB)\
                       .filter(FlatEntityDB.uid == uid)\
                       .filter(FlatEntityDB.archived_at is not None)
        results = check.all()
        assert len(results) == 1
        query = session.query(FlatEntityDB)\
                       .filter(FlatEntityDB.uid == uid)\
                       .filter(FlatEntityDB.archived_at is not None)\
                       .update({"archived_at": when})
        session.commit()
        return when

def test_flat_persistence(engine):
    original = FlatEntity(uid="flat01", name="flat", count=3)
    flatEntityCreate(engine, original)

    restored = flatEntityGet(engine, "flat01", FlatEntity)
    assert restored == original

    assert flatEntityExists(engine, "flat01")
    assert not flatEntityExists(engine, "something else")

def test_flat_archiving(engine):
    first = FlatEntity(uid="flat01", name="flat", count=3)
    flatEntityCreate(engine, first)
    assert flatEntityExists(engine, "flat01")

    now = datetime.now()
    assert flatEntityArchive(engine, "flat01", when=now) == now

    assert not flatEntityExists(engine, "flat01")
    assert flatEntityExists(engine, "flat01", True)

    second = FlatEntity(uid="flat01", name="flat", count=5)
    flatEntityCreate(engine, second)
    assert flatEntityExists(engine, "flat01")
    assert flatEntityGet(engine, "flat01", FlatEntity) == second

# ----------------------------------------------------------------------

# And now, nested records (in memory) with foreign-key relationships
# (in the database). First, create a child record that we can later
# include in a parent record (like a sample is part of a luciferase
# experiment). We give the child record a `datetime` field to exercise
# JSON persistence a little more.
    
from datetime import datetime

@dataclass_json
@dataclass
class ChildRecord(DomainEntity):
    when: datetime

def test_child_dict():
    when = datetime(2019, 1, 2, 3, 4, 5)
    rec = ChildRecord(uid="child01", when=when)
    as_dict = rec.to_dict()
    assert as_dict == {"uid": "child01", "when": when}

    restored = ChildRecord.from_dict(as_dict)
    assert restored == rec

# HERE
    
# A parent record contains a list of child records that need
# to be persisted separately (e.g., `LuciferaseExperiment` and
# its samples).
# - We have to reconstitute the children in `from_dict` ourselves.
# - We implement the `replace_uid` parameter of `to_json`, so `children`
#   can hold either `ChildRecord` or `str` (UIDs).

from typing import List, Union

@dataclass_json
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

    def to_dict(self, replace_uid=False) -> dict:
        temp = asdict(self)
        if replace_uid:
            temp["children"] = [c["uid"] for c in temp["children"]]
        return temp

    def to_json(self, replace_uid=False) -> str:
        return json_dump(self.to_dict(replace_uid))

    @classmethod
    def from_json(cls, src: str, contained=None):
        temp = json_load(src)
        if contained is not None:
            contained = {c.uid: c for c in contained}
            temp["children"] = [contained[c] for c in temp["children"]]
        return cls.from_dict(temp)

@pytest.fixture
def first_time():
    return datetime(2019, 1, 2, 3, 4, 5)

@pytest.fixture
def second_time():
    return datetime(2020, 6, 7, 8, 9, 0)

@pytest.fixture
def first_child(first_time):
    return ChildRecord(uid="child01", when=first_time)

@pytest.fixture
def second_child(second_time):
    return ChildRecord(uid="child02", when=second_time)

@pytest.fixture
def parent(first_child, second_child):
    return ParentRecord(uid="parent01", name="parent", children=[first_child, second_child])

def test_parent_record_dict_json(first_time, second_time, first_child, second_child, parent):
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

# Now that the parent/child relationship is working, let's stuff them in
# the database. We're putting all the child data in the JSON blob, so
# the fields are the same as they are for a flat record, *except* for a
# parent ID column.

from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

class ParentRecordDB(SqlBase):
    __tablename__ = "parent_table"
    uid = Column(String(16), primary_key=True, nullable=False)
    data = Column(JSON)
    created_at = Column(TIMESTAMP, primary_key=True, nullable=False)
    archived_at = Column(TIMESTAMP)
    children = relationship("ChildRecordDB")

class ChildRecordDB(SqlBase):
    __tablename__ = "child_table"
    uid = Column(String(16), primary_key=True, nullable=False)
    created_at = Column(TIMESTAMP, primary_key=True, nullable=False)
    data = Column(JSON)
    archived_at = Column(TIMESTAMP)
    parent_id = Column(String(16), ForeignKey("parent_table.uid"))

def parentRecordCreate(engine, obj, when=None):
    when = when if (when is not None) else datetime.now() # to support testing
    as_dict = obj.to_dict(replace_uid=True)
    print("PARENT AS DICT", as_dict)
    with Session(engine) as session:
        parentDB = ParentRecordDB(uid=obj.uid, data=as_dict, created_at=when, archived_at=None)
        session.add(parentDB)
        for child in obj.children:
            as_dict = child.to_dict()
            print("CHILD AS DICT", as_dict)
            childDB = ChildRecordDB(uid=child.uid, data=as_dict, created_at=when, archived_at=None, parent_id=obj.uid)
            session.add(childDB)
        print("ABOUT TO COMMIT WHEN CREATING")
        session.commit()
        print("COMMITTED WHEN CREATING")

def parentRecordGet(engine, uid, cls, archived=False):
    with Session(engine) as session:
        query = session.query(ParentRecordDB)\
                       .filter(ParentRecordDB.uid == uid)
        if not archived:
            query = query.filter(ParentRecordDB.archived_at == None)
        results = query.all()
        assert len(results) == 1
        return cls.from_dict(results[0].data)

def test_nested_record_persistence(engine, parent, first_child, second_child):
    parentRecordCreate(engine, parent)
    restored = parentRecordGet(engine, parent.uid, ParentRecord)
    assert restored == parent
