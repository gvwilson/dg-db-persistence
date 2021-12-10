#!/usr/bin/env python

"""Persistence demo.

This demo shows how NexusPy uses dataclasses + SQLAlchemy to persist
to SQLite (in memory for testing) and PostgreSQL (for production). It
also shows how we do bulk import for data migration. Imports are mixed
with functions and running code for tutorial purposes.
"""

import sys
import pytest

# ----------------------------------------------------------------------

# We need to handle timestamps in objects (and in future may need to
# handle other types as well). Doing this with `dataclasses.asdict`
# didn't work: its `dict_factory` parameter doesn't do what we
# need. Luckily, there is a package called `dataclasses-json` that
# handles stuff correctly. If we stack the `@dataclass_json` decorator
# on top of the `@dataclass` decorator and declare `datetime` fields
# using `field` and `config` as shown below, we get the conversion
# machinery we need.

from datetime import datetime
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config

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
    expected_dict = {"uid": "abc123", "when": "2021-12-05T01:02:03"}

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
    expected_dict = {"uid": "abc123", "child": {"uid": "pqr789", "when": "2021-12-06T07:08:09"}}

    assert obj.to_dict() == expected_dict
    assert ConversionNested.from_dict(expected_dict) == obj

# ----------------------------------------------------------------------
    
# Create a base class for domain entities that has a unique ID
# field and persist to/from dictionaries and JSON strings.
# - Persistence takes an extra argument `replace_uid` which is
#   ignored here, but which will be used to trigger replacement
#   of contained objects with their UIDs in derived classes.
# - Reconstruction from JSON can take a list of objects (which must
#   have `uid` fields) and matches them to the UID values in the
#   sub-object list.
# - We use the names `to_nexus_dict` and `from_nexus_dict` because
#   `@dataclass_json` creates `to_dict` and `from_dict` directly on
#   this class, so we can't upcall to an inherited version.

from typing import List

@dataclass_json
@dataclass
class DomainEntity:
    uid: str

    @classmethod
    def from_nexus_dict(cls, src: dict, children: List=None):
        return cls.from_dict(src)

    def to_nexus_dict(self, replace_uid=False) -> dict:
        return self.to_dict()

# Let's test this with a flat record that does not contain sub-objects
# that need to be persisted separately (similar to `CellLine`). Note
# that we have to repeat the decorators even though it derives from
# `DomainEntity` so that `name` and `count` will be registered.

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

# Persist a plain JSON blob to and from a database using SQLAlchemy.
# No DomainEntity stuff is involved here: we'll connect that up later.

from sqlalchemy import create_engine, select, Column, String, TIMESTAMP
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.types import JSON

# The base class from which all SQLAlchemy classes must derive.
SqlBase = declarative_base()

# Create a fresh in-memory database for each test using a fixture.
@pytest.fixture
def engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    SqlBase.metadata.create_all(engine)
    return engine

# Let's persist some JSON. In production, we're going to put this
# class and others like it in a conditional load because we need
# slightly different types and queries for SQLite and PostgreSQL
# (because JSON handling isn't part of the standard.)
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
    
# Flat entity database persistence is a straightforward combination of
# the things we've built so far.
# - We add a `created_at` field to the database, which is part of the
#   primary key.
# - We also add an `archived_at` field, which is `NULL` for the "active"
#   version of the record (at most one).
# - We do the object-dict conversion when creating an object.

from sqlalchemy import UniqueConstraint

class FlatEntityDB(SqlBase):
    __tablename__ = "flat_table"
    uid = Column(String(16), primary_key=True, nullable=False)
    data = Column(JSON)
    created_at = Column(TIMESTAMP, primary_key=True, nullable=False)
    archived_at = Column(TIMESTAMP)
    __table_args__ = (UniqueConstraint('uid', 'created_at', name='flat_entity_uid_created_at_unique'),)

    def __eq__(self, other):
        return (self.uid == other.uid) \
            and (self.data == other.data) \
            and (self.created_at == other.created_at)

    def __str__(self):
        return f"<FlatEntityDB/{self.uid}/{self.data}/{self.created_at}/{self.archived_at}>"

# We optionally pass in the creation time `when` to make testing
# easier: the production version will use current UTC time.
def flatEntityCreate(engine, obj, when=None):
    when = when if (when is not None) else datetime.now()
    as_dict = obj.to_nexus_dict()
    with Session(engine) as session:
        rec = FlatEntityDB(uid=as_dict["uid"], data=as_dict, created_at=when, archived_at=None)
        session.add(rec)
        session.commit()

# We build the query that returns every match, then add a phrase to
# filter out archived entries if requested. In production, we'll
# raise a NexusException instead of asserting.
def flatEntityGet(engine, uid, cls, archived=False):
    with Session(engine) as session:
        query = session.query(FlatEntityDB)\
                       .filter(FlatEntityDB.uid == uid)
        if not archived:
            query = query.filter(FlatEntityDB.archived_at.is_(None))
        results = query.all()
        assert len(results) == 1
        return cls.from_nexus_dict(results[0].data)

# We can add an `archived` parameter to this in production if we want.
def flatEntityGetAll(engine, cls):
    with Session(engine) as session:
        query = session.query(FlatEntityDB)
        results = query.all()
        return [cls.from_nexus_dict(r.data) for r in results]

# This duplicates code from `flatEntityGet`; we can factor that
# into a utility function in production.
def flatEntityExists(engine, uid, archived=False):
    with Session(engine) as session:
        query = session.query(FlatEntityDB)\
                       .filter(FlatEntityDB.uid == uid)
        if not archived:
            query = query.filter(FlatEntityDB.archived_at.is_(None))
        results = query.all()
        return len(results) > 0

# This checks that the entity exists before setting its archived status;
# again, in production we'll raise a NexusException. The result is the
# archive timestamp because I can't think of anything better to return.
# Again, `when` is provided to simplify testing.
def flatEntityArchive(engine, uid, when=None):
    when = when if (when is not None) else datetime.now()
    with Session(engine) as session:
        check = session.query(FlatEntityDB)\
                       .filter(FlatEntityDB.uid == uid)\
                       .filter(FlatEntityDB.archived_at.is_(None))
        results = check.all()
        assert len(results) == 1
        query = session.query(FlatEntityDB)\
                       .filter(FlatEntityDB.uid == uid)\
                       .filter(FlatEntityDB.archived_at.is_(None))\
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
    original = FlatEntity(uid="flat01", name="flat", count=3)
    long_ago = datetime(2011, 1, 1, 1, 1, 1)
    flatEntityCreate(engine, original, when=long_ago)
    assert flatEntityExists(engine, "flat01")

    recently = datetime(2012, 2, 2, 2, 2, 2)
    assert flatEntityArchive(engine, "flat01", when=recently) == recently

    assert not flatEntityExists(engine, "flat01")
    assert flatEntityExists(engine, "flat01", True)

    replacement = FlatEntity(uid="flat01", name="flat", count=5)
    now = datetime(2021, 12, 12, 12, 12, 12)
    flatEntityCreate(engine, replacement, when=now)
    assert flatEntityExists(engine, "flat01")
    assert flatEntityGet(engine, "flat01", FlatEntity) == replacement

# ----------------------------------------------------------------------

# And now, nested records (in memory) with foreign-key relationships
# (in the database). First, create a child record that we can later
# include in a parent record (like a sample is part of a luciferase
# experiment). We give the child record a `datetime` field to exercise
# JSON persistence a little more.
    
from datetime import datetime

@dataclass_json
@dataclass
class ChildEntity(DomainEntity):
    when: datetime = field(
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat
        ))

def test_child_dict():
    when = datetime(2019, 1, 2, 3, 4, 5)
    expected = {"uid": "child01", "when": when.isoformat()}
    rec = ChildEntity(uid="child01", when=when)
    as_dict = rec.to_dict()
    assert as_dict == expected

    restored = ChildEntity.from_nexus_dict(as_dict)
    assert restored == rec

# A parent record contains a list of child records that need to be
# persisted separately (e.g., `LuciferaseExperiment` and its samples).
# We have to reconstitute the children in `from_dict` ourselves.

from typing import List, Union

@dataclass_json
@dataclass
class ParentEntity(DomainEntity):
    name: str
    children: List[Union[ChildEntity, str]]

    @classmethod
    def from_nexus_dict(cls, src: dict, children: List=None):
        result = cls(**src)
        if children:
            assert all(isinstance(c, str) for c in result.children), \
                "Cannot use non-string values to look up children by UID"
            by_uid = {c.uid:c for c in children}
            result.children = [by_uid[c] for c in result.children]
        else:
            assert all(isinstance(c, dict) for c in result.children), \
                "Cannot convert non-dict to child"
            result.children = [ChildEntity.from_nexus_dict(c) for c in result.children]
        return result

    def to_nexus_dict(self, replace_uid=False) -> dict:
        temp = self.to_dict()
        if replace_uid:
            temp["children"] = [c["uid"] for c in temp["children"]]
        return temp

@pytest.fixture
def first_time():
    return datetime(2019, 1, 2, 3, 4, 5)

@pytest.fixture
def second_time():
    return datetime(2020, 6, 7, 8, 9, 0)

@pytest.fixture
def first_child(first_time):
    return ChildEntity(uid="child01", when=first_time)

@pytest.fixture
def second_child(second_time):
    return ChildEntity(uid="child02", when=second_time)

def test_parent_dict_no_replacement(first_time, second_time, first_child, second_child):
    parent = ParentEntity(uid="parent01", name="parent", children=[first_child, second_child])
    expected = {"uid": "parent01", "name": "parent", "children": [
        {"uid": "child01", "when": first_time.isoformat()},
        {"uid": "child02", "when": second_time.isoformat()}
    ]}
    as_dict = parent.to_nexus_dict()
    assert as_dict == expected

    restored = ParentEntity.from_nexus_dict(as_dict)
    assert restored == parent

def test_parent_dict_with_replacement(first_time, second_time, first_child, second_child):
    parent = ParentEntity(uid="parent01", name="parent", children=[first_child, second_child])
    expected = {"uid": "parent01", "name": "parent", "children": ["child01", "child02"]}

    as_dict = parent.to_nexus_dict(replace_uid=True)
    assert as_dict == expected

    restored = ParentEntity.from_nexus_dict(as_dict, [first_child, second_child])
    assert restored == parent
    assert all(isinstance(c, ChildEntity) for c in restored.children)

# ----------------------------------------------------------------------

# Now that parent/child JSON persistence is working, let's make it
# work in the ORM. We want a many-to-one relationship from `ChildDB`
# to `ParentDB`, and a one-to-many relationship from `ParentDB` to
# `ChildDB` so that we can do lookups in both directions.

# To achieve this, we need two things to `ChildDB`:
# - `parent_id` creates the foreign key into `ParentDB`
# - `parent` defines the relationship (and back ref) to the application
#
# Since the parent's primary key has two parts, we have to use
# `ForeignKeyConstraint` instead of the simpler `ForeignKey`: see
# https://docs.sqlalchemy.org/en/14/core/constraints.html?highlight=foreignkeyconstraint#metadata-foreignkeys
# for details.

from sqlalchemy import ForeignKey # FIXME
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.orm import relationship

class ParentDB(SqlBase):
    __tablename__ = "parent_table"
    uid = Column(String(16), primary_key=True, nullable=False)
    data = Column(JSON)
    created_at = Column(TIMESTAMP, primary_key=True, nullable=False)
    archived_at = Column(TIMESTAMP)
    __table_args__ = (
        UniqueConstraint('uid', 'created_at', name='parent_db_uid_created_at_unique'),
    )

class ChildDB(SqlBase):
    __tablename__ = "child_table"
    uid = Column(String(16), primary_key=True, nullable=False)
    created_at = Column(TIMESTAMP, primary_key=True, nullable=False)
    data = Column(JSON)
    archived_at = Column(TIMESTAMP)

    parent_uid = Column(String(16))
    parent_created_at = Column(TIMESTAMP)
    parent = relationship("ParentDB", backref="child")

    __table_args__ = (
        UniqueConstraint('uid', 'created_at', name='child_db_uid_created_at_unique'),
        ForeignKeyConstraint(["parent_uid", "parent_created_at"],
                             ["parent_table.uid", "parent_table.created_at"])
    )

    def __str__(self):
        return f"<ChildDB {self.uid}/{self.data}/@{self.parent_uid}>"

def test_parent_child_relationship(engine):
    with Session(engine) as session:
        when = datetime(2021, 11, 12, 9, 8, 7)
        p = ParentDB(uid="p123", created_at=when, data={"parent": "data"})
        session.add(p)
        first = ChildDB(uid="c01", created_at=when, data={"child": "first"},
                        parent_uid="p123", parent_created_at=when)
        session.add(first)
        second = ChildDB(uid="c02", created_at=when, data={"child": "second"},
                         parent_uid="p123", parent_created_at=when)
        session.add(second)
        session.commit()

    with Session(engine) as session:
        records = session.query(ParentDB)\
            .join(ChildDB, ParentDB.uid == ChildDB.parent_uid)\
            .filter(ChildDB.uid == "c01")\
            .all()
        assert len(records) == 1
        assert records[0].uid == "p123"

# ----------------------------------------------------------------------

# Let's combine JSON and ORM persistence for experiments and samples.
# This is what the production code will look like (except classes will
# have many more fields).

# SampleEntity looks like our ChildEntity.
@dataclass_json
@dataclass
class SampleEntity(DomainEntity):
    measure: int

# LuciferaseEntity is like our ParentEntity.
@dataclass_json
@dataclass
class LuciferaseEntity(DomainEntity):
    name: str
    samples: List[Union[SampleEntity, str]]

    @classmethod
    def from_nexus_dict(cls, src: dict, samples: List=None):
        result = cls(**src)
        if samples:
            assert all(isinstance(s, str) for s in result.samples), \
                "Cannot use non-string values to look up samples by UID"
            by_uid = {s.uid:s for s in samples}
            result.samples = [by_uid[s] for s in result.samples]
        else:
            assert all(isinstance(s, dict) for s in result.samples), \
                "Cannot convert non-dict to sample"
            result.samples = [SampleEntity.from_nexus_dict(s) for s in result.samples]
        return result

    def to_nexus_dict(self, replace_uid=False) -> dict:
        temp = self.to_dict()
        if replace_uid:
            temp["samples"] = [s["uid"] for s in temp["samples"]]
        return temp

# LuciferaseDB maps LuciferaseEntity to the database.
class LuciferaseDB(SqlBase):
    __tablename__ = "luciferase_table"
    uid = Column(String(16), primary_key=True, nullable=False)
    data = Column(JSON)
    created_at = Column(TIMESTAMP, primary_key=True, nullable=False)
    archived_at = Column(TIMESTAMP)
    __table_args__ = (
        UniqueConstraint('uid', 'created_at', name='luciferase_uid_created_at_unique'),
    )

    def __str__(self):
        return f"<LuciferaseDB {self.uid}/{self.data}>"

# SampleDB maps a single sample to the database.
class SampleDB(SqlBase):
    __tablename__ = "sample_table"
    uid = Column(String(16), primary_key=True, nullable=False)
    created_at = Column(TIMESTAMP, primary_key=True, nullable=False)
    data = Column(JSON)
    archived_at = Column(TIMESTAMP)

    luciferase_uid = Column(String(16))
    luciferase_created_at = Column(TIMESTAMP)
    parent = relationship("LuciferaseDB", backref="child")

    __table_args__ = (
        UniqueConstraint('uid', 'created_at', name='sample_db_uid_created_at_unique'),
        ForeignKeyConstraint(["luciferase_uid", "luciferase_created_at"],
                             ["luciferase_table.uid", "luciferase_table.created_at"])
    )

    def __str__(self):
        return f"<SampleDB {self.uid}/{self.data}/@{self.luciferase_uid}>"

# Create a database record, replacing contained samples with their IDs
# in the luciferase JSON record and saving the samples separately.
def dbLuciferaseCreate(engine, luc, when=None):
    when = when if (when is not None) else datetime.now()
    luc_dict = luc.to_nexus_dict(replace_uid=True)
    sample_dicts = [sample.to_nexus_dict() for sample in luc.samples]
    with Session(engine) as session:
        rec = LuciferaseDB(uid=luc_dict["uid"], data=luc_dict, created_at=when)
        session.add(rec)
        for sample in sample_dicts:
            rec = SampleDB(uid=sample["uid"], data=sample, created_at=when,
                           luciferase_uid=luc_dict["uid"])
            session.add(rec)
        session.commit()

# Get a luciferase object, filling in the samples by grabbing their
# records from the database and inserting them into the main record.
# We can add a flag to skip this step, in which case the JSON will
# have sample UIDs rather than sample objects, but that seems like
# it would be error-prone.
def dbLuciferaseGet(engine, uid, archived=False):
    with Session(engine) as session:
        query = session.query(SampleDB)\
            .filter(SampleDB.luciferase_uid == uid)
        if not archived:
            query = query.filter(SampleDB.archived_at == None)
        samples = query.all()
        samples = [SampleEntity.from_nexus_dict(s.data) for s in samples]

        query = session.query(LuciferaseDB)\
            .filter(LuciferaseDB.uid == uid)
        if not archived:
            query = query.filter(LuciferaseDB.archived_at == None)
        results = query.all()
        assert len(results) == 1
        luc = LuciferaseEntity.from_nexus_dict(results[0].data, samples)

        return luc

def test_luciferase(engine):
    when = datetime(2021, 12, 15, 7, 7, 7)
    sample = SampleEntity(uid="sample0001", measure=123)
    luc = LuciferaseEntity(uid="luc987", name="luci", samples=[sample])
    dbLuciferaseCreate(engine, luc, when)

    # Recover data directly to check creation
    with Session(engine) as session:
        recovered = session.query(LuciferaseDB).all()
        assert len(recovered) == 1
        assert recovered[0].uid == "luc987"
        assert recovered[0].data == {"uid": "luc987", "name": "luci", "samples": ["sample0001"]}

        recovered = session.query(SampleDB).all()
        assert len(recovered) == 1
        assert recovered[0].uid == "sample0001"
        assert recovered[0].data == {"uid": "sample0001", "measure": 123}

    # Test our own getter
    recovered = dbLuciferaseGet(engine, "luc987")
    assert isinstance(recovered, LuciferaseEntity)
    assert recovered.uid == "luc987"
    assert recovered.name == "luci"
    assert len(recovered.samples) == 1
    assert isinstance(recovered.samples[0], SampleEntity)
    assert recovered.samples[0].uid == "sample0001"

# ----------------------------------------------------------------------

# Does this work with PostgreSQL? This code assumes PostgreSQL is running
# locally, that you are logged in as <NAME>, and that there is a database
# called <NAME>.

from conftest import skip_psql_tests
import os
import pwd

@pytest.fixture
def psql_engine():
    username = pwd.getpwuid(os.getuid()).pw_name
    engine = create_engine(f"postgresql://{username}:@localhost:5432/{username}", future=True)
    SampleDB.__table__.drop(engine, checkfirst=True)
    LuciferaseDB.__table__.drop(engine, checkfirst=True)
    LuciferaseDB.__table__.create(engine)
    SampleDB.__table__.create(engine)
    return engine

@pytest.mark.skipif(skip_psql_tests, reason="You told me to")
def test_psql(psql_engine):
    when = datetime(2021, 12, 15, 7, 7, 7)
    sample = SampleEntity(uid="sample0001", measure=123)
    luc = LuciferaseEntity(uid="luc987", name="luci", samples=[sample])

    dbLuciferaseCreate(psql_engine, luc, when)
    recovered = dbLuciferaseGet(psql_engine, "luc987")

    assert isinstance(recovered, LuciferaseEntity)
    assert recovered.uid == "luc987"
    assert recovered.name == "luci"
    assert len(recovered.samples) == 1
    assert isinstance(recovered.samples[0], SampleEntity)
    assert recovered.samples[0].uid == "sample0001"
