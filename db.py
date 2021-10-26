#!/usr/bin/env python

import sys

# Echo SQL as we go?
Echo = (len(sys.argv) > 1) and (sys.argv[1] == '--echo')

# Special field encoding class name for JSON persistence.
JSON_CLS = '_json_cls'

def show(title, values):
    '''Show a list of values.'''
    print(title)
    for v in values:
        print('..', v)

# ----------------------------------------------------------------------
# Step 1: the classes we want, in-memory only.

class MemExperiment:
    '''An experiment has a name and some details.'''

    def __init__(self, name, details):
        self.name = name
        self.details = details

    def __repr__(self):
        return f'<{self.__class__.__name__} name="{self.name}" details={self.details}>'

class MemDetailsTxt:
    '''Details with text only.'''

    def __init__(self, text):
        self.text = text

    def __repr__(self):
        return f'<{self.__class__.__name__} text="{self.text}">'

class MemDetailsNum:
    '''Details with number only.'''

    def __init__(self, number):
        self.number = number

    def __repr__(self):
        return f'<{self.__class__.__name__} number={self.number}>'

print('== In-memory tests')
show('in-memory', [
    MemExperiment('with text', MemDetailsTxt('text content')),
    MemExperiment('with number', MemDetailsNum(1234))
])
print()

# ----------------------------------------------------------------------
# Step 2: try to persist in-memory classes as JSON.

import json

print('== Failed attempt to persist in-memory classes as JSON.')
m = MemExperiment('with text', MemDetailsTxt('text content')),
try:
    m_as_text = json.dumps(m)
except TypeError as e:
    print('direct in-memory to JSON:', e)
print()

# ----------------------------------------------------------------------
# Step 3: custom serializer. In practice, we would give the original
# classes multiple parent classes, and later on we'll rely on the fact
# that Pydantic identifies fields for encoding rather than listing them
# explicitly with `_enc`.

class Encodable:
    '''Empty base class to make encodable classes identifiable.'''
    pass

class EncExperiment(MemExperiment, Encodable):
    _enc = ['name', 'details']

class EncDetailsTxt(MemDetailsTxt, Encodable):
    _enc = ['text']

class EncDetailsNum(MemDetailsNum, Encodable):
    _enc = ['number']

class EncEncoder(json.JSONEncoder):
    '''Custom JSON encoder.'''

    def default(self, obj):
        '''Record encodable objects as dicts with class name.'''
        if isinstance(obj, Encodable):
            class_name = obj.__class__.__name__
            obj = {key:obj.__dict__[key] for key in obj._enc}
            obj[JSON_CLS] = class_name
        return obj


enc_tests = [
    EncExperiment('with dictionary', {'k': 0}),
    EncExperiment('with text', EncDetailsTxt('text content')),
    EncExperiment('with number', EncDetailsNum(1234))
]
print('== Custom JSON encoding.')
show('encodable test cases', enc_tests)
show('encoded JSON persistence',
     [json.dumps(e, cls=EncEncoder) for e in enc_tests])
print()

# ----------------------------------------------------------------------
# Step 4: let's persist these to and from a SQL database. The top-level
# class will map to a table; the details will be a JSON column. We'll
# use an in-memory SQLite database for demo purposes; the same technique
# works with PostgreSQL.

from sqlalchemy import create_engine, select, Column, String
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.types import TypeDecorator, TEXT

class JSONizable:
    '''Mix-in class for identifying JSONizable objects.'''
    pass

class OrmJSONEncoder(json.JSONEncoder):
    '''Custom JSON encoder for ORM.'''

    def default(self, obj):
        '''Record encodable objects as dicts with class name.'''
        if isinstance(obj, JSONizable):
            class_name = obj.__class__.__name__
            obj = {key:obj.__dict__[key] for key in obj._enc}
            obj[JSON_CLS] = class_name
        return obj


class OrmJSONColumn(TypeDecorator):
    """Persist a field as JSON."""

    impl = TEXT
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value, cls=OrmJSONEncoder)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
            cls = value[JSON_CLS]
            del value[JSON_CLS]
            cls = globals()[cls]
            value = cls(**value)
        return value


# Create the base for SQLAlchemy classes
SqlBase = declarative_base()

class OrmExperiment(SqlBase, JSONizable):
    '''
    Experiment is the only ORMable class, but is also JSONizable.
    We will replace `_enc` later.
    '''

    _enc = ['name', 'details']
    
    __tablename__ = 'orm_experiment'
    name = Column(String(255), primary_key=True, nullable=False)
    details = Column(OrmJSONColumn())

    def __repr__(self):
        return f'<OrmExperiment name="{self.name}" details={self.details}>'

class OrmDetailsTxt(JSONizable):
    '''
    Details with text only: not ORMable, but JSONizable.
    Again, we'll replace `_enc` later.
    '''

    _enc = ['text']

    def __init__(self, text):
        self.text = text

    def __repr__(self):
        return f'<Text text="{self.text}">'

class OrmDetailsNum(JSONizable):
    '''
    Details with number only: not ORMable, but JSONizable.
    Again, we'll replace `_enc` later.
    '''

    _enc = ['number']

    def __init__(self, number):
        self.number = number

    def __repr__(self):
        return f'<Number number={self.number}>'


orm_tests = [
    OrmExperiment(name='with text', details=OrmDetailsTxt('text content')),
    OrmExperiment(name='with number', details=OrmDetailsNum(1234))
]
print('== ORMable and JSONable.')
show('ORMable test cases', orm_tests)
show('ORMable JSON persistence',
     [json.dumps(e, cls=OrmJSONEncoder) for e in orm_tests])

# Create the database engine (in-memory SQLite for demo) and the tables.
engine = create_engine("sqlite+pysqlite:///:memory:", future=True, echo=Echo)
SqlBase.metadata.create_all(engine)

# Insert the objects we've created.
with Session(engine) as session:
    session.bulk_save_objects(orm_tests)
    session.commit()

# Select rows back.
with Session(engine) as session:
    for (i, r) in enumerate(session.execute(select(OrmExperiment))):
        print('row:', i, r)

print()

# ----------------------------------------------------------------------
# Step 5: Add type-checking with Pydantic. We keep OrmExperiment,
# OrmDetailsTxt, and OrmDetailsNum as they were, but mirror them with
# Pydantic classes as described in
# https://pydantic-docs.helpmanual.io/usage/models/#orm-mode-aka-arbitrary-class-instances.
# We will also get rid of the `_enc` field that's been following us around.

from pydantic import BaseModel
from typing import Any

class ModelJSONEncoder(json.JSONEncoder):
    '''Custom JSON encoder for Pydantic model classes.'''

    def default(self, obj):
        '''
        Record encodable objects as dicts with class name.
        Instead of using a manual field `_enc`, this version
        relies on the __fields__ property.
        '''
        if isinstance(obj, BaseModel):
            class_name = obj.__class__.__name__
            obj = obj.__fields__.copy()
            obj[JSON_CLS] = class_name
        return obj


class ModelJSONColumn(TypeDecorator):
    """Persist a field as JSON."""

    impl = TEXT
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value, cls=ModelJSONEncoder)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
            cls = value[JSON_CLS]
            del value[JSON_CLS]
            cls = globals()[cls]
            value = cls(**value)
        return value


class ModelOrmExperiment(SqlBase):
    '''ORMable.'''
    
    __tablename__ = 'pydantic_experiment'
    name = Column(String(255), primary_key=True, nullable=False)
    details = Column(ModelJSONColumn())

    def __repr__(self):
        return f'<ModelOrmExperiment name="{self.name}" details={self.details}>'

class ModelDetailsTxt(BaseModel):
    '''This class is *not* ORM mode, but it *is* BaseModel (so JSON persistable).'''
    text: str

class ModelDetailsNum(BaseModel):
    '''This class is *not* ORM mode, but it *is* BaseModel (so JSON persistable).'''
    number: int

# Build instances for database persistence.
model_tests = [
    ModelOrmExperiment(name='with text', details=ModelDetailsTxt('text content')),
    ModelOrmExperiment(name='with number', details=ModelDetailsNum(1234))
]
print('== ORM versions of models')
show('model test cases (ORM version)', model_tests)

# Test database persistence.
engine = create_engine("sqlite+pysqlite:///:memory:", future=True, echo=Echo)
SqlBase.metadata.create_all(engine)
with Session(engine) as session:
    session.bulk_save_objects(orm_tests)
    session.commit()
print('testing database persistence')
with Session(engine) as session:
    for (i, r) in enumerate(session.execute(select(ModelOrmExperiment))):
        print('row:', i, r)


class ModelPydanticExperiment(BaseModel):
    name: str # should add a length constraint
    details: Any # should add a Type[BaseClass] constraint
    class Config:
        orm_mode = True
