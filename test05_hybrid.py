#!/usr/bin/env python

'''
Step 5: Add type-checking with Pydantic. We keep OrmExperiment,
OrmDetailsTxt, and OrmDetailsNum as they were, but mirror them with
Pydantic classes as described in

https://pydantic-docs.helpmanual.io/usage/models/#orm-mode-aka-arbitrary-class-instances.

We will also get rid of the `_enc` field that's been following us around.
'''

import json
from pydantic import BaseModel, ValidationError
from sqlalchemy import create_engine, select, Column, String
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.types import TypeDecorator, TEXT

from util import JSON_CLS, show

# Create the base for SQLAlchemy classes
SqlBase = declarative_base()

class Encoder(json.JSONEncoder):
    '''Custom JSON encoder for ORM.'''

    def default(self, obj):
        '''Use Pydantic's BaseModel to signal JSONizability.'''
        if isinstance(obj, BaseModel):
            class_name = obj.__class__.__name__
            obj = obj.dict()
            obj[JSON_CLS] = class_name
        elif isinstance(obj, SqlBase):
            columns = obj.__table__.columns.keys()
            class_name = obj.__class__.__name__
            obj = {key:getattr(obj, key) for key in columns}
            obj[JSON_CLS] = class_name
        else:
             return json.JSONEncoder.default(self, obj) # raises TypeError
        return obj


class JSONColumn(TypeDecorator):
    """Persist a field as JSON."""

    impl = TEXT
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value, cls=Encoder)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
            cls = value[JSON_CLS]
            del value[JSON_CLS]
            cls = globals()[cls] # I should be ashamed of doing this…
            value = cls(**value)
        return value


class Experiment(SqlBase):
    '''
    Experiment is the only ORMable class, but it is _not_ directly JSONizable.
    '''

    __tablename__ = 'experiment'
    name = Column(String(255), primary_key=True, nullable=False)
    details = Column(JSONColumn())

    def __repr__(self):
        return f'<Experiment name="{self.name}" details={self.details}>'

class DetailsTxt(BaseModel):
    '''
    Pydantic representation of textual experimental details.
    '''
    text: str


class DetailsNum(BaseModel):
    '''
    Pydantic representation of numerical experimental details.
    '''
    number: int

# Can't violate Pydantic constraints.
print('== Pydantic')
try:
    DetailsNum(number='not a number')
except ValidationError as e:
    print('trying to create invalid Pydantic field:', e)

# Tests that work.
tests = [
    Experiment(name='with text', details=DetailsTxt(text='text content')),
    Experiment(name='with number', details=DetailsNum(number=1234))
]
show('test cases (objects)', tests)
show('JSON persistence',
     [json.dumps(e, cls=Encoder) for e in tests])

# Create the database engine (in-memory SQLite for demo) and the tables.
engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
SqlBase.metadata.create_all(engine)

# Insert the objects we've created.
with Session(engine) as session:
    session.bulk_save_objects(tests)
    session.commit()

# Select rows back.
print('SQL persistence')
with Session(engine) as session:
    for (i, r) in enumerate(session.execute(select(Experiment))):
        print('row:', i, r)
