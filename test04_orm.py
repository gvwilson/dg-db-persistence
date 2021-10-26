#!/usr/bin/env python

'''
Step 4: let's persist these to and from a SQL database. The top-level
class will map to a table; the details will be a JSON column. We'll
use an in-memory SQLite database for demo purposes; the same technique
works with PostgreSQL.
'''

import json
from sqlalchemy import create_engine, select, Column, String
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.types import TypeDecorator, TEXT

from util import JSON_CLS, show

class JSONizable:
    '''Mix-in class for identifying JSONizable objects.'''
    pass

class Encoder(json.JSONEncoder):
    '''Custom JSON encoder for ORM.'''

    def default(self, obj):
        '''Record encodable objects as dicts with class name.'''
        if isinstance(obj, JSONizable):
            class_name = obj.__class__.__name__
            obj = {key:obj.__dict__[key] for key in obj._enc}
            obj[JSON_CLS] = class_name
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


# Create the base for SQLAlchemy classes
SqlBase = declarative_base()

class Experiment(SqlBase, JSONizable):
    '''
    Experiment is the only ORMable class, but is also JSONizable.
    We will replace `_enc` later.
    '''

    _enc = ['name', 'details']
    
    __tablename__ = 'experiment'
    name = Column(String(255), primary_key=True, nullable=False)
    details = Column(JSONColumn())

    def __str__(self):
        return f'<Experiment name="{self.name}" details={self.details}>'

class DetailsTxt(JSONizable):
    '''
    Details with text only: not ORMable, but JSONizable.
    Again, we'll replace `_enc` later.
    '''

    _enc = ['text']

    def __init__(self, text):
        self.text = text

    def __str__(self):
        return f'<Text text="{self.text}">'

class DetailsNum(JSONizable):
    '''
    Details with number only: not ORMable, but JSONizable.
    Again, we'll replace `_enc` later.
    '''

    _enc = ['number']

    def __init__(self, number):
        self.number = number

    def __str__(self):
        return f'<Number number={self.number}>'


tests = [
    Experiment(name='with text', details=DetailsTxt('text content')),
    Experiment(name='with number', details=DetailsNum(1234))
]
print('== ORMable and JSONable.')
show('ORMable test cases', tests)
show('ORMable JSON persistence',
     [json.dumps(e, cls=Encoder) for e in tests])

# Create the database engine (in-memory SQLite for demo) and the tables.
engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
SqlBase.metadata.create_all(engine)

# Insert the objects we've created.
with Session(engine) as session:
    session.bulk_save_objects(tests)
    session.commit()

# Select rows back.
print('selecting back')
with Session(engine) as session:
    for (i, r) in enumerate(session.execute(select(Experiment))):
        print('..', i, r[0])
