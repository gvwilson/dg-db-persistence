#!/usr/bin/env python

'''Saving JSON with SQLAlchemy'''

import json
import sys

import sqlalchemy
from sqlalchemy import create_engine, insert, select, text
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.types import TypeDecorator, TEXT

# Echo SQL as we go?
echo = (len(sys.argv) > 1) and (sys.argv[1] == '--echo')

# Encode column as JSON.
class JSONColumn(TypeDecorator):
    """Persist a field as JSON."""

    impl = TEXT
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            temp = value.__dict__.copy()
            temp['__cls__'] = value.__class__.__name__
            value = json.dumps(temp)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
            cls = value['__cls__']
            del value['__cls__']
            cls = globals()[cls]
            value = cls(**value)
        return value


# Create the base for SQLAlchemy classes
SqlBase = declarative_base()

# Create the base class for JSONification
class JSONizable:
    pass

# Store details of an experiment
class Experiment(SqlBase, JSONizable):
    __tablename__ = 'experiment'
    name = Column(String(255), primary_key=True, nullable=False)
    details = Column(JSONColumn())

    def toJSON(self):
        return {'name': self.name, 'details': self.details}

    def __repr__(self):
        return f'<Experiment {self.name} {self.details}>'


# Store a particular experiment
class PotionDetails(JSONizable):
    def __init__(self, potion):
        self.potion = potion

    def toJSON(self):
        return {'potion': self.potion}

    def __repr__(self):
        return f'<Potion potion={self.potion}>'


# Define custom serialization
class MyJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        print('...obj', obj)
        if isinstance(obj, JSONizable):
            obj = obj.toJSON()
            print('...obj becomes', obj)
        return json.JSONEncoder.default(self, obj)


# Can I create directly?
exp = Experiment(name='direct', details=PotionDetails('hot sauce'))
print('direct creation', exp)
print('...as JSON', json.dumps(exp, cls=MyJsonEncoder))


# Create the database engine (in-memory SQLite for now)
engine = create_engine("sqlite+pysqlite:///:memory:", future=True, echo=echo)

# Create the table(s).
SqlBase.metadata.create_all(engine)

# Add some rows.
with Session(engine) as session:
    et = Experiment.__table__
    session.execute(insert(et).values(name='with potion',
                                      details=PotionDetails('syrup')))
    for (i, r) in enumerate(session.execute(select(Experiment))):
        print('object   :', i, r)
