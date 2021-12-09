#!/usr/bin/env python

'''
Step 6: let's persist raw JSON blobs using PostgreSQL and
try to look them up.
'''

import json
from sqlalchemy import create_engine, Column, MetaData, String, Table, delete, insert, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, Session
import sys

from util import show


USER = 'gregwilson'
PASSWORD = 'postgresql'
HOST = '127.0.0.1'
PORT = 5432
DATABASE = 'learnjson'


# Create the PostgreSQL database engine.
engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}', future=True)

# Create the base for SQLAlchemy classes
SqlBase = declarative_base()
class Experiment(SqlBase):
    __tablename__ = 'experiments'
    name = Column(String, primary_key=True)
    details = Column(JSONB)

    def __repr__(self):
        return f'Experiment name="{self.name}" details={self.details}'

# Re-create table
SqlBase.metadata.create_all(engine)
with Session(engine) as session:
    session.execute(delete(Experiment))
    session.commit()

# Write in one session
with Session(engine) as session:
    session.add(
        Experiment(name='first',
                   details={
                       'scientist': 'Marie Curie',
                       'year': 1903
                   })
    )
    session.add(
        Experiment(name='second',
                   details={
                       'scientist': 'Barbara McClintock',
                       'year': 1983
                   })
    )
    session.commit()

# Read all back in another
with Session(engine) as session:
    temp = session.execute(select(Experiment))
    show('selecting all back', temp)

# Read SQL-filtered data back
with Session(engine) as session:
    temp = session.execute(select(Experiment).where(Experiment.name == 'second'))
    show('SQL filtering', temp)

# Read filtered data back
with Session(engine) as session:
    temp = session.execute(select(Experiment).where(Experiment.details['scientist'].astext == 'Marie Curie'))
    show('JSON filtering:', temp)
