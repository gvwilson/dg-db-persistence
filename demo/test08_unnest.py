#!/usr/bin/env python

'''
Step 8: flatten nested data inside JSON blobs using views.
'''

import json
from sqlalchemy import create_engine, Column, MetaData, String, Table, delete, insert, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy_views import CreateView, DropView
import sys

from util import show


USER = 'gregwilson'
PASSWORD = 'postgresql'
HOST = '127.0.0.1'
PORT = 5432
DATABASE = 'learnjson'

ECHO = (len(sys.argv) > 1) and (sys.argv[1] == '--echo')


# Create the PostgreSQL database engine.
engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}', future=True, echo=ECHO)

# Create the base for SQLAlchemy classes
SqlBase = declarative_base()
class Experiment(SqlBase):
    __tablename__ = 'experiments'
    name = Column(String, primary_key=True)
    details = Column(JSONB)

    def __repr__(self):
        return f'Experiment name="{self.name}" details={self.details}'

# Re-create table and clear out any pre-existing data
SqlBase.metadata.create_all(engine)
with Session(engine) as session:
    session.execute(delete(Experiment))
    session.commit()

# Write data containing arrays of objects.
with Session(engine) as session:
    session.add(
        Experiment(name='first',
                   details=[
                       {'color': 'red'},
                       {'color': 'orange'}
                   ])
    )
    session.add(
        Experiment(name='second',
                   details=[
                       {'color': 'green'},
                       {'color': 'blue'}
                   ])
    )
    session.commit()

# (Re-)create the view of colors
with Session(engine) as session:
    session.execute(text("drop view if exists colors"))
    session.execute(text("create view colors as select name, jsonb_array_elements(details)->>'color' as d from experiments"))
    session.commit()

# Read view data
with Session(engine) as session:
    temp = session.execute(text("select * from colors"))
    show('flattened colors view:', temp)
