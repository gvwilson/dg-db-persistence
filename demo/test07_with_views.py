#!/usr/bin/env python

'''
Step 7: let's get some JSON data up to the 'table' level with views.
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

# (Re-)create the view of names (SQL level)
view_table = Table('all_names', MetaData())
drop_view = DropView(view_table, if_exists=True)
create_view = CreateView(view_table, text('select name from experiments')).compile()
with Session(engine) as session:
    session.execute(str(drop_view))
    session.execute(str(create_view))
    session.commit()

# Write data
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

# Read name view data
with Session(engine) as session:
    temp = session.execute(text('select * from all_names'))
    show('name view:', temp)

# (Re-)create the view of years (JSON)
with Session(engine) as session:
    session.execute(text("drop view if exists years"))
    session.execute(text("create view years as select details->>'year' as y from experiments"))
    session.commit()

# Read view data
with Session(engine) as session:
    temp = session.execute(text("select * from years"))
    show('years view:', temp)
