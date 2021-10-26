#!/usr/bin/env python

'''Saving JSON with SQLAlchemy'''

from sqlalchemy import create_engine, insert, select, text
from sqlalchemy import MetaData, Table, Column, Integer, String
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import declarative_base, Session

import sys

# Echo SQL as we go?
echo = (len(sys.argv) > 1) and (sys.argv[1] == '--echo')

# Create the base for SQLAlchemy classes
SqlBase = declarative_base()

# Store details of an experiment
class Experiment(SqlBase):
    __tablename__ = 'experiment'
    name = Column(String(30), primary_key=True, nullable=False)
    details = Column(JSON)

    def __repr__(self):
        return f'<Experiment {self.name} {self.details}>'


# Create the database engine (in-memory SQLite for now)
engine = create_engine("sqlite+pysqlite:///:memory:", future=True, echo=echo)

# Create the table(s).
SqlBase.metadata.create_all(engine)

# Add some rows.
with Session(engine) as session:
    et = Experiment.__table__
    session.execute(insert(et).values(name='with potion',
                                      details={'potion': 'syrup'}))
    session.execute(insert(et).values(name='with spell',
                                      details={'spell': 1234}))
    for (i, r) in enumerate(session.execute(text('select * from Experiment'))):
        print('select * :', i, r)
    for (i, r) in enumerate(session.execute(select(Experiment))):
        print('object   :', i, r)
