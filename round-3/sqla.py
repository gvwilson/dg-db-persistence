"""Solve the problem with SQLAlchemy."""

import os
import sys
from sqlalchemy import create_engine, func, Column, Date, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy.schema import PrimaryKeyConstraint

def show(title, records):
    print(title)
    for r in records:
        print(f"| {' | '.join([str(x) for x in r])} |")

Base = declarative_base()

class Person(Base):
    __tablename__ = "Person"
    id = Column(String, primary_key=True)
    personal = Column(String)
    family = Column(String)

    def __repr__(self):
        return f"<{self.id} {self.personal} {self.family}>"

class Visited(Base):
    __tablename__ = "Visited"
    id = Column(Integer, primary_key=True)
    site = Column(String)
    dated = Column(Date)

    def __repr__(self):
        return f"<{self.id} {self.site} {self.dated}>"

class Survey(Base):
    __tablename__ = "Survey"
    taken = Column(Integer, ForeignKey("Visited.id"))
    person = Column(String, ForeignKey("Person.id"))
    quant = Column(String)
    reading = Column(Float)
    visited = relationship("Visited")
    __table_args__ = (
        PrimaryKeyConstraint("taken", "quant", "reading"),
    )

    def __repr__(self):
        return f"<{self.taken} {self.person} {self.quant} {self.reading}>"

# finish setup
url = f"sqlite:///{sys.argv[1]}"
engine = create_engine(url)
Base.metadata.create_all(engine)

# visits per site
with Session(engine) as session:
    q = session.query(Visited.site, func.count(Visited.site))\
               .group_by(Visited.site)
    show("Number of visits per site", q.all())

# number of readings of each type per site
with Session(engine) as session:
    q = session.query(Visited.site, Survey.quant, func.count())\
               .join(Survey)\
               .filter(Visited.id == Survey.taken)\
               .group_by(Visited.site, Survey.quant)
    show("Number of readings of each type for each site", q.all())

# highest reading of each type by each person on each day
with Session(engine) as session:
    q = session.query(Person.personal, Person.family, Visited.dated, Survey.quant, func.max(Survey.reading))\
               .join(Person)\
               .filter(Person.id == Survey.person)\
               .join(Visited)\
               .filter(Visited.id == Survey.taken)\
               .filter(Visited.dated.isnot(None))\
               .group_by(Person.id, Visited.dated, Survey.quant)\
               .order_by(Person.family, Person.personal, Visited.dated, Survey.quant)
    show("Highest reading of each type taken by each person on each day", q.all())
