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
from typing import Any

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
            result = cls(**value)
            value = result
        return value


class ExperimentSql(SqlBase):
    '''
    ExperimentSql is the only ORMable class.
    '''

    __tablename__ = 'experiment'
    name = Column(String(255), primary_key=True, nullable=False)
    details = Column(JSONColumn())

    def __str__(self):
        return f'<ExperimentSql name="{self.name}" details={self.details}>'


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


class ExperimentModel(BaseModel):
    '''
    Pydantic-validated class derived from ORM model.
    '''
    name: str
    details: Any
    class Config:
        orm_mode = True

    def __str__(self):
        return f'<ExperimentModel name="{self.name}" details={self.details}>'

if __name__ == '__main__':
    # Can't violate Pydantic constraints.
    print('== Pydantic')
    try:
        DetailsNum(number='not a number')
    except ValidationError as e:
        print('trying to create invalid Pydantic field:', e)

    # Tests that work.
    tests = [
        ExperimentSql(name='with text', details=DetailsTxt(text='text content')),
        ExperimentSql(name='with number', details=DetailsNum(number=1234))
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

    # Select rows back directly.
    print('SQL direct selection')
    with Session(engine) as session:
        for (i, row) in enumerate(session.execute(select(ExperimentSql))):
            print('..', i, row[0])

    # Test direct construction and conversion.
    temp = ExperimentSql(name='temp', details=DetailsNum(number=5678))
    print('temp originally constructed', temp)
    temp_as_pydantic = ExperimentModel.from_orm(temp)
    print('temp constructed by Pydantic', temp_as_pydantic)

    # Test conversion to/from JSON.
    temp_as_json = json.dumps(temp, cls=Encoder)
    print('temp as JSON', temp_as_json)
    temp_from_json = ExperimentModel(**json.loads(temp_as_json))
    print('temp constructed from JSON', temp_from_json)

    # Select rows back directly.
    print('SQL selection and Pydantic construction')
    with Session(engine) as session:
        for (i, row) in enumerate(session.execute(select(ExperimentSql))):
            converted = ExperimentModel.from_orm(row[0])
            print('..', i, converted)
