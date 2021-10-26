#!/usr/bin/env python

'''More SQLAlchemy'''
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
with engine.connect() as conn:
    conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 1, "y": 2}, {"x": 3, "y": 4}]
    )
    conn.commit()

stmt = text('select x, y from some_table where y > :y order by x, y').bindparams(y=2)
with Session(engine) as session:
    result = session.execute(stmt)
    for row in result:
        print(row)
