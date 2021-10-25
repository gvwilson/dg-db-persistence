#!/usr/bin/env python

'''SQLAlchemy example'''

from sqlalchemy import create_engine, text

engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
with engine.connect() as conn:
    conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 1, "y": 2}, {"x": 3, "y": 4}]
    )
    conn.commit()
with engine.connect() as conn:
    res = conn.execute(text('select * from some_table'))
    for row in res:
        print(row)
with engine.connect() as conn:
    res = conn.execute(text("select * from some_table where y > :y"),
                       {'y': 2})
    for row in res:
        print(row)
