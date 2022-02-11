#!/usr/bin/env python

"""Test application."""

import configparser
import os
import sys

from sqlalchemy import Boolean, Column, Integer, String, Text, \
    create_engine, event, inspect, not_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.pool import StaticPool

from alembic.config import Config
from alembic import command


UID_LEN = 16
SqlBase = declarative_base()


def get_schema(engine):
    """Find the tables and columns in the database."""
    result = {}
    inspector = inspect(engine)
    for table_name in inspector.get_table_names():
        result[table_name] = set()
        for column in inspector.get_columns(table_name):
            result[table_name].add(column["name"])
    return result


class Machine(SqlBase):
    __tablename__ = "machine"
    uid = Column(String(UID_LEN), primary_key=True, nullable=False)
    size = Column(Integer, nullable=False)
    active = Column(Boolean)
    # name = Column(Text, default="-unnamed-", nullable=False)

    def __str__(self):
        name = self.name if hasattr(self, "name") else "-no name field-"
        return f"<{self.uid}:{self.size}:{self.active}:{name}>"


def setup(config_file):
    # https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#sqlite-foreign-keys
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        """Ensure that cascading deletes work for SQLite."""
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    config = configparser.ConfigParser()
    config.read(config_file)
    url = config["alembic"]["sqlalchemy.url"]

    # https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#threading-pooling-behavior
    engine = create_engine(
        url,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SqlBase.metadata.create_all(engine)

    return engine


def create_machines(engine):
    with Session(engine) as session:
        session.add(Machine(uid="m001", size="large", active=True))
        session.add(Machine(uid="m002", size="medium", active=False))
        session.add(Machine(uid="m003", size="small"))
        session.commit()


def get_machines(engine, active):
    with Session(engine) as session:
        query = session.query(Machine)
        if active:
            query = query.filter(Machine.active)
        else:
            query = query.filter(not_(Machine.active))
        return query.all()


def main():
    if len(sys.argv) != 3:
        print("usage: app.py <config.ini> [true|false]", file=sys.stderr)
        sys.exit(1)
    config_file = os.path.abspath(sys.argv[1])

    alembic_cfg = Config(config_file)
    command.upgrade(alembic_cfg, "head")

    engine = setup(config_file)
    schema = get_schema(engine)
    print("SCHEMA", schema)

    create_machines(engine)
    results = get_machines(engine, True)
    print(f"ACTIVE: {', '.join([str(r) for r in results])}")
    results = get_machines(engine, False)
    print(f"INACTIVE: {', '.join([str(r) for r in results])}")


if __name__ == "__main__":
    main()
