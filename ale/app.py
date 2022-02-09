"""Test application."""

import os
import sys

from sqlalchemy import Boolean, Column, Integer, String, create_engine, event, not_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.pool import StaticPool


UID_LEN = 16
SqlBase = declarative_base()


class Machine(SqlBase):
    __tablename__ = "machine"
    uid = Column(String(UID_LEN), primary_key=True, nullable=False)
    size = Column(Integer, nullable=False)
    active = Column(Boolean)

    def __str__(self):
        return f"<{self.uid}:{self.size}:{self.active}>"


def setup(filename):
    # https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#sqlite-foreign-keys
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        """Ensure that cascading deletes work for SQLite."""
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    # https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#threading-pooling-behavior
    engine = create_engine(
        f"sqlite:///{filename}",
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


def get_active_machines(engine):
    with Session(engine) as session:
        query = session.query(Machine).filter(Machine.active)
        return query.all()


def get_inactive_machines(engine):
    with Session(engine) as session:
        query = session.query(Machine).filter(not_(Machine.active))
        return query.all()


def main():
    filename = os.path.abspath(sys.argv[1])
    if os.path.exists(filename):
        os.remove(filename)
    engine = setup(filename)
    create_machines(engine)
    results = get_active_machines(engine)
    print(f"ACTIVE: {[str(r) for r in results]}")
    results = get_inactive_machines(engine)
    print(f"INACTIVE: {[str(r) for r in results]}")


if __name__ == "__main__":
    main()
