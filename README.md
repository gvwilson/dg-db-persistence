# hybrid-persistence

Small test/demo programs while figuring out hybrid persistence strategies.

- `test01_mem_only.py`: classes in memory.

- `test02_json_persist_fail.py`: showing that we can't persist directly as JSON.

- `test03_json_custom.py`: using a custom JSON serializer.

- `test04_orm.py`: persisting to a database with SQLAlchemy and a JSON column.

- `test05_hybrid.py`: adding type checking with Pydantic.

- `test06_using_postgresql.py`: persist JSON to PostgreSQL and select fields from JSON.

- `test07_with_views.py`: using views to reflect data in JSON fields.
