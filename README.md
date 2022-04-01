# dg-db-persistence

Round 3: comparing pure SQL, [SQLAlchemy][sqlalchemy], and [Ibis][ibis].

## Sample Data

Our database has four tables of survey measurements from Antarctica in the 1920s and 1930s:

```
$ sqlite3 survey.db
sqlite> .schema
```
```sql
CREATE TABLE Person (id text, personal text, family text);
CREATE TABLE Site (name text, lat real, long real);
CREATE TABLE Survey (taken integer, person text, quant text, reading real);
CREATE TABLE Visited (id integer, site text, dated text);
```

The `Person` table has an ID and the scientist's first and last names:

```sql
select * from Person;
```
```
+----------+-----------+----------+
|    id    | personal  |  family  |
+----------+-----------+----------+
| dyer     | William   | Dyer     |
| pb       | Frank     | Pabodie  |
| lake     | Anderson  | Lake     |
| roe      | Valentina | Roerich  |
| danforth | Frank     | Danforth |
+----------+-----------+----------+
```

The `Site` table has a site name and location - we don't use it in our three sample queries but it's included here for completeness:

```sql
select * from Site;
```
```
+-------+--------+---------+
| name  |  lat   |  long   |
+-------+--------+---------+
| DR-1  | -49.85 | -128.57 |
| DR-3  | -47.15 | -126.72 |
| MSK-4 | -48.87 | -123.4  |
+-------+--------+---------+
```

The `Visited` table gives each visit to each site a unique ID.
Note that there is one missing date:

```sql
select * from Visited;
```
```
+-----+-------+------------+
| id  | site  |   dated    |
+-----+-------+------------+
| 619 | DR-1  | 1927-02-08 |
| 622 | DR-1  | 1927-02-10 |
| 734 | DR-3  | 1930-01-07 |
| 735 | DR-3  | 1930-01-12 |
| 751 | DR-3  | 1930-02-26 |
| 752 | DR-3  |            |
| 837 | MSK-4 | 1932-01-14 |
| 844 | DR-1  | 1932-03-22 |
+-----+-------+------------+
```

Finally, the `Survey` table records readings by specific people during specific visits.
The `quant` column is an enumeration of `rad` (radiation), `sal` (salinity), and `temp` (temperature);
some personal IDs are missing:

```sql
select * from Survey;
```
```
+-------+--------+-------+---------+
| taken | person | quant | reading |
+-------+--------+-------+---------+
| 619   | dyer   | rad   | 9.82    |
| 619   | dyer   | sal   | 0.13    |
| 622   | dyer   | rad   | 7.8     |
| 622   | dyer   | sal   | 0.09    |
| 734   | pb     | rad   | 8.41    |
| 734   | lake   | sal   | 0.05    |
| 734   | pb     | temp  | -21.5   |
| 735   | pb     | rad   | 7.22    |
| 735   |        | sal   | 0.06    |
| 735   |        | temp  | -26.0   |
| 751   | pb     | rad   | 4.35    |
| 751   | pb     | temp  | -18.5   |
| 751   | lake   | sal   | 0.1     |
| 752   | lake   | rad   | 2.19    |
| 752   | lake   | sal   | 0.09    |
| 752   | lake   | temp  | -16.0   |
| 752   | roe    | sal   | 41.6    |
| 837   | lake   | rad   | 1.46    |
| 837   | lake   | sal   | 0.21    |
| 837   | roe    | sal   | 22.5    |
| 844   | roe    | rad   | 11.25   |
+-------+--------+-------+---------+
```

## Goals

1. Count the number of times each site was visited.

2. Show the number of readings of each type for each site.

3. Show the highest reading of each type taken by each person on each date.

## The SQL Solution

### Number of visits per site

```sql
select
  site,
  count(*) as num_visits
from Visited
group by site;
```
```
+-------+------------+
| site  | num_visits |
+-------+------------+
| DR-1  | 3          |
| DR-3  | 4          |
| MSK-4 | 1          |
+-------+------------+
```

### Number of readings of each type per site

```sql
select
  Visited.site as site,
  Survey.quant as quant,
  count(*) as num_readings
from Visited join Survey
on Visited.id = Survey.taken
group by Visited.site, Survey.quant;
```
```
+-------+-------+--------------+
| site  | quant | num_readings |
+-------+-------+--------------+
| DR-1  | rad   | 3            |
| DR-1  | sal   | 2            |
| DR-3  | rad   | 4            |
| DR-3  | sal   | 5            |
| DR-3  | temp  | 4            |
| MSK-4 | rad   | 1            |
| MSK-4 | sal   | 2            |
+-------+-------+--------------+
```

### Highest reading of each type taken by each person on each day

```sql
select
  Person.personal as personal,
  Person.family as family,
  Visited.dated as dated,
  Survey.quant as quant,
  max(Survey.reading) as reading
from Person join Visited join Survey
on (Person.id = Survey.person) and (Visited.id = Survey.taken)
where Visited.dated is not null
group by Person.id, Visited.dated, Survey.quant
order by Person.family, Person.personal, Visited.dated, Survey.quant;
```
```
+-----------+---------+------------+-------+---------+
| personal  | family  |   dated    | quant | reading |
+-----------+---------+------------+-------+---------+
| William   | Dyer    | 1927-02-08 | rad   | 9.82    |
| William   | Dyer    | 1927-02-08 | sal   | 0.13    |
| William   | Dyer    | 1927-02-10 | rad   | 7.8     |
| William   | Dyer    | 1927-02-10 | sal   | 0.09    |
| Anderson  | Lake    | 1930-01-07 | sal   | 0.05    |
| Anderson  | Lake    | 1930-02-26 | sal   | 0.1     |
| Anderson  | Lake    | 1932-01-14 | rad   | 1.46    |
| Anderson  | Lake    | 1932-01-14 | sal   | 0.21    |
| Frank     | Pabodie | 1930-01-07 | rad   | 8.41    |
| Frank     | Pabodie | 1930-01-07 | temp  | -21.5   |
| Frank     | Pabodie | 1930-01-12 | rad   | 7.22    |
| Frank     | Pabodie | 1930-02-26 | rad   | 4.35    |
| Frank     | Pabodie | 1930-02-26 | temp  | -18.5   |
| Valentina | Roerich | 1932-01-14 | sal   | 22.5    |
| Valentina | Roerich | 1932-03-22 | rad   | 11.25   |
+-----------+---------+------------+-------+---------+
```

## The SQLAlchemy Solution

### Setup

```python
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

class Survey(Base):
    __tablename__ = "Survey"
    taken = Column(Integer)
    person = Column(String)
    quant = Column(String)
    reading = Column(Float)
    __table_args__ = (
        PrimaryKeyConstraint("taken", "quant", "reading"),
    )

class Visited(Base):
    __tablename__ = "Visited"
    id = Column(Integer, primary_key=True)
    site = Column(String)
    dated = Column(Date)
```

### Number of visits per site

```python
session.query(Visited.site, func.count(Visited.site))\
       .group_by(Visited.site)\
       .all()
```
```
| DR-1 | 3 |
| DR-3 | 4 |
| MSK-4 | 1 |
```

### Number of readings of each type per site

```python
session.query(Visited.site, Survey.quant, func.count())\
       .join(Survey)\
       .filter(Visited.id == Survey.taken)\
       .group_by(Visited.site, Survey.quant)\
       .all()
```
```
| DR-1 | rad | 3 |
| DR-1 | sal | 2 |
| DR-3 | rad | 4 |
| DR-3 | sal | 5 |
| DR-3 | temp | 4 |
| MSK-4 | rad | 1 |
| MSK-4 | sal | 2 |
```

### Highest reading of each type taken by each person

```sql
q = session.query(Person.personal, Person.family, Visited.dated, Survey.quant, func.max(Survey.reading))\
           .join(Person)\
           .filter(Person.id == Survey.person)\
           .join(Visited)\
           .filter(Visited.id == Survey.taken)\
           .filter(Visited.dated.isnot(None))\
           .group_by(Person.id, Visited.dated, Survey.quant)\
           .order_by(Person.family, Person.personal, Visited.dated, Survey.quant)
```
```
+-----------+---------+------------+-------+---------+
| personal  | family  |   dated    | quant | reading |
+-----------+---------+------------+-------+---------+
| William   | Dyer    | 1927-02-08 | rad   | 9.82    |
| William   | Dyer    | 1927-02-08 | sal   | 0.13    |
| William   | Dyer    | 1927-02-10 | rad   | 7.8     |
| William   | Dyer    | 1927-02-10 | sal   | 0.09    |
| Anderson  | Lake    | 1930-01-07 | sal   | 0.05    |
| Anderson  | Lake    | 1930-02-26 | sal   | 0.1     |
| Anderson  | Lake    | 1932-01-14 | rad   | 1.46    |
| Anderson  | Lake    | 1932-01-14 | sal   | 0.21    |
| Frank     | Pabodie | 1930-01-07 | rad   | 8.41    |
| Frank     | Pabodie | 1930-01-07 | temp  | -21.5   |
| Frank     | Pabodie | 1930-01-12 | rad   | 7.22    |
| Frank     | Pabodie | 1930-02-26 | rad   | 4.35    |
| Frank     | Pabodie | 1930-02-26 | temp  | -18.5   |
| Valentina | Roerich | 1932-01-14 | sal   | 22.5    |
| Valentina | Roerich | 1932-03-22 | rad   | 11.25   |
+-----------+---------+------------+-------+---------+
```

## The Ibis Solution

### Setup

```python
import ibis
import sys

def show(title, records):
    print(title)
    print(records)

conn = ibis.sqlite.connect(sys.argv[1])
person = conn.table("Person")
visited = conn.table("Visited")
survey = conn.table("Survey")
```

### Number of visits per site

```python
q = visited\
    .group_by("site")\
    .aggregate(visited.site.count().name("number"))
show("Number of visits per site", q.execute())
```
```
    site  number
0   DR-1       3
1   DR-3       4
2  MSK-4       1
```

### Number of readings of each type per site

```python
q = visited\
    .inner_join(survey, visited["id"]==survey["taken"])\
    .group_by([visited.site, survey.quant])\
    .aggregate(visited.site.count().name("number"))
show("Number of readings of each type for each site", q.execute())
```
```
    site quant  number
0   DR-1   rad       3
1   DR-1   sal       2
2   DR-3   rad       4
3   DR-3   sal       5
4   DR-3  temp       4
5  MSK-4   rad       1
6  MSK-4   sal       2
```

### Highest reading of each type taken by each person


```python
q = person\
    .inner_join(survey, person.id==survey.person)\
    .inner_join(visited, visited.id==survey.taken)\
    .group_by([person.id, visited.dated, survey.quant])\
    .aggregate(survey.reading.max().name("largest"))
show("Highest reading of each type by each person on each day (simple)", q.execute())
```
```
Highest reading of each type by each person on each day (simple)
      id       dated quant  largest
0   dyer  1927-02-08   rad     9.82
1   dyer  1927-02-08   sal     0.13
2   dyer  1927-02-10   rad     7.80
3   dyer  1927-02-10   sal     0.09
4   lake        None   rad     2.19
5   lake        None   sal     0.09
6   lake        None  temp   -16.00
7   lake  1930-01-07   sal     0.05
8   lake  1930-02-26   sal     0.10
9   lake  1932-01-14   rad     1.46
10  lake  1932-01-14   sal     0.21
11    pb  1930-01-07   rad     8.41
12    pb  1930-01-07  temp   -21.50
13    pb  1930-01-12   rad     7.22
14    pb  1930-02-26   rad     4.35
15    pb  1930-02-26  temp   -18.50
16   roe        None   sal    41.60
17   roe  1932-01-14   sal    22.50
18   roe  1932-03-22   rad    11.25
```

The generated SQL is clean:

```sql
SELECT
  t0.id, t2.dated, t1.quant, max(t1.reading) AS largest 
FROM
  base."Person" AS t0
JOIN
  base."Survey" AS t1
ON t0.id = t1.person
JOIN
  base."Visited" AS t2
ON
  t2.id = t1.taken
GROUP BY t0.id, t2.dated, t1.quant;
```

However, the query above doesn't get rid of the `NULL` values in the `dated` column.
I haven't been able to make that work with a simple filter expression inline in the query;
I've filed a bug report and am getting help with it.
Filtering before the query works just fine:

```python
visited = visited.filter(visited.dated.notnull())
q = person\
    .inner_join(survey, person.id==survey.person)\
    .inner_join(visited, visited.id==survey.taken)\
    .group_by([person.id, visited.dated, survey.quant])\
    .aggregate(survey.reading.max().name("largest"))
show("Highest reading of each type by each person on each day", q.execute())
```
```
Highest reading of each type by each person on each day
      id       dated quant  largest
0   dyer  1927-02-08   rad     9.82
1   dyer  1927-02-08   sal     0.13
2   dyer  1927-02-10   rad     7.80
3   dyer  1927-02-10   sal     0.09
4   lake  1930-01-07   sal     0.05
5   lake  1930-02-26   sal     0.10
6   lake  1932-01-14   rad     1.46
7   lake  1932-01-14   sal     0.21
8     pb  1930-01-07   rad     8.41
9     pb  1930-01-07  temp   -21.50
10    pb  1930-01-12   rad     7.22
11    pb  1930-02-26   rad     4.35
12    pb  1930-02-26  temp   -18.50
13   roe  1932-01-14   sal    22.50
14   roe  1932-03-22   rad    11.25
```

In this case the SQL contains a spurious sub-query that I believe PostgreSQL would optimize away:

```sql
SELECT
  t0.id, t2.dated, t1.quant, max(t1.reading) AS largest 
FROM
  base."Person" AS t0
JOIN
  base."Survey" AS t1
ON t0.id = t1.person
JOIN
  (
    SELECT
      t3.id AS id, t3.site AS site, t3.dated AS dated
    FROM
      base."Visited" AS t3
    WHERE t3.dated IS NOT NULL
  ) AS t2
ON
  t2.id = t1.taken
GROUP BY t0.id, t2.dated, t1.quant;
```
