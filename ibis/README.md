# Comparing Embedded SQL, SQLAlchemy, and Ibis

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

The `Site` table has a site name and location:

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

3. Show the highest reading of each type taken by each person.

## The SQL Solution

### Nubmer of visited per site

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

## Number of readings of each type per site

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

3. Show the highest reading of each type taken by each person.

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
