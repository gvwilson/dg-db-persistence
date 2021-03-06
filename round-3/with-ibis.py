import ibis
import sys

def show(title, records):
    print(title)
    print(records)

conn = ibis.sqlite.connect(sys.argv[1])
person = conn.table("Person")
visited = conn.table("Visited")
survey = conn.table("Survey")

q = visited\
    .group_by("site")\
    .aggregate(visited.site.count().name("number"))
show("Number of visits per site", q.execute())

q = visited\
    .inner_join(survey, visited["id"]==survey["taken"])\
    .group_by([visited.site, survey.quant])\
    .aggregate(visited.site.count().name("number"))
show("Number of readings of each type for each site", q.execute())

q = person\
    .inner_join(survey, person.id==survey.person)\
    .inner_join(visited, visited.id==survey.taken)\
    .group_by([person.id, visited.dated, survey.quant])\
    .aggregate(survey.reading.max().name("largest"))
show("Highest reading of each type by each person on each day (simple)", q.execute())
print(ibis.sqlite.compile(q))

visited = visited.filter(visited.dated.notnull())
q = person\
    .inner_join(survey, person.id==survey.person)\
    .inner_join(visited, visited.id==survey.taken)\
    .group_by([person.id, visited.dated, survey.quant])\
    .aggregate(survey.reading.max().name("largest"))
show("Highest reading of each type by each person on each day", q.execute())
print(ibis.sqlite.compile(q))
