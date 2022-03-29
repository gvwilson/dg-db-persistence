import ibis
import sys

conn = ibis.sqlite.connect(sys.argv[1])
person = conn.table("Person")
visited = conn.table("Visited")
survey = conn.table("Survey")

print("This works, but has NULLs in the 'dated' column")
q = person\
    .inner_join(survey, person.id==survey.person)\
    .inner_join(visited, visited.id==survey.taken)\
    .group_by([person.id, visited.dated, survey.quant])\
    .aggregate(survey.reading.max().name("largest"))
print(ibis.sqlite.compile(q))
print(q.execute())

print("This attempt to filter the NULLs doesn't work")
q = person\
    .inner_join(survey, person.id==survey.person)\
    .inner_join(visited, visited.id==survey.taken)\
    .filter(person.id.notnull())\
    .group_by([person.id, visited.dated, survey.quant])\
    .aggregate(survey.reading.max().name("largest"))
print(ibis.sqlite.compile(q))
print(q.execute())
