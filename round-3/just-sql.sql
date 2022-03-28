.headers on
.mode table

.print "Number of visits per site"
select
  site,
  count(*) as num_visits
from Visited
group by site;

.print "Number of readings of each type for each site"
select
  Visited.site as site,
  Survey.quant as quant,
  count(*) as num_readings
from Visited join Survey
on Visited.id = Survey.taken
group by Visited.site, Survey.quant;

.print "Highest reading of each type taken by each person on each day"
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
