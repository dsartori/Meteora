-- count all rows
select count(*) from observations

-- count high temperatures over 20C by station and year
select 
	[name],
	year(observation_date) as year,
	count(*) as days_over_20
from observations 
where max_temp > 20.0
group by name, year(observation_date)


-- count low temperatures under 0 by station and year
select 
	[name],
	year(observation_date) as year,
	count(*) as days_over_20
from observations 
where min_temp < 0.0
group by name, year(observation_date)

-- get all observations within a specified polygon 
-- (see https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry for details of text representation)
-- declare bounding box
declare @bbox geometry
set @bbox = geometry::STPolyFromText('POLYGON ((-125.0 40.0, -120.0 40.0, -120.0 49.0, -125.0 49.0, -125.0 40.0))',4326)
set @bbox = @bbox.MakeValid()

select @bbox.STAsText()

-- query observations table for rows inside the bounding poly
select * from observations
where @bbox.STContains(geometry::Point(longitude,latitude,4326)) = 1

-- annual high temperature for stations inside the bounding poly
select 
	[name],
	year(observation_date),
	max(max_temp)
from observations o
where @bbox.STContains(geometry::Point(longitude,latitude,4326)) = 1 
group by [name],year(observation_date)
order by [name],year(observation_date)
