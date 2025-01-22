-- kenyan  maandamano analytics
select * from maandamano_data;

-- locations where maandamano took place
select
	distinct location
from
	maandamano_data;

-- causes for maandamano
select
	distinct cause
from
	maandamano_data;

-- total participants of the maandamano
-- we will come back to this
select 
    sum(participants) as total_participants
from maandamano_data 


-- outcomes of the maandamano
select 
    distinct outcome
from maandamano_data


-- organizers of the maandamano
select
	distinct organizer
from
	maandamano_data

-- extract date, location, cause, participants, and additional date information
with stg_maandamano as (
    select
        date,
        location,
        cause,
        participants
    from
        maandamano_data
)
select
    date,
    location,
    cause,
    participants,
    extract(year from date) as year,
    extract(month from date) as month,
    extract(day from date) as day,
    to_char(date, 'day') as day_of_week
from
    stg_maandamano
group by location,cause,date,participants;
order by date desc

