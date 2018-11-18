-- Set DB
use moneysmart;

-- Create table query
create table if not exists experimentGroups
(
visitDate date,
groupBucket text,
experiment varchar(10)
);

-- Solution A
select
    groupBucket,
    experiment,
    count(*) as numberOfUsers
from experimentGroups
group by groupBucket, experiment;

-- Solution B
with temp0 as (
    select
        visitDate,
        experiment,
        count(*) as numberOfUsers
    from experimentGroups
    group by visitDate, experiment)
select
    temp0.visitDate
from temp0
order by temp0.numberOfUsers desc
limit 1;