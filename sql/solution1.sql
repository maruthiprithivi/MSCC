-- Set DB
use moneysmart;

-- Create table query
create table if not exists samplePageViews
(
ID int,
User_ID int,
Page_ID int,
Visit_Date date,
Visit_Time time
);

-- Data load qery
load data local infile '/data/SamplePageviews.csv' -- Set local path to files
ignore into table samplePageViews
fields terminated by ','
lines terminated by '\n'
ignore 1 lines;

-- Solution
with temp1 as (
    with temp0 as (
        select
            user_id,
            page_id,
            visit_date,
            visit_time,
            lag(visit_time, 1) over (partition by user_id, page_id order by visit_time, visit_date) as prev_time
    from samplePageViews)
        select
            user_id,
            page_id,
            visit_date,
            visit_time,
            prev_time,
            time_to_sec(timediff(visit_time ,prev_time))/60 as tdiff
        from temp0)
select
    page_id,
    visit_date,
    count(page_id) as total_user_sessions
from temp1
where tdiff > 10 -- to count user sessions that don't fall within the 10 minutes rule
or tdiff is null
group by page_id, visit_date;