-- Set DB
use moneysmart;

-- Create table query
create table if not exists sampleOrders
(
RowID int,
OrderID varchar(256),
OrderDated date,
CustomerID varchar(256),
ProductID varchar(256),
ProductName text,
Sales float,
Quantity int
);

-- Data load qery
load data local infile '/data/SampleOrders.csv' -- Set local path to files
into table sampleOrders
fields terminated by ','
lines terminated by '\n'
ignore 1 lines
(RowID, OrderID, @OrderDated, CustomerID, ProductID, ProductName, Sales, Quantity)
set OrderDated = str_to_date(@OrderDated, '%e/%c/%y'); -- TO cast the date format into MariaDB Date field type

-- Solution
with temp5 as (
    with temp4 as (
        with temp3 as (
            with temp2 as (
                with temp1 as (
                    with temp0 as (
                        select
                            a.ProductID as ProductA,
                            b.ProductID as ProductB
                        from sampleOrders as a join sampleOrders as b
                        on a.OrderID = b.OrderID and a.ProductId < b.ProductId and a.ProductId != b.ProductId)
                    select
                        temp0.ProductA,
                        temp0.ProductB,
                        count(*) as Occurrences,
                    (select count(distinct(CustomerID)) from sampleOrders) as uniqueCustomers
                    from temp0 group by temp0.ProductA, temp0.ProductB)
                select
                    temp1.ProductA,
                    temp1.ProductB,
                    temp1.Occurrences,
                    temp1.uniqueCustomers,
                    (temp1.Occurrences/temp1.uniqueCustomers) as Support -- Support tabulated here
                from temp1)
            select
                temp2.ProductA,
                temp2.ProductB,
                temp2.Occurrences,
                temp2.uniqueCustomers,
                temp2.Support,
                join0.ProductAcount,
                (join0.ProductAcount/temp2.uniqueCustomers) as PofA -- Probability of Product A
            from temp2 left join
            (select ProductId, count(distinct(CustomerID)) as ProductAcount from sampleOrders group by ProductID) as join0
            on temp2.ProductA = join0.ProductID)
        select
            temp3.ProductA,
            temp3.ProductB,
            temp3.Occurrences,
            temp3.Support,
            temp3.PofA,
            temp3.ProductAcount,
            join1.ProductBcount,
            (join1.ProductBcount/temp3.uniqueCustomers) as PofB -- Probability of Product B
        from temp3 left join
        (select ProductId, count(distinct(CustomerID)) as ProductBcount from sampleOrders group by ProductID) as join1 on temp3.ProductB = join1.ProductID)
    select
        temp4.ProductA,
        temp4.ProductB,
        temp4.Occurrences,
        temp4.Support,
        temp4.ProductAcount,
        temp4.ProductBcount,
        (temp4.Support/(temp4.PofA)) as Confidence, -- Confidence tabulated here
        ((temp4.Support/(temp4.PofA))/(temp4.PofB)) as Lift -- Lift tabulated here
    from temp4)
select
    temp5.ProductA,
    temp5.ProductB,
    temp5.Occurrences,
    temp5.Support,
    temp5.Confidence,
    temp5.Lift
from temp5
where temp5.Support >= 0.2 and temp5.Confidence >= 0.6 and temp5.Lift > 1 -- Filtering Conditions
order by temp5.ProductAcount desc, temp5.Occurrences desc limit 10; -- Rules to display top 10
