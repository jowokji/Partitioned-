
create table sales_data(
    sale_id integer,
    product_id integer not null,
    region_id integer not null,
    salesperson_id integer not null,
    sale_amount numeric not null,
    sale_date date not null ,
    primary key (sale_id, sale_date)
) partition by range (sale_date);



create table sales_data_2023_01 partition of sales_data
    for values from ('2023-01-01') to ('2023-02-01');

create table sales_data_2023_02 partition of sales_data
    for values from ('2023-02-01') to ('2023-03-01');

create table sales_data_2023_03 partition of sales_data
    for values from ('2023-03-01') to ('2023-04-01');

create table sales_data_2023_04 partition of sales_data
    for values from ('2023-04-01') to ('2023-05-01');

create table sales_data_2023_05 partition of sales_data
    for values from ('2023-05-01') to ('2023-06-01');

create table sales_data_2023_06 partition of sales_data
    for values from ('2023-06-01') to ('2023-07-01');

create table sales_data_2023_07 partition of sales_data
    for values from ('2023-07-01') to ('2023-08-01');

create table sales_data_2023_08 partition of sales_data
    for values from ('2023-08-01') to ('2023-09-01');

create table sales_data_2023_09 partition of sales_data
    for values from ('2023-09-01') to ('2023-10-01');

create table sales_data_2023_10 partition of sales_data
    for values from ('2023-10-01') to ('2023-11-01');

create table sales_data_2023_11 partition of sales_data
    for values from ('2023-11-01') to ('2023-12-01');

create table sales_data_2023_12 partition of sales_data
    for values from ('2023-12-01') to ('2024-01-01');


create or replace function generate_insert_data()
returns void
language plpgsql
as $$
declare
    SALE_DATE date;
    NEW_SALE_ID integer;
begin
    for COUNTER in 1..1000 loop
    SALE_DATE := '2023-01-01'::date + (floor(random() * 365) * interval '1 day');
    NEW_SALE_ID := COUNTER;

        insert into sales_data(sale_id, sale_date, salesperson_id, region_id, product_id, sale_amount)
        values (
            NEW_SALE_ID,
            SALE_DATE,
            1 + floor(random() * 6), 
            1 + floor(random() * 10), 
            1 + floor(random() * 8),  
            40 + floor(random() * 1000)  
        );
    end loop;
end;
$$;

select generate_insert_data();


select 
    extract(month from sale_date) as MONTH_SALE,
    count(*) as MONTH_TOTAL
from sales_data
group by MONTH_SALE
order by MONTH_SALE;

select 
  extract(month from sale_date) as MONTH_SALE, 
  sum(sale_amount) as TOTAL_AMOUNT
from sales_data
group by MONTH_SALE
order by MONTH_SALE;



with PERSON_SALE as (
    select 
        salesperson_id,
        region_id,
        sum(sale_amount) as TOTAL_AMOUNT,
        rank() over (partition by region_id order by sum(sale_amount) desc) as PERSON_RANK
    from 
        sales_data
    group by 
        region_id, salesperson_id
)
select 
    salesperson_id,
    region_id,
    TOTAL_AMOUNT
from 
     PERSON_SALE
where 
    PERSON_RANK <= 3;


create or replace procedure manage_partitions()
language plpgsql
as $$
declare
    CURRENT_DATE date := current_date;
    LAST_YEAR_DATE date := CURRENT_DATE - interval '1 year';
    PARTITION_DATE_TO_REMOVE date;
    NEXT_MONTH_START date := date_trunc('month', CURRENT_DATE);
    NEXT_MONTH_END date := date_trunc('month', NEXT_MONTH_START) + interval '1 month';
    MONTH_START date;
    MONTH_END date;
    PARTITION_DATE_TO_ADD date;
    PARTITION_NAME varchar;
    NEXT_MONTH_NAME varchar;
begin

    for COUNTER in 0..11 loop
        PARTITION_DATE_TO_REMOVE := LAST_YEAR_DATE - (interval '1 month' * COUNTER);
        PARTITION_NAME := 'sales_data_' || to_char(PARTITION_DATE_TO_REMOVE, 'yyyy_mm');
        if to_regclass(PARTITION_NAME) is not null then
            execute format('drop table %i', PARTITION_NAME);
            raise notice 'dropped partition: %', PARTITION_NAME;
        else
            raise notice 'partition % does not exist, skipping drop.', PARTITION_NAME;
        end if;
        
        PARTITION_DATE_TO_ADD = NEXT_MONTH_START - (interval '1 month' * COUNTER);
        NEXT_MONTH_NAME := 'sales_data_' || to_char(PARTITION_DATE_TO_ADD, 'yyyy_mm');
        MONTH_START = NEXT_MONTH_START - (interval '1 month' * COUNTER);
        MONTH_END = NEXT_MONTH_END - (interval '1 month' * COUNTER);
        if to_regclass(NEXT_MONTH_NAME) is null then
            execute format('create table %i partition of sales_data for values from (%l) to (%l)', 
                           NEXT_MONTH_NAME, MONTH_START, MONTH_END);
            raise notice 'created partition: %', NEXT_MONTH_NAME;
        else
            raise notice 'partition % already exists, skipping creation.', NEXT_MONTH_NAME;
    end if;
    end loop;

end;
$$;


call manage_partitions();
