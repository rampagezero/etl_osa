with cte as (select id  from(
select max(updated_at) over(partition by "_key") max_1,* from main_table )t where updated_at<max_1)
select * from cte
