with cte as (select id  from(
select max(updated_at) over(partition by "_key") max_1,* from main_table )t where updated_at<max_1)
delete from main_table mt using cte ct where  ct.id=mt.id 