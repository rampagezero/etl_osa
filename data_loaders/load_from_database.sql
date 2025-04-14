select * from main_table where extract(year from _date)=2024 
and (extract(month from _date)=11 or extract(month from _date)=12)