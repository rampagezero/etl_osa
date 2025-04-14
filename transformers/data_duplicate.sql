-- Docs: https://docs.mage.ai/guides/sql-blocks
delete from main_table m using 
(select "_key",max(updated_at) as max_time from main_table  group by "_key") k
where m.updated_at<>k.max_time and m."_key"=k."_key"