select  invoice_ref_num,meta_order_timeline->'$[0].status' status  From v0_orders vo  join v0_order_detail vod ON vo.id=vod.order_id where meta_order_timeline->'$[0].status'='RETURNED' and store_id=26 and year(create_time)=2025 and order_status>=400 and month(create_time)=3 
-- Docs: https://docs.mage.ai/guides/sql-blocks
