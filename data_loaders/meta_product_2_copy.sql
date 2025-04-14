select 
  meta_product, 
  v1.order_id ,
  v1.payment_date,
  v2.store_id,
  v2.meta_preorder -> "$.preorder_process_start" preorder
from 
  v0_order_detail v1 
  join v0_orders v2 on v1.order_id = v2.id 
where 
  v1.updated_at>=(date(now()) - interval 2 day)
  -- extract(month from v1.create_time)=12 and extract(year from v1.create_time)=2024
  and (v2.store_id=33 )
  -- and v2.order_status >=400 
