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
  v2.store_id=26 and
  year(create_time)=2025
      and order_status>=400
      and month(v1.create_time)=month(now())