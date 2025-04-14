select 
      v1.create_time, 
      v1.order_id
from 
      v0_order_detail v1 
      join v0_orders v2 on v1.order_id = v2.id 
      join v0_orders v3 on v3.id = v1.order_id 
      join v0_stores v4 on v2.store_id=v4.id
where 
      v2.store_id = 26
      and (extract(
        year 
        from 
          v1.create_time
      )= 2024 or extract(
        year 
        from 
          v1.create_time
      )= 2025)