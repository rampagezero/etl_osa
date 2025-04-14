select 
  * 
from 
  (
    select 
      date(v1.create_time) as date, 
      v2.created_at as date_metadata ,
      vw.meta,
      v2.store_id, 
      v2.meta_wms,
      v1.general_info ,
      v1.shipping -> '$.awb' as no_resi, 
      v1.shipping -> '$.shipping_agency' as logistic_service, 
      v1.payment ,
      v2.order_status as status, 
      v1.amount -> '$.total_amt' as total_amt, 
      vw.name,
      vw.meta -> '$.city_name' as warehouse_city,
      v1.amount -> '$.product_total_amount' as product_total_amount, 
      vp.name as store_name,
      v2.platform_id,
      v1.meta_product,
      v1.updated_at,
      v1.order_id,
      v2.invoice_ref_num
    from 
      v0_order_detail v1 
      join v0_orders v2 on v1.order_id = v2.id 
      join v0_stores vs on vs.id = v2.store_id 
      join v0_platforms vp on vp.id = vs.platform_id 
      join v0_warehouses vw on v1.ecom_warehouses_id = vw.id
    where 
     v1.updated_at>= (date(now()) - interval 2 day)
    -- extract(month from v1.create_time)=12 and extract(year from v1.create_time)=2024
      and (v2.store_id=33 )
      ) data