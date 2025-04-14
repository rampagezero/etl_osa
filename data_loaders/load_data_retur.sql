-- Docs: https://docs.mage.ai/guides/sql-blocks 
select vo.invoice_ref_num,shipping->'$.awb' awb,v1.create_time create_time,product,status,vo.store_id,v1.order_id,meta_general -> '$.payment_detail.order_income.items'
from v1_order_complaint voc 
join v0_order_detail v1 on v1.order_id=voc.order_id
join v0_orders vo on v1.order_id=vo.id
join v1_reko_mp_trx vrmt on vo.invoice_ref_num=vrmt.invoice_ref_num
 where extract(year from v1.create_time)=2024 and (extract(month from v1.create_time)=11 or extract(month from v1.create_time)=12)  and vo.store_id=26