-- Docs: https://docs.mage.ai/guides/sql-blocks 
select vo.id,vrmt.invoice_ref_num,meta_general -> '$.payment_detail.order_income.items' meta_payment
from 
v0_orders vo 
join v1_reko_mp_trx vrmt on vo.invoice_ref_num=vrmt.invoice_ref_num
where 
vo.store_id=26 and
year(vrmt.create_time)=2025
and order_status>=400
and month(vrmt.create_time)=month(now())