-- Docs: https://docs.mage.ai/guides/sql-blocks 
select vo.id,vrmt.invoice_ref_num,meta_general -> '$.payment_detail.order_income.items' meta_payment
from 
v0_orders vo 
join v1_reko_mp_trx vrmt on vo.invoice_ref_num=vrmt.invoice_ref_num
where 
vo.store_id=33 and
vrmt.updated_at>= (date(now()) - interval 2 day)