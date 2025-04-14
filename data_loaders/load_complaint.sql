select create_time, invoice_ref_num,order_id,product,status from v1_order_complaint voc
where year(voc.create_time)=2025 and store_id=26 and month(voc.create_time)=month(now())
-- select * from v1_order_complaint voc where invoice_ref_num ='2412051JUQYKUQ'