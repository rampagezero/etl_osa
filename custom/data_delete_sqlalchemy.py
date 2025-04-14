if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    import pandas as pd
    from sqlalchemy import create_engine

    import pandas.io.sql as psql
    from sqlalchemy import create_engine
    engine=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
    #delete past updated_at
    engine.execute('''delete from main_table mt where mt.id in (select id  from(select max(updated_at) over(partition by "_key") max_1,* from main_table )t
     where updated_at<>max_1 or _status<400)''')
    engine.execute('''delete from shipping_table where id in (select id from (select no_resi,rn,id,max(rn) over(partition by "no_resi") from (select id,rn,no_resi from(
select *,row_number() over (partition by "no_resi" ) rn from shipping_table)t )yt)t where rn<>"max")''')
    engine.execute('''delete from platform_table where id in (select id from (select rn,id,max(rn) over(partition by "store_id") from (select id,rn,store_id from(
select *,row_number() over (partition by "store_id" ) rn from platform_table)t )yt)t where rn<>"max")''')
    engine.execute('''delete from product_table where id in (select id from (select rn,id,max(rn) over(partition by "sku_bi") from (select id,rn,sku_bi from(
select *,row_number() over (partition by "sku_bi" ) rn from product_table)t )yt)t where rn<>"max")''')
    engine.execute('''delete from payment_table where id in (select id from (select rn,id,max(rn) over(partition by "order_id") from (select id,rn,order_id from(
select *,row_number() over (partition by "order_id" ) rn from payment_table)t )yt)t where rn<>"max")''')
    engine.execute('''delete from users_table where id in (select id from (select rn,id,max(rn) over(partition by "order_id") from (select id,rn,order_id from(
select *,row_number() over (partition by "order_id" ) rn from users_table)t )yt)t where rn<>"max")''')
    #delete duplicate 
    engine.execute('''delete from main_table where id in (select id from(
select *,row_number() over (partition by "_key" order by "id" desc) rn from main_table)t where t.rn>1)''')
    # reset id
    engine.execute('''alter table shipping_table  drop column id''')
    engine.execute('''ALTER TABLE shipping_table  ADD COLUMN id SERIAL PRIMARY KEY''')
    engine.execute('''alter table users_table  drop column id''')
    engine.execute('''ALTER TABLE users_table  ADD COLUMN id SERIAL PRIMARY KEY''') 
    engine.execute('''alter table payment_table  drop column id''')
    engine.execute('''ALTER TABLE payment_table  ADD COLUMN id SERIAL PRIMARY KEY''')
    engine.execute('''alter table product_table  drop column id''')
    engine.execute('''ALTER TABLE product_table  ADD COLUMN id SERIAL PRIMARY KEY''')
    

    return {}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'