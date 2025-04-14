if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


@custom
def transform_custom(*args, **kwargs):
    from sqlalchemy import create_engine,text
    eng=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
    #delete past updated_at 
    with eng.connect() as engine:
        # engine.execute(text('''delete from main_table mt where mt.id in (select id  from(select max(updated_at) over(partition by "_key") max_1,* from main_table )t
        # where updated_at<>max_1 )''')) 
        # engine.execute(text('''delete from shipping_table where id in (select id from (select no_resi,rn,id,max(rn) over(partition by "no_resi") from (select id,rn,no_resi from(
        # select *,row_number() over (partition by "no_resi" ) rn from shipping_table)t )yt)t where rn<>"max")'''))
        # engine.execute(text('''delete from platform_table where id in (select id from (select rn,id,max(rn) over(partition by "store_id") from (select id,rn,store_id from(
        # select *,row_number() over (partition by "store_id" ) rn from platform_table)t )yt)t where rn<>"max")''')) 
        # engine.execute(text('''delete from product_table where id in (select id from (select rn,id,max(rn) over(partition by "sku_bi") from (select id,rn,sku_bi from(
        # select *,row_number() over (partition by "sku_bi" ) rn from product_table)t )yt)t where rn<>"max")'''))
        # engine.execute(text('''delete from payment_table where order_id in (select order_id from (select rn,order_id,max(rn) over(partition by "order_id") from (select rn,order_id from(
        # select *,row_number() over (partition by "order_id" ) rn from payment_table)t )yt)t where rn<>"max")'''))
        # engine.execute(text('''delete from users_table where id in (select id from (select rn,id,max(rn) over(partition by "order_id") from (select id,rn,order_id from(
        # select *,row_number() over (partition by "order_id" ) rn from users_table)t )yt)t where rn<>"max")'''))
        #delete duplicate  
        engine.execute(text('''delete from main_table where id in (select id from(
        select *,row_number() over (partition by "_key" order by "id" desc) rn from main_table)t where t.rn>1)''')) 
        engine.execute(text('''delete from shipping_table  where id in (select id from(
  select *,row_number() over (partition by "no_resi" order by "id" desc) rn from shipping_table)t where t.rn>1)'''))
        engine.execute(text('''delete from payment_table  where  id in (select id from(
select *,row_number() over (partition by "order_id" order by "id" desc) rn from payment_table)t where t.rn>1)'''))
        engine.execute(text('''delete from users_table where id in (select id from(
        select *,row_number() over (partition by "order_id" order by "id" desc) rn from users_table)t where t.rn>1)'''))
        engine.execute(text('''alter table main_table  drop column id'''))
        engine.execute(text('''ALTER TABLE main_table  ADD COLUMN id SERIAL PRIMARY KEY'''))
        engine.execute(text('''alter table shipping_table  drop column id'''))
        engine.execute(text('''ALTER TABLE shipping_table  ADD COLUMN id SERIAL PRIMARY KEY'''))
        engine.execute(text('''alter table users_table  drop column id'''))  
        engine.execute(text('''ALTER TABLE users_table  ADD COLUMN id SERIAL PRIMARY KEY''')) 
        engine.execute(text('''alter table payment_table  drop column id'''))  
        engine.execute(text('''ALTER TABLE payment_table  ADD COLUMN id SERIAL PRIMARY KEY''')) 
        engine.execute(text('''alter table product_table  drop column id'''))  
        engine.execute(text('''ALTER TABLE product_table  ADD COLUMN id SERIAL PRIMARY KEY'''))

    return print('done fixing duplicate!')


