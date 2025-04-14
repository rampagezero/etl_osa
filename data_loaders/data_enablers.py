from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


    

@data_loader
def load_data_from_mysql(data_last,*args, **kwargs):
    """
    Template for loading data from a MySQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#mysql
    """
    
    query = f"""select 
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
      v1.payment -> '$.gateway_name' as payment_method, 
      v2.order_status as status, 
      v1.amount -> '$.total_amt' as total_amt, 
      vw.name,
      vw.meta -> '$.city_name' as warehouse_city,
      v1.amount -> '$.product_total_amount' as product_total_amount, 
      vp.name as store_name,
      v2.platform_id,
      v1.meta_product,
      v1.updated_at,
      v1.order_id
    from 
      v0_order_detail v1 
      join v0_orders v2 on v1.order_id = v2.id 
      join v0_stores vs on vs.id = v2.store_id 
      join v0_platforms vp on vp.id = vs.platform_id 
      join v0_warehouses vw on v1.ecom_warehouses_id = vw.id
    where 
      v1.updated_at>={{block output ("updated_at",parse=lambda data,_vars:data['updated_at'])}}
      and v2.store_id=33
      and vp.name like '%Shopee%' 
      and v2.order_status >= 400
      ) data1
      """
    # Specify your SQL query here
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dashboard_gmv' 

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return loader.load(query)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'