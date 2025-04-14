from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_mysql(*args, **kwargs):
    """
    Template for loading data from a MySQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#mysql
    """
    query ="""select 
  meta_product, 
  v1.order_id ,
  v1.payment_date,
  v2.store_id,
  v2.meta_preorder -> "$.preorder_process_start" preorder
from 
  v0_order_detail v1 
  join v0_orders v2 on v1.order_id = v2.id 
where 
  v1.updated_at>='{{block_output("updated_at",parse=lambda data,_vars:data.iloc[0,0])}}'
  and v2.store_id=33
  and v2.order_status >=400 
  """

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
