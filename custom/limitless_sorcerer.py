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
    # delete updated records from 'records'
    

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
