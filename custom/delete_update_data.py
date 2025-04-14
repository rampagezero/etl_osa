if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import DeclarativeBase
    class Base(DeclarativeBase):
         pass

    engine=create_engine('postgresql://salesanalyst:**salesanalyst*@192.168.33.182:5432/dashboard_gmv')
    Base.metadata.create_all(engine)
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
