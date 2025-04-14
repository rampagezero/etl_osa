if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data,data_2,*args, **kwargs):
    import pandas as pd
    # data=data.drop('platform_discount_y',axis=1).rename(columns={'platform_discount_x':'platform_discount'})
    data=pd.concat([data,data_2],axis=0)
    data['final_payment']=data['payment_before_discount_platform']-data['platform_discount']
    data['final_payment']=data['final_payment'].astype('int')
    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
