if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data,data_2,data_3,data_4,data_5,*args, **kwargs):
    import pandas as pd
    import numpy as np
    import datetime 
    # mapping data result
    data.drop_duplicates(subset='SKU URL',keep='first',inplace=True)
    data_5.drop_duplicates(subset='sku',keep='last',inplace=True)
    data_2['stock']=np.where(data_2['stock'].astype(int)>0,"1","0")
    data_3['stock']=np.where(data_3['stock']=='AVAILABLE',"1","0")
    data_4['stock']=np.where(data_4['stock'].astype(int)>0,"1","0")
    data_5['stock']=np.where(data_5['stock']=='true',"1","0")
    df1=data[(data['Availabilty']=='Active') & (data['eCustomer'].str.contains('Bukalapak'))].merge(data_2,left_on='Key',right_on='skuid',how='left').loc[:,['KAM','KAE','eCustomer','SKU BI','SKU URL','SKU NAME','date','stock','Key','Full SKU ID']]
    df2=data[(data['Availabilty']=='Active') & (data['eCustomer'].str.contains('Blibli'))].merge(data_3,left_on='Key',right_on='sku',how='left').loc[:,['KAM','KAE','eCustomer','SKU BI','SKU URL','SKU NAME','date','stock','Key','Full SKU ID']]
    df3=data[(data['Availabilty']=='Active')& (data['eCustomer'].str.contains('Tokopedia'))].merge(data_4,left_on='Key',right_on='url',how='left').loc[:,['KAM','KAE','eCustomer','SKU BI','SKU URL','SKU NAME','date','stock','Key','Full SKU ID']]
    df4=data[(data['Availabilty']=='Active')& (data['eCustomer'].str.contains('Lazada'))].merge(data_5,left_on='Key',right_on='sku',how='left').loc[:,['KAM','KAE','eCustomer','SKU BI','SKU URL','SKU NAME','date','stock','Key','Full SKU ID']]
    df_result=pd.concat([df4,df2,df3,df1])
    df_result['stock']=df_result['stock'].astype('str')
    df_null=df_result[df_result['stock']!="1"]
    df_null.columns=df_null.columns.str.lower()
    df_null['date']=datetime.datetime.today()
    df_null['date']=df_null['date'].astype('str')
    df_null=df_null.drop_duplicates(subset='sku url')
    df_null['stock']=df_null['stock'].astype('str')
    df_null['sku url']=df_null['sku url'].astype('str')
    return df_null
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
