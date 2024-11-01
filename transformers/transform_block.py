if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data,data_2,data_3,data_4,data_5,data_6, *args, **kwargs):
    import pandas as pd
    import numpy as np
    import datetime 
    # mapping data result
    data['stock']=np.where(data['stock']=='true',"1","0")
    data_2['stock']=np.where(data_2['stock'].astype(int)>0,"1","0")
    data_3['stock']=np.where(data_3['stock']=='AVAILABLE',"1","0")
    data_4['stock']=np.where(data_4['stock'].astype(int)>0,"1","0")
    
    df2=data_5[(data_5['Availabilty']=='Active')&(data_5["eCustomer"].str.contains("Blibli"))].merge(data_3,left_on='Key',right_on='sku',how='left').loc[:,['KAM','KAE','eCustomer','SKU BI','SKU URL','SKU NAME','date','stock','Key']]
    df3=data_5[(data_5['Availabilty']=='Active')&(data_5["eCustomer"].str.contains("Tokopedia"))].merge(data_2,left_on='Key',right_on='url',how='left').loc[:,['KAM','KAE','eCustomer','SKU BI','SKU URL','SKU NAME','date','stock','Key']]
    df5=data_5[(data_5['Availabilty']=='Active')&(data_5["eCustomer"]=="Bukalapak")].merge(data_4,left_on='Key',right_on='skuid',how='left').loc[:,['KAM','KAE','eCustomer','SKU BI','SKU URL','SKU NAME','date','stock','Key']]
    df4=data_5[(data_5['Availabilty']=='Active')&(data_5["eCustomer"]=="Lazada")].merge(data,left_on='Key',right_on='sku',how='left').loc[:,['KAM','KAE','eCustomer','SKU BI','SKU URL','SKU NAME','date','stock','Key']]
    df_result=pd.concat([df2,df3,df4,df5])
    df_non_null=df_result[df_result['stock']=="1"].iloc[:,0:8]
    df_null=data_6.iloc[:,0:8]
    df_non_null['date']=df_non_null['date'].astype('str')
    df_non_null.columns=df_null.columns
    df_result=pd.concat([df_non_null,df_null])
    df_result['date']=datetime.datetime.today().strftime('%Y-%m-%d')
    return df_result

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
