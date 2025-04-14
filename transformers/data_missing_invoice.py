if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data,data_2,data_3,data_4,data_5,data_6, *args, **kwargs):
    import pandas as pd
    from time import strftime,localtime
    import json
    import numpy as np
    import datetime
    data['name']=data['general_info'].apply(lambda x:json.loads(x)['buyer_fullname'])
    data['province']=data['general_info'].apply(lambda x:json.loads(json.loads(x)['meta_address'])['address_province'])
    data['city']=data['general_info'].apply(lambda x:json.loads(json.loads(x)['meta_address'])['address_city'])
    data['full_address']=data['general_info'].apply(lambda x:json.loads(json.loads(x)['meta_address'])['address_street'])
    data['order_id']=data['order_id'].astype('int64')
    # data_3['order_id']=data_3['order_id'].astype('int64')
    data_join=data_3.join(data.set_index('order_id'),on='order_id',lsuffix='_sql_detail',rsuffix='_sql',how='left')
    data_join['key']=data_join['order_id'].astype('str')+data_join['sku'].astype('str')+data_join['product_id'].astype('str')
    #data_join['key']=data_join['key'].astype('str')  
    #data_join=data_join.join(data_3.set_index('order_id'))
    # PREORDER HANDLER
    date_list=data_join['date'].to_list()
    date_preorder_list=data_join['preorder_date'].to_list()
    for i,j in enumerate(date_preorder_list):
        if date_preorder_list[i]=="None":
            date_list[i]=date_list[i]
        else:
            date_list[i]=date_preorder_list[i]
    data_join['date']=date_list
    data_join['date']=pd.to_datetime(data_join['date'])
    data_6.rename(columns={'discount':'platform_discount'},inplace=True)
    data_6=data_6[['_key','platform_discount']]
    data_2=data_2.loc[:,['_key','platform_discount']]
    data_2=pd.concat([data_2,data_6]).drop_duplicates('_key')
    data_join=pd.merge(left=data_join,right=data_2,left_on='key',right_on='_key',how='left')
    data_join['final_payment']=0
    data_join['date']=pd.to_datetime(data_join['date'])
    data_join[~(data_join['preorder_date'].isnull())]['date']=data_join[~(data_join['preorder_date'].isnull())]['preorder_date']
    # data_join['platform']=data_join['meta'].apply(lambda x:json.loads(x))
    # data_join['shop_id']=data_join['meta'].apply(lambda x:json.loads(str(x))['shop_id'])
    # data_join.rename(columns={0:'platform_discount'},inplace=True)
    # data_join['product_id']=data_join['product_id'].astype('str')
    # data_join['payment_ref_number']=data_join['payment'].apply(lambda x:json.loads(x)['payment_ref_num'])
    # data_join['payment_date']=data_join['payment'].apply(lambda x:json.loads(x)['payment_date'])
    # data_join['gateway_name']=data_join['payment'].apply(lambda x:json.loads(x)['gateway_name'])
    data_join['product_id']=data_join['product_id'].map(str)
    data_join['payment_before_discount_platform']=data_join['payment_before_discount_platform'].astype('int')
    pivotted=pd.pivot_table(data_join,index='date',values='final_payment',aggfunc='sum').reset_index()
    data_join_not_found=pd.merge(data_join[data_join['platform_discount'].isnull()],data_4['key_complain'],left_on='key',right_on='key_complain',how='left')
    # data_join[['date','invoice_ref_num','platform_discount','_key','order_id','key']][data_join['platform_discount'].isnull()]
    # data_gbg=pd.merge(data_join[data_join['platform_discount'].isnull()],data_4[['escrow_date','key_complain']],left_on='key',right_on='key_complain',how='left')
    # data_gbg[data_gbg['key_complain'].isnull()][['date','escrow_date','invoice_ref_num']]
    data_not_found=data_join_not_found[data_join_not_found['key_complain'].isnull()]
    data_not_found=pd.merge(data_not_found,data_5, on='invoice_ref_num',how='left')
    data_not_found['create_time']=datetime.datetime.today()
    return data_not_found[data_not_found['platform_discount'].isnull()]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
