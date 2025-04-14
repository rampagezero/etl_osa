if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    import pandas as pd
    import json
    id_l=[]
    invoice_ref_num_l=[]
    data_payment_item_id_l=[]
    data_payment_item_name_l=[]
    data_payment_item_sku_l=[]
    data_payment_item_shopee_discount_l=[]
    data_subtotal={}
    for i in range(0,len(data)):
        id=data.iloc[i,0]
        invoice_ref_num=data.iloc[i,1]
        for j in range(0,len(json.loads(data.iloc[i,2]))):
            data_payment_item_id=json.loads(data.iloc[i,2])[j]['item_id']
            data_payment_item_sku=json.loads(data.iloc[i,2])[j]['item_sku']
            data_payment_item_name=json.loads(data.iloc[i,2])[j]['item_name']
            data_payment_item_shopee_discount=json.loads(data.iloc[i,2])[j]['shopee_discount']
            id=data.iloc[i,0]
            invoice_ref_num=data.iloc[i,1]
            data_payment_item_id_l.append(data_payment_item_id)
            data_payment_item_name_l.append(data_payment_item_name)
            data_payment_item_sku_l.append(data_payment_item_sku)
            data_payment_item_shopee_discount_l.append(data_payment_item_shopee_discount)
            id_l.append(id)
            invoice_ref_num_l.append(invoice_ref_num)
    data_subtotal['id']=id_l
    data_subtotal['invoice_ref_num']=invoice_ref_num_l
    data_subtotal['item_id']=data_payment_item_id_l
    data_subtotal['name']=data_payment_item_name_l
    data_subtotal['sku']=data_payment_item_sku_l
    data_subtotal['platform_discount']=data_payment_item_shopee_discount_l
    data_subtotal=pd.DataFrame.from_dict(data_subtotal)
    data_subtotal['_key']=data_subtotal['id'].astype('str')+data_subtotal['sku'].astype('str')+data_subtotal['item_id'].astype('str')
    return data_subtotal


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
