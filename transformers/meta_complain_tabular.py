if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    import pandas as pd
    import json
    import datetime as dt
    # data_frame=pd.json_normalize(data,meta=[0,['product_id']])
    data_1={}
    product_id_l=[]
    sku_l=[]
    nama_barang_l=[]
    sku_bi_l=[]
    order_id_l=[]
    payment_date_l=[]
    qty_l=[]
    subtotal_l=[]
    preorder_l=[]
    product_weight_l=[]
    for j in range(0,len(data)):
        for i in range(0,len(json.loads(data.loc[j,'product']))):
            product_id=json.loads(data.loc[j,'product'])[i]['product_id']
            sku=json.loads(data.loc[j,'product'])[i]['sku']
            qty=json.loads(data.loc[j,'product'])[i]['quantity']
            nama_barang=json.loads(data.loc[j,'product'])[i]['name']
            price=json.loads(data.loc[j,'product'])[i]['price']
            order_id=data.loc[j,'order_id']
            order_id_l.append(order_id)
            product_id_l.append(product_id)
            sku_l.append(sku)
            nama_barang_l.append(nama_barang)
            qty_l.append(qty)
            subtotal_l.append(price)
    data_1['product_id']=product_id_l
    data_1['sku']=sku_l
    data_1['sku_name']=nama_barang_l
    data_1['quantity']=qty_l
    data_1['price']=subtotal_l
    data_1['order_id']=order_id_l
    data_1=data_1=pd.DataFrame(data_1)
    data_1['key_complain']=data_1['order_id'].astype('str')+data_1['sku'].astype('str')+data_1['product_id'].astype('str')

    return data_1


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
