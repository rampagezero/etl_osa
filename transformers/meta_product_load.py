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
        for i in range(0,len(json.loads(data.iloc[j,0]))):
            try:
                product_id=json.loads(json.loads(data.iloc[j,0])[i])['product_model_id']
            except:
                product_id=json.loads(json.loads(data.iloc[j,0])[i])['product_id']
            if product_id==0:
                product_id=json.loads(json.loads(data.iloc[j,0])[i])['product_id']
            sku=json.loads(json.loads(data.iloc[j,0])[i])['sku']
            qty=json.loads(json.loads(data.iloc[j,0])[i])['quantity']
            nama_barang=json.loads(json.loads(data.iloc[j,0])[i])['product_name']
            sku_bi=json.loads(json.loads(data.iloc[j,0])[i])['sku']
            price=json.loads(json.loads(data.iloc[j,0])[i])['subtotal_price']
            product_weight=json.loads(json.loads(data.iloc[j,0])[i])['product_weight']
            product_id_l.append(product_id)
            sku_l.append(sku)
            nama_barang_l.append(nama_barang)
            qty_l.append(qty)
            sku_bi_l.append(sku_bi)
            subtotal_l.append(price)
            order_id_l.append(data.iloc[j,1])
            product_weight_l.append(product_weight)
            payment_date_l.append(dt.datetime.strftime(data.iloc[j,2],'%Y-%m-%d'))
            if data.iloc[j,4]!=None:
                preorder_l.append(dt.datetime.strptime(data.iloc[j,4].replace('"','').split('T')[0],'%Y-%m-%d'))
            else:
                preorder_l.append("None")
    data_1['product_id']=product_id_l
    data_1['sku']=sku_l
    data_1['sku_name']=nama_barang_l
    data_1['sku_bi']=sku_bi_l
    data_1['quantity']=qty_l
    data_1['payment_before_discount_platform']=subtotal_l
    data_1['order_id']=order_id_l
    data_1['payment_date']=payment_date_l
    data_1['preorder_date']=preorder_l  
    data_1['product_weight']=product_weight_l
    data_1=data_1=pd.DataFrame(data_1)
    data_1=data_1[~data_1.sku_bi.str.contains('/B')]
    return data_1
    

# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'