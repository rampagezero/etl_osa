if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data,data_2, *args, **kwargs):
    import json
    import numpy as np
    import pandas as pd
    data_1={}
    product_id_l=[]
    invoice_ref_num_l=[]
    sku_l=[]
    shop_id_l=[]
    status_l=[]
    nama_barang_l=[]
    awb_l=[]
    sku_bi_l=[]
    quantity_l=[]
    order_id_l=[]
    price_l=[]
    date_l=[]
    key_l=[]
    for j in range(0,len(data)):
        for i in range(0,len(json.loads(data['product'][j]))):
            data_product_retur=json.loads(data.loc[j,'product'])
            date=data.loc[j,'create_time']
            status=data.loc[j,'status']
            product_id=data_product_retur[i]['product_id']
            invoice=data.loc[j,'invoice_ref_num']
            awb=data.loc[j,'awb']
            nama_barang=data_product_retur[i]['name']
            sku_bi=data_product_retur[i]['sku']
            shop_id=data.loc[j,'store_id']
            quantity=data_product_retur[i]['quantity']
            price=data_product_retur[i]['price']
            order_id=data.loc[j,'order_id']
            date_l.append(date)
            awb_l.append(awb)
            product_id_l.append(product_id)
            shop_id_l.append(shop_id)
            invoice_ref_num_l.append(invoice)
            status_l.append(status)
            nama_barang_l.append(nama_barang)
            sku_bi_l.append(sku_bi)
            quantity_l.append(quantity)
            order_id_l.append(order_id)
            price_l.append(price)
    data_1['date']=date_l
    # data_1['preorder_date']=None
    data_1['invoice_ref_num']=invoice_ref_num_l
    data_1['awb']=awb_l
    data_1['shop_id']=shop_id_l
    data_1['order_id']=order_id_l
    # data_1['no_resi']=None
    data_1['product_id']=product_id_l
    data_1['status']=status_l
    data_1['sku_name']=nama_barang_l
    data_1['sku_bi']=sku_bi_l
    data_1['quantity']=quantity_l
    data_1['price']=price_l
    data_join=pd.DataFrame.from_dict(data_1)
    # data_join['final_payment']=np.array(data_1['quantity'])*np.array(data_1['price'])
    # # for i in range(0,len(data)):
    # #     for j in range(0,len(json.loads(data.loc[i,'product']))):
    # #         list_json=json.dumps(json.loads(data.loc[i,'product'])[j]).update({'order_id':data.loc[i,'order_id']})
    # #         list_product_retur.append(list_json)sh
    # seller dispute,accepted,processing,judging
    data_join=data_join[['invoice_ref_num','product_id','status','awb','sku_bi','order_id','quantity']][((data_join['status']=='ACCEPTED')|(data_join['status']=='SELLER_DISPUTE')|(data_join['status']=='PROCESSING')|(data_join['status']=='JUDGING'))]
    data_join['_key']=data_join['order_id'].astype(str)+data_join['sku_bi'].astype(str)+data_join['product_id'].astype(str)
    data_2=data_2.loc[:,['_key','final_payment']]
    data_join=pd.merge(data_join,data_2,how='left',left_on='_key',right_on='_key')
    return data_join
      

