import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(data,*args, **kwargs):
    import requests
    import pprint
    import json
    import hmac
    import time
    import hashlib
    import requests
    import pprint
    import pandas as pd
    import json
    from urllib.parse import quote,urlencode
    headers={
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Host": "enabler.orbiz.id",
            "Priority": "u=0, i",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
            "Cookie":"_ga_1RR23X09YM=GS1.1.1740214595.9.1.1740214619.0.0.0; _ga=GA1.1.1585873770.1726721190; XSRF-TOKEN=eyJpdiI6IkhhajcyQkhGMVNGazAyWnVCTUdWSWc9PSIsInZhbHVlIjoiQ210ZXZQTHVDSWNPMjdoenBpa0pWODlmMndzMkpBT3BwbmVHZzdIRVhGT2FSSzJyRnpabERINm5VZ3ZVTXRIcmtMMnlwd1I3Y3Q4bmp2VXJkVk4yR0VmaExUR0lYbUZLUnZ3MHVodWlDYk1FR2llY0ZvYVVMK3hMZ3NNT2Eva1EiLCJtYWMiOiJkZmFhOWYwNWFkMTFjMjM1MDcwZWNjMzg2ODJkNGIxM2EwMjZkMTQzYjU1ZjJiZjhlZWVmYmY4Y2Q2YjAxYjBlIiwidGFnIjoiIn0%3D; orbiz_e_commerce_enabler_session=eyJpdiI6IkNDYVE3YnRKQkxFQm5nWXFhVWhubHc9PSIsInZhbHVlIjoiL1p4ZmFQN0c4VEk1Ti9Qek5TcEVSU0pIMFQ2cnVWTktjbzhqeFZBMVBQbzVyZXA2OXA1cVg1QVVnRDlWUlkrakJYeEVsUU1ZTzZlei9CeUFxcUVaYUVSaHRzbHlwK3Vod3F6QXBtbVZMY1pmbUozSkFZcXN3d2F5NCtCVzc3N3YiLCJtYWMiOiJmZTdlNjUzYTc2NTBiYTQ2YWJiMWU3NTNlNGM1NGRjYzQ3YzEyOGM2MGQzZmQ5OTNiZTU4OTFiNWEwNDI1NmRhIiwidGFnIjoiIn0%3D"
    }

    response=requests.get('https://enabler.orbiz.id/noverse/26',headers=headers)
    data_token=json.loads(response.text)
    partner_id='2004863'
    shop_id='30082703'
    tmp_partner_key='4872506d726a7657535867436c416350576e6a6b416e4e49657177726b436f79'
    access_token=f"{data_token['decrypt']['access_token']}"
    host = "https://partner.shopeemobile.com"
    path = "/api/v2/payment/get_escrow_detail_batch"
    timest = int(time.time())
    host = "https://partner.shopeemobile.com"
    def signature(partner_id, path, timestamp, access_token, shop_id, partner_key):
        try:
            # Construct the base string
            base_string = f"{int(partner_id)}{path}{timestamp}{access_token}{int(shop_id)}"
            
            # Generate the HMAC-SHA256 signature
            signature = hmac.new(
                partner_key.encode(),  # Partner key must be encoded to bytes
                base_string.encode(),  # Base string must be encoded to bytes
                hashlib.sha256         # Use SHA-256 hashing algorithm
            ).hexdigest()              # Get the hexadecimal representation of the signature
            
            return signature
        except Exception as e:
            print(f"Error generating signature: {e}")
            return None
    a=0
    order_sn_l=[]
    item_id_l=[]
    item_sku_l=[]
    discount_l=[]
    item_name_l=[]
    quantity_l=[]
    final_payment_l=[]
    invoice_missing=data.loc[:5,'invoice_ref_num'].to_list()
    for batch in range(0,len(invoice_missing),20):
        timest = int(time.time())
        sign=signature(partner_id,path,timest,access_token,shop_id,tmp_partner_key)
        url = f"{host}{path}?access_token={access_token}&partner_id={partner_id}&shop_id={shop_id}&sign={sign}&timestamp={timest}"
        body={'order_sn_list':invoice_missing[a:batch+20]}
        headers={}
        response = requests.request("POST",url,headers=headers, json=body, allow_redirects=False)
        data_1=json.loads(response.text)['response']
        for i in range(0,len(data_1)):
            items=data_1[i]['escrow_detail']['order_income']['items']
            for j in range(0,len(items)):
                item_id=items[j]['model_id']
                if item_id==0:
                    item_id=items[j]['item_id']
                item_sku=items[j]['model_sku']
                if item_sku=="":
                    item_sku=items[j]['item_sku']
                order_sn=data_1[i]['escrow_detail']['order_sn']
                discount=items[j]['shopee_discount']
                item_name=items[j]['item_name']
                quantity=items[j]['quantity_purchased']
                final_payment=items[j]['selling_price']
                item_name_l.append(item_name)
                item_id_l.append(item_id)
                item_sku_l.append(item_sku)
                discount_l.append(discount)
                order_sn_l.append(order_sn)
                quantity_l.append(quantity)
                final_payment_l.append(final_payment)
        prog=batch/len(invoice_missing)*100
        print(f'%.2f completed' %prog,end='\r')
        a=a+20
    data_table={'invoice_ref_num':order_sn_l,'sku':item_sku_l,'product_id':item_id_l,'sku_name':item_name_l,'quantity':quantity_l,'platform_discount':discount_l,'payment_before_discount_platform':final_payment_l}
    data_missing_invoice=pd.DataFrame(data=data_table)
    data=data.drop(['product_id','sku','sku_name','sku_bi','quantity','payment_before_discount_platform','platform_discount'],axis=1)
    data=pd.merge(data_missing_invoice,data,on='invoice_ref_num',how='left')
    return data
 

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
