if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    import json
    import pandas
    import requests
    import pandas as pd
    import datetime
    import pytz
    import time
    import hmac
    import hashlib
    import hashlib
    import hmac
    import json
    headers={'token':'L3fhEAKpPH4T6k3IxNQDE7Es73rADCpJXEKM'}
    shop_code=86
    response=requests.get(f'https://enabler.orbiz.id/api/v1/stores/noverse/{shop_code}',headers=headers)
    data_token=json.loads(response.text)
    tmp_partner_key='4872506d726a7657535867436c416350576e6a6b416e4e49657177726b436f79'
    data_token=data_token['decrypt']['data']['access_token']
    input=f"app_key65vjmeq89dfmltimestamp{int(time.time())}"
    app_key='65vjmeq89dfml'
    app_secret="fc024d576de90495649208b991c3f3c5f1222764"
    path="/api/products/search"
    base=app_secret+path+input+app_secret
    signature = hmac.new(
                app_secret.encode(),  # Partner key must be encoded to bytes
                base.encode(), # Base string must be encoded to bytes
                hashlib.sha256        # Use SHA-256 hashing algorithm
            ).hexdigest()
    dump_pid=[]
    url="https://open-api.tiktokglobalshop.com/"
    url = url + path + "?app_key=" +app_key +"&timestamp=" + str(int(time.time())) + "&sign=" + signature+"&access_token="+	data_token
    for i in range(1,4):
        data={"page_number": i,"page_size":100
            }
        data_pid=json.loads(requests.post(url,json=data).text)['data']['products']
        dump_pid.extend(data_pid)
    data_pid_l=pd.DataFrame(dump_pid)['skus'].apply(lambda x:x[0]['id']).to_list()
    headers={'token':'L3fhEAKpPH4T6k3IxNQDE7Es73rADCpJXEKM'}
    shop_code=86
    response=requests.get(f'https://enabler.orbiz.id/api/v1/stores/noverse/{shop_code}',headers=headers)
    data_token=json.loads(response.text)
    tmp_partner_key='4872506d726a7657535867436c416350576e6a6b416e4e49657177726b436f79'
    data_token=data_token['decrypt']['data']['access_token']
    input=f"app_key65vjmeq89dfmltimestamp{int(time.time())}"
    app_key='65vjmeq89dfml'
    app_secret="fc024d576de90495649208b991c3f3c5f1222764"
    path="/api/products/stock/list"
    base=app_secret+path+input+app_secret
    signature = hmac.new(
                app_secret.encode(),  # Partner key must be encoded to bytes
                base.encode(), # Base string must be encoded to bytes
                hashlib.sha256        # Use SHA-256 hashing algorithm
            ).hexdigest()   
    url="https://open-api.tiktokglobalshop.com/"
    url = url + path + "?app_key=" +app_key +"&timestamp=" + str(int(time.time())) + "&sign=" + signature+"&access_token="+data_token
    data={"sku_ids": 
        data_pid_l
    }
    data=json.loads(requests.post(url,json=data).text)
    data_stock=pd.json_normalize(data['data']['product_stocks'],['skus'],['product_id'])
    data_warehouse=pd.json_normalize(data['data']['product_stocks'],['skus','warehouse_stock_infos'],'product_id')
    data_final=pd.merge(data_stock,data_warehouse).drop('warehouse_stock_infos',axis=1)
    local_timezone = pytz.timezone("Asia/Jakarta")
    data_final['timestamp']=datetime.datetime.fromtimestamp(time.time()).astimezone(local_timezone).strftime('%Y-%m-%d %H:%M:%S')
    data_final['total_available_stock_distribution.campaign_stock']=data_final['total_available_stock_distribution.campaign_stock'].replace({None:0})
    data_id=pd.DataFrame(dump_pid)
    data_id['skus']=data_id['skus'].apply(lambda x:x[0]['id'])
    data_id=data_id[['skus','name']]
    data_final=pd.merge(data_id,data_final,right_on='sku_id',left_on='skus')


    return data_final


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
