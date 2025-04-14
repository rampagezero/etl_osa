if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(data,*args, **kwargs):
    import json
    import pandas
    import requests
    import pandas as pd
    import datetime
    import pytz
    import time
    import hmac
    import hashlib
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
    url = url + path + "?app_key=" +app_key +"&timestamp=" + str(int(time.time())) + "&sign=" + signature+"&access_token="+	"ROW_P-5CagAAAACAzZBtVuMBkoj0WT4sgnOwM0fDrmrJTTl-Nc8HDujbJFeepsm6deaUUpj7MDVt76sYPVJ88S5cd3j-o_F8T0fq3ekCaQZsTToI2cAC3nEzOFbX35-WqEGEZ5datFU2e9U0p4T19s6sAKzcjZ-OcTR54HgxD75wEIwbxCGi2x0Hhw"
    data={"sku_ids": [
        "1730410218406643459","1730493515584407299"
    ]}
    data=json.loads(requests.post(url,json=data).text)
    data_stock=pd.json_normalize(data['data']['product_stocks'],['skus'],['product_id'])
    data_warehouse=pd.json_normalize(data['data']['product_stocks'],['skus','warehouse_stock_infos'],'product_id')
    data_final=pd.merge(data_stock,data_warehouse).drop('warehouse_stock_infos',axis=1)
    local_timezone = pytz.timezone("Asia/Jakarta")
    data_final['timestamp']=datetime.datetime.fromtimestamp(time.time()).astimezone(local_timezone).strftime('%Y-%m-%d %H:%M:%S')
  

    return data_final


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
