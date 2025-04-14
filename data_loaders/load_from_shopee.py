import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
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
    import pandas as pd 
    import requests
    import json
    import datetime
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
    class ApiData:
        def __init__(self,platform,code,order_status):
            import numpy as np
            import pandas as pd
            import datetime
            from sqlalchemy import create_engine,text
            import pandas as pd
            from tqdm import tqdm
            self.platform=platform
            self.shop_code=code
            self.call=0
            self.order_status_get=order_status
            self.task_manager()
        def shopee_call(self):
            headers={'token':'L3fhEAKpPH4T6k3IxNQDE7Es73rADCpJXEKM'}
            shop_code=self.shop_code
            response=requests.get(f'https://enabler.orbiz.id/api/v1/stores/noverse/{shop_code}',headers=headers)
            data_token=json.loads(response.text)
            self.partner_id='2004863'
            self.shop_id=data_token['decrypt']['shop_id']
            self.tmp_partner_key='4872506d726a7657535867436c416350576e6a6b416e4e49657177726b436f79'
            self.access_token=data_token['decrypt']['access_token']
            self.host = "https://partner.shopeemobile.com"
            timest = int(time.time())
            print('token initiated!')
            self.call='done'
            return self.task_manager()
        def task_manager(self):
            if self.platform=='shopee' and self.call!='done':
                return self.shopee_call()
        def data_invoice_list(self,start_time,end_time):
            from zoneinfo import ZoneInfo
            start_time=datetime.datetime.strptime(start_time,'%Y/%m/%d').replace(tzinfo=ZoneInfo('Asia/Jakarta'))
            end_time=datetime.datetime.strptime(end_time,'%Y/%m/%d').replace(tzinfo=ZoneInfo('Asia/Jakarta'))
            if end_time-start_time>datetime.timedelta(days=15):
                return print('caution Error !  (Maximum days 15) ')
            else:
                pass
            start_time=int(start_time.timestamp())
            end_time=int(end_time.timestamp())
            path = "/api/v2/order/get_order_list"
            timest = int(time.time())
            sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
            url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
            list_order_sn=[]
            params={'time_range_field':'create_time',
                    'time_from':start_time,
                    'time_to':end_time,
                    'page_size':100,
                    'cursor':"",
                    'order_status':self.order_status_get
            }
            print('getting data....',end='\r')
            response=requests.get(url,params=params).json()
            # add serial number on list
            data_sn=response['response']['order_list']
            list_order_sn.extend(data_sn)
            cursor = response['response']['next_cursor']
            more = response['response']['more']
            
            while more is True:
                    timest = int(time.time())
                    sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                    url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
                    params={'time_range_field':'create_time',
                            'time_from':int(start_time),
                            'time_to':end_time,
                            'page_size':100,
                            'cursor':cursor,
                            'order_status':self.order_status_get
                    }
                    response=requests.get(url,params=params).json()
                    # add serial number on list
                    data_sn=response['response']['order_list']
                    list_order_sn.extend(data_sn)
                    cursor = response['response']['next_cursor']
                    more=response['response']['more']
            else:
                    print('get order sn done!')
            self.list_order_sn=[i['order_sn'] for i in list_order_sn]
            return self.list_order_sn
        def get_data_all(self):
            data_order=self.get_order_detail()
            data_payment=self.get_data_payment()
            data_final=pd.merge(data_order,data_payment)
            data_final['payment_before_discount_platform']=data_final['final_payment']+data_final['shopee_discount']
            data_final.rename(columns={'shopee_discount':'platform_discount','payment_method':'gateway_name','pay_time':'payment_date'},inplace=True)
            return data_final
        def get_order_detail(self):
            from sqlalchemy import create_engine,text
            import pandas as pd
            import numpy as np
            from time import strftime, localtime
            import datetime
            from datetime import timezone
            from dateutil import tz
            from tqdm.notebook import tqdm
            from zoneinfo import  ZoneInfo
            engine=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
            query=text('select max(order_id) from main_table ')
            #  NEED TO BE UPDATED
            shop_name_dict={'179066421':'Tefal Indonesia Official Shop','30082703':'Philips Lighting Official Shop'}
            with engine.connect() as conn:
                result=conn.execute(query)
                max_id=result.fetchall()
            max_id=max_id[0][0]
            list_order=self.list_order_sn
            a=0
            dump=[]
            i=50
            j=len(list_order) #452
            
            while j>=0:
                path = "/api/v2/order/get_order_detail"
                timest = int(time.time())
                sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
                push_sn=str(list_order[a:i]).replace('[','').replace("'",
                                                        "").replace(']','').replace(' ','')
                params={
                    'order_sn_list':push_sn,
                    'response_optional_fields':'item_list,total_amount,payment_method,invoice_data,pay_time,shipping_carrier,package_list,buyer_user_id,buyer_username,recipient_address'
                }
                response=requests.get(url,params=params).json()
                data_order=response['response']['order_list']
                dump.extend(data_order)
                a+=50
                i+=50
                j=j-50
                if j<50:
                    i=i+j-50
                print(f'check_prog:{round(len(dump)/len(list_order)*100,2)} %',end='\r')
            check_data_len=len(list_order)-len(dump)
            if check_data_len==0:
                print('check done, data okay!',end='\r')
            else:
                print(f'Total Order Detail:{len(dump)}')
                print(f'Total Order Serial Number:{len(list_order)}')
                print('some data missing')
            data=pd.DataFrame(dump)[['order_sn','order_status']]
            data['order_id']=[max_id+i for i in data.index]
            data_detail=pd.json_normalize(dump,'item_list',['payment_method','order_sn','pay_time','create_time','update_time','shipping_carrier','package_list',['recipient_address','name'],['recipient_address','state'],['recipient_address','city'],['recipient_address','full_address']])
            data_final=pd.merge(data_detail,data,on='order_sn',how='left')
            ## data preprocessing
            data_final['product_id']=np.where(data_final['model_id']==0,data_final['item_id'],data_final['model_id'])
            data_final['sku_name']=np.where(data_final['model_name']!='',data_final['item_name']+'-'+data_final['model_name'],data_final['item_name'])
            data_final['sku']=np.where(data_final['model_sku']=='',data_final['item_sku'],data_final['model_sku'])
            data_final.rename(columns={'model_quantity_purchased':'quantity'},inplace=True)
            data_final.rename(columns={'model_discounted_price':'final_payment'},inplace=True)
            data_final['key']=data_final['order_sn'].astype('str')+'_'+data_final['product_id'].astype('str')
        #  repair data!!!
            tz=ZoneInfo('Asia/Jakarta')
            data_final['pay_time']=data_final['pay_time'].apply(lambda x:datetime.datetime.fromtimestamp(x,tz=tz) if x!=None else 0)
            data_final['create_time']=data_final['create_time'].apply(lambda x:datetime.datetime.fromtimestamp(x,tz=tz))
            data_final['create_time']=pd.to_datetime(data_final['create_time']).dt.tz_localize(None)
            data_final['date']=data_final['create_time'].apply(lambda x: datetime.datetime.strftime(x,'%d-%m-%Y'))
            data_final['preorder_date']=None
            if self.partner_id=='2004863':
                data_final['platform_id']=6
            order_status_map={'COMPLETED':690,'TO_CONFIRM_RECEIVE':601,'SHIPPED':500,'CANCELLED':15,'TO_RETURN':550}
            data_final['order_status']=data_final['order_status'].map(order_status_map)
            # adding no_resi                task
            data_resi=self.get_tracking_number()
            data_final['no_resi']=data_final['order_sn'].map(data_resi)
            data_tracking_info=self.get_tracking_info()
            data_final['meta_wms']=data_final['order_sn'].map(data_tracking_info)
            path = "/api/v2/shop/get_warehouse_detail"
            timest = int(time.time())
            sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
            url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
            response=requests.get(url,params=params).json()
            data_warehouse_detail=pd.json_normalize(response['response'])
            data_final['product_location_id']=data_final['product_location_id'].map(lambda x:x[0])
            data_final=pd.merge(data_final,data_warehouse_detail,left_on='product_location_id',right_on='location_id',how='left')
            data_final.rename(columns={'shipping_carrier':'logistic_service','city':'warehouse_city','sku':'sku_bi','weight':'product_weight','recipient_address.state':'province','recipient_address.name':'name','recipient_address.full_address':'full_address','recipient_address.city':'city','order_status':'status','order_sn':'invoice_ref_num','update_time':'updated_at'},inplace=True)
            data_final['store_id']=self.shop_id
            data_final['store_name']=data_final['store_id'].map(shop_name_dict)
            return data_final
        def get_data_payment(self):
            import numpy as np
            from tqdm.notebook import tqdm
            path = "/api/v2/payment/get_escrow_detail_batch"
            a=0
            dump=[]
            i=50
            list_order=self.list_order_sn
            j=len(list_order)
            timest = int(time.time())
            data_payment_l=[]
            while j>=0:
                timest = int(time.time())
                sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
                json={
                    'order_sn_list':list_order[a:i]
                }
                response=requests.post(url,json=json).json()
                data_order=response['response']
                data_payment_l.extend(data_order)
                a+=50
                i+=50
                j=j-50
                if j<50:
                    i=i+j-50
                print(f'check_prog:{round(len(dump)/len(list_order)*100,2)}',end='\r')
            data_payment=pd.json_normalize(data_payment_l,['escrow_detail','order_income','items'],[['escrow_detail','order_sn']])
            data_payment['product_id']=np.where(data_payment['model_id']==0,data_payment['item_id'],data_payment['model_id'])
            data_payment['key']=data_payment['escrow_detail.order_sn'].astype('str')+'_'+data_payment['product_id'].astype('str')
            data_payment['payment_ref_number']=None
            
            return data_payment
        def get_tracking_number(self):
            from tqdm.notebook import tqdm
            print('get_tracking_number....')
            partner_id='2004863'
            shop_id=self.shop_id
            tmp_partner_key='4872506d726a7657535867436c416350576e6a6b416e4e49657177726b436f79'
            access_token=self.access_token
            host = "https://partner.shopeemobile.com"
            path='/api/v2/logistics/get_tracking_number'
            data_tracking_number={}
            order_sn=self.list_order_sn
            
            for j in tqdm(order_sn):
                shop_code=self.shop_code
                response=requests.get(f'https://enabler.orbiz.id/api/v1/stores/noverse/{shop_code}',headers=headers)
                data_token=json.loads(response.text)
                self.partner_id='2004863'
                self.shop_id=data_token['decrypt']['shop_id']
                self.tmp_partner_key='4872506d726a7657535867436c416350576e6a6b416e4e49657177726b436f79'
                self.access_token=data_token['decrypt']['access_token']
                timest = int(time.time())
                sign=signature(partner_id,path,timest,access_token,shop_id,tmp_partner_key)
                url = f"{host}{path}?access_token={access_token}&partner_id={partner_id}&shop_id={shop_id}&sign={sign}&timestamp={timest}"
                params={'order_sn':j}
                response=requests.get(url,params=params).json()
                data_tracking_number[j]=response['response']['tracking_number']
            print('get_tracking_number_done...',end='\r')
            return data_tracking_number
        def get_tracking_info(self):
            print('getting tracking info....',end='\r')
            from time import strftime, localtime
            from tqdm.notebook import tqdm
            partner_id='2004863'
            shop_id=self.shop_id
            tmp_partner_key='4872506d726a7657535867436c416350576e6a6b416e4e49657177726b436f79'
            access_token=self.access_token
            host = "https://partner.shopeemobile.com"
            path = "/api/v2/logistics/get_tracking_info"
            
            order_sn=self.list_order_sn
            data_tracking_info={}
            for j in tqdm(order_sn):
                
                timest = int(time.time())
                sign=signature(partner_id,path,timest,access_token,shop_id,tmp_partner_key)
                url = f"{host}{path}?access_token={access_token}&partner_id={partner_id}&shop_id={shop_id}&sign={sign}&timestamp={timest}"
                params={
                                'order_sn':j
                            }
                response=requests.get(url,params=params).json()
                data_tracking_info[j]=response['response']['tracking_info']
            return data_tracking_info
    data_status_nmv=['SHIPPED','COMPLETED']
    data_frame_final=pd.DataFrame()
    from sqlalchemy import create_engine
    engine=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
    data_frame_raw=[]
    for i in data_status_nmv:
        shopee_api=ApiData('shopee',26,i)
        data_mar=shopee_api.data_invoice_list('2025/03/1','2025/03/14')
        data_order=shopee_api.get_data_all()
        data_frame_raw.append(data_order)
    return data_frame_raw


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
