if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

 
 
@custom  
def transform_custom(*args, **kwargs):  
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
    import requests
    import aiohttp
    import asyncio  
    import pprint 
    import json
    import hmac
    import time 
    import hashlib
    import requests
    import pprint 
    import pandas as pd
    import aiohttp
    import json
    from urllib.parse import quote,urlencode 
    import pandas as pd 
    import requests
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
        def __init__(self,platform,code,order_status=None,time_status=None,tracking_info_order_sn=None):
            import numpy as np
            import pandas as pd 
            from datetime import datetime
            from datetime import timedelta 
            from sqlalchemy import create_engine,text
            import pandas as pd
            from tqdm import tqdm
            import time  
            self.tracking_info_order_sn=tracking_info_order_sn
            self.time_status=time_status 
            self.platform=platform
            self.shop_code=code 
            self.call=0
            self.order_status_get=order_status
            self.task_manager()
        def shopee_call(self):
            self.get_access_token()
            print('token initiated!')
            self.call='done' 
            return self.task_manager()
        def task_manager(self):
            if self.platform=='shopee' and self.call!='done':
                return self.shopee_call()  
        def get_access_token(self):  
            try: 
                headers={'token':'L3fhEAKpPH4T6k3IxNQDE7Es73rADCpJXEKM'} 
                response=requests.get(f'https://enabler.orbiz.id/api/v1/stores/noverse/{self.shop_code}',headers=headers).json()
            except:
                time.sleep(10)
                headers={'token':'L3fhEAKpPH4T6k3IxNQDE7Es73rADCpJXEKM'}
                response=requests.get(f'https://enabler.orbiz.id/api/v1/stores/noverse/{self.shop_code}',headers=headers).json()
            data_token=response
            self.partner_id='2004863'
            self.shop_id=data_token['decrypt']['shop_id']  
            self.tmp_partner_key='4872506d726a7657535867436c416350576e6a6b416e4e49657177726b436f79'
            self.access_token=data_token['decrypt']['access_token']
            self.host = "https://partner.shopeemobile.com"
            timest = int(time.time()) 
        def data_invoice_list(self,start_time,end_time):
            from zoneinfo import ZoneInfo 
            from datetime import timedelta
            start_time=datetime.strptime(start_time,'%Y/%m/%d').replace(tzinfo=ZoneInfo('Asia/Jakarta'))
            end_time=datetime.strptime(end_time,'%Y/%m/%d').replace(tzinfo=ZoneInfo('Asia/Jakarta'))
            if end_time-start_time>timedelta(days=15):
                return print('caution Error !  (Maximum days 15) ') 
            else:
                pass
            start_time=int(start_time.timestamp())
            self.start_time=start_time
            end_time=int(end_time.timestamp())
            path = "/api/v2/order/get_order_list"
            timest = int(time.time())
            self.get_access_token()
            sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
            url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
            list_order_sn=[]
            if self.order_status_get!='':
                params={'time_range_field':self.time_status, 
                'time_from':start_time,
                'time_to':end_time,
                'page_size':100,
                'cursor':"",
                'order_status':self.order_status_get
        }
            else :
                params={'time_range_field':self.time_status,
                'time_from':start_time,
                'time_to':end_time,
                'page_size':100,
                'cursor':""}
            print('getting data....',end='\r')
            response=requests.get(url,params=params).json()
            # add serial number on list
            data_sn=response['response']['order_list'] 
            list_order_sn.extend(data_sn) 
            cursor = response['response']['next_cursor']
            more = response['response']['more']
                
            while more is True:
                headers={'token':'L3fhEAKpPH4T6k3IxNQDE7Es73rADCpJXEKM'}
                response=requests.get(f'https://enabler.orbiz.id/api/v1/stores/noverse/{self.shop_code}',headers=headers).status_code
                if response!=200:
                    self.get_access_token()
                else:
                    pass
                    sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                    url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
                    if self.order_status_get!='':
                        params={'time_range_field':self.time_status,
                        'time_from':int(start_time),
                        'time_to':end_time, 
                        'page_size':100,
                        'cursor':cursor,
                        'order_status':self.order_status_get
                }
                    else:
                        params={'time_range_field':self.time_status, 
                        'time_from':int(start_time),
                        'time_to':end_time,
                        'page_size':100,
                        'cursor':cursor
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
    
        async def get_order_detail(self): 
             
            print('get_order_detail....')
            from sqlalchemy import create_engine,text
            import pandas as pd
            import numpy as np
            from time import strftime, localtime
            from datetime import datetime
            from datetime import timezone
            from dateutil import tz
            from tqdm.notebook import tqdm
            from zoneinfo import  ZoneInfo
            engine=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
            query=text('select max(order_id) from main_table ')
            #  NEED TO BE UPDATED
            shop_name_dict={1309152862:'Shopee WiZ Lighting ID',179066421:'Shopee Tefal Indonesia Official Shop',30082703:'Shopee Philips Lighting Official Shop',280207604:'Shopee IMeal Official Shop',1102188272:'Shopee Fumakilla Indonesia Store',64601228:'Shopee Nabati Official Store',13026781:'Shopee SKRINEER',42345408:'Shopee FiberCreme Official Shop',912133875:'Shopee Morin Official Shop',334913120:'Shopee Yeos'}
            engine=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
            if self.tracking_info_order_sn==None:
                list_order=self.list_order_sn
            else :
                list_order=self.tracking_info_order_sn
            a=0
            i=50 
            j=len(list_order)
            if j>0:
                dump_push_sn=[]
                while j>0:
                    push_sn=str(list_order[a:i]).replace('[','').replace("'",
                                                            "").replace(']','').replace(' ','') 
                    dump_push_sn.append(push_sn) 
                    a+=50
                    i+=50
                    j=j-50 
                    if j<50:
                        i=i+j-50
                async def get_requests(url,params):
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url,params=params) as response:
                            data=await response.json()
                            return data
                async def get_order_details_bulky():
                    path = "/api/v2/order/get_order_detail" 
                    timest = int(time.time())
                    sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                    url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
                    tasks=[get_requests(url,{
                            'order_sn_list':push_sn,
                            'response_optional_fields':'item_list,total_amount,payment_method,invoice_data,pay_time,shipping_carrier,package_list,buyer_user_id,buyer_username,recipient_address'
                        }) for push_sn in dump_push_sn]
                    result=await asyncio.gather(*tasks)
                    return result
                async def bundling_data():
                    data_orders=await get_order_details_bulky()
                    dump=[data for data in data_orders]
                    return dump
                    # response=self.get_url().get(url,params=params).json()     
    
                test_dump=await bundling_data()
                dump=[]
                for i in test_dump:
                    dump.extend(i['response']['order_list'])
                check_data_len=len(list_order)-len(dump)
                if check_data_len==0:
                    print('check done, data okay!',end='\r')
                else:
                    print(f'Total Order Detail:{len(dump)}')
                    print(f'Total Order Serial Number:{len(list_order)}')
                    print('some data missing') 
                data_package_number_list=[]
                for i in dump:
                    data_package_number_list.append(dict(('package_number',j['package_number']) for j in i['package_list']))
                data_mapper_sn_pn=dict(( i['package_list'][0]['package_number'],i['order_sn'])for i in dump)
                print(data_package_number_list)
                a=0 
                i=20
                j=len(data_package_number_list)
                data_dump_tracking_number=[]
                tracking_number_list=[]
                while j>0:
                    # try:
                    tracking_number_list.append(data_package_number_list[a:i])
                    a+=20
                    i+=20
                    j=j-20  
                    if j<20:  
                        i=i+j-20
                async def post_request(url,json):
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url,json=json) as response:
                            data=await response.json()
                            return data
                async def get_order_tracking_number():
                    self.get_access_token()  
                    path = "/api/v2/logistics/get_mass_tracking_number"
                    timest=int(time.time())
                    sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                    url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
                    tasks=[post_request(url,{'package_list':tracking_number} )for tracking_number in tracking_number_list]
                    data=await asyncio.gather(*tasks)
                    return data 
                async def get_bulky_tracking_number():
                    data=await get_order_tracking_number()
                    result=[a for a in data]
                    return result 
                data_dump_tracking_number=[]
                test_dump=await get_bulky_tracking_number()
                for i in test_dump:
                    data_dump_tracking_number.extend(i['response']['success_list'])
                data_tracking_number=dict((i['package_number'],i['tracking_number']) for i in data_dump_tracking_number)
                data_tn_mapper=dict((data_mapper_sn_pn[key],data_tracking_number[key]) for key in data_tracking_number)
                data=pd.DataFrame(dump)[['order_sn','order_status']] 
                data_detail=pd.json_normalize(dump,'item_list',['payment_method','buyer_username','order_sn','pay_time','create_time','update_time','shipping_carrier','package_list',['recipient_address','name'],['recipient_address','state'],['recipient_address','city'],['recipient_address','full_address']])
                data_final=pd.merge(data_detail,data,on='order_sn',how='left')
                ## data preprocessing
                # data_final['product_id']=np.where(data_final['model_id']==0,data_final['item_id'],data_final['model_id'])
                data_final['no_resi']=data_final['order_sn'].map(data_tn_mapper)
                # data_final['sku_name']=np.where(data_final['model_name']!='',data_final['item_name']+'-'+data_final['model_name'],data_final['item_name'])
                # data_final['sku']=np.where(data_final['model_sku']=='',data_final['item_sku'],data_final['model_sku']) 
                
                # data_final['key']=data_final['order_sn'].astype('str')+'/'+data_final['promotion_id'].astype('str')+'/'+data_final['product_id'].astype('str') 
            #  repair data!!! 
                tz=ZoneInfo('Asia/Jakarta') 
                data_final['pay_time']=data_final['pay_time'].apply(lambda x:datetime.fromtimestamp(x,tz=tz) if x!=None else 0)
                data_final['create_time']=data_final['create_time'].apply(lambda x:datetime.fromtimestamp(x,tz=tz))
                data_final['create_time']=pd.to_datetime(data_final['create_time']).dt.tz_localize(None)
                data_final['date']=data_final['create_time'].apply(lambda x: datetime.strftime(x,'%d-%m-%Y'))
                data_final['preorder_date']=None
                if self.partner_id=='2004863':
                    data_final['platform_id']=6
                order_status_map={'COMPLETED':690,'PROCESSED':430,'READY_TO_SHIP':401,'TO_CONFIRM_RECEIVE':601,'SHIPPED':500,'CANCELLED':15,'TO_RETURN':550,'UNPAID':103}
                data_final['order_status']=data_final['order_status'].map(order_status_map)
                # adding no_resi                task
                data_tracking_info=self.get_tracking_info()
                data_final['meta_wms']=None 
                path = "/api/v2/shop/get_warehouse_detail" 
                timest = int(time.time())
                sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
                response=requests.get(url).json() 
                try:
                    data_warehouse_detail=pd.json_normalize(response['response']) 
                    data_final['product_location_id']=data_final['product_location_id'].map(lambda x:x[0])
                    data_final=pd.merge(data_final,data_warehouse_detail,left_on='product_location_id',right_on='location_id',how='left')
                    data_final.rename(columns={'shipping_carrier':'logistic_service','city':'warehouse_city','sku':'sku_bi','weight':'product_weight','recipient_address.state':'province','recipient_address.name':'name','recipient_address.full_address':'full_address','recipient_address.city':'city','order_status':'status','update_time':'updated_at'},inplace=True)
                    data_final['store_id']=self.shop_id
                    data_final['store_name']=data_final['store_id'].map(shop_name_dict) 
                except:
                    warehouse_json={'warehouse_id': 100175681, 'warehouse_name': 'warehouse1', 'location_id': 'IDZ', 'holiday_mode_state': 0, 'region': 'ID', 'state': 'DKI JAKARTA', 'city': 'KOTA JAKARTA TIMUR', 'district': 'CAKUNG', 'town': '', 'address': 'Pergudangan PT Alun Indah Blok D A1-7 Jl. Cakung Cilincing RT.3/RW.4, Cakung Bar., Kec. Cakung, Kota Jakarta Timur, Daerah Khusus Ibukota Jakarta 13910', 'zipcode': '13910', 'address_id': 21757155, 'warehouse_type': 1}
                    data_warehouse_detail=pd.DataFrame.from_dict(warehouse_json,orient='index').T
                    data_final['product_location_id']='IDZ'
                    data_final=pd.merge(data_final,data_warehouse_detail,left_on='product_location_id',right_on='location_id',how='left')
                    data_final.rename(columns={'shipping_carrier':'logistic_service','city':'warehouse_city','sku':'sku_bi','weight':'product_weight','recipient_address.state':'province','recipient_address.name':'name','recipient_address.full_address':'full_address','recipient_address.city':'city','order_status':'status','update_time':'updated_at'},inplace=True)
                    data_final['store_id']=self.shop_id
                    data_final['store_name']=data_final['store_id'].map(shop_name_dict)  
            else:
                data_final=None
            return data_final
        async def get_data_payment(self):
            time.sleep(1)
            import numpy as np
            from tqdm.notebook import tqdm
            self.get_access_token()  
            a=0
            dump=[]
            i=50
            list_order=self.list_order_sn
            j=len(list_order)
            if j>0:
                timest = int(time.time())
                data_payment_l=[]  
                dump_list_order_sn=[]
                try_1=0
                while try_1<1:
                    while j>0: 
                        dump_list_order_sn.append(list_order[a:i])
                        a+=50
                        i+=50 
                        j=j-50 
                        if j<50: 
                            i=i+j-50
                    print(dump_list_order_sn) 
                    async def post_request(url,json):
                        async with aiohttp.ClientSession() as session:
                            async with session.post(url,json=json) as response: 
                                data=await response.json() 
                                return data 
                    async def get_payment_data_bulky():
                        path = "/api/v2/payment/get_escrow_detail_batch"
                        timest = int(time.time()) 
                        sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                        url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
                        tasks=[post_request(url,{
                            'order_sn_list':sn
                        } )for sn in dump_list_order_sn]
                        data=await asyncio.gather(*tasks)
                        return data
                    async def get_bulky_payment():
                        data=await get_payment_data_bulky()
                        result=[a for a in data]
                        return result
                    
                    data_payment_l=[]
                    dump_temp=await get_bulky_payment()
                    try:
                        for i in dump_temp:
                            data_order=i['response'] 
                            data_payment_l.extend(data_order)
                        try_1=1
                    except:
                        try_1=0
                
                    
                
                data_payment=pd.json_normalize(data_payment_l,['escrow_detail','order_income','items'],[['escrow_detail','order_sn']])
                data_payment.rename(columns={'discounted_price':'total_payment'},inplace=True)
                data_payment['product_id']=np.where(data_payment['model_id']==0,data_payment['item_id'],data_payment['model_id'])
                data_payment['sku_name']=np.where(data_payment['model_name']!='',data_payment['item_name']+'-'+data_payment['model_name'],data_payment['item_name'])
                data_payment['sku_bi']=np.where(data_payment['model_sku']=='',data_payment['item_sku'],data_payment['model_sku'])
                data_payment.rename(columns={'escrow_detail.order_sn':'order_sn'},inplace=True)
                data_payment.rename(columns={'quantity_purchased':'quantity'},inplace=True)
                data_payment['rn']=data_payment.groupby(['order_sn']).cumcount()+1
                data_payment['key']=data_payment['order_sn'].astype('str')+'/'+data_payment['activity_id'].astype('str')+'/'+data_payment['rn'].astype('str')+'/'+data_payment['product_id'].astype('str')
                data_payment['payment_ref_number']=None
            else:
                data_payment=None
            return  data_payment
        def deleting_duplicate(self): 
            from sqlalchemy import create_engine,text 
            eng=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
            #delete past updated_at    
            with eng.connect() as engine: 
                
                # engine.execute(text('''delete from main_table mt where mt.id in (select id  from(select max(updated_at) over(partition by "_key") max_1,* from main_table )t
                # where updated_at<>max_1 )''')) 
                # engine.execute(text('''delete from payment_table pt where pt.id in (select id  from(select max(updated_at) over(partition by "order_id") max_1,* from payment_table )t
                # where updated_at<>max_1 )'''))
                # engine.execute(text('''delete from shipping_table st where st.id in (select id  from(select max(updated_at) over(partition by "no_resi") max_1,* from shipping_table )t
                # where updated_at<>max_1 )'''))
                # engine.execute(text('''delete from product_table st where st.id in (select id  from(select max(updated_at) over(partition by "product_id") max_1,* from product_table )t
                # where updated_at<>max_1 )'''))
                # engine.execute(text('''delete from platform_table st where st.id in (select id  from(select max(updated_at) over(partition by "store_id") max_1,* from platform_table )t
                # where updated_at<>max_1 )''')) 
                 
                #delete duplicate  
                engine.execute(text('''delete from main_table where id in (select id from(
                select *,row_number() over (partition by "_key" order by "id" desc) rn from main_table)t where t.rn>1)''')) 
                engine.execute(text('''delete from shipping_table  where id in (select id from(
         select *,row_number() over (partition by "no_resi" order by "id" desc) rn from shipping_table)t where t.rn>1)'''))
                engine.execute(text('''delete from payment_table  where  id in (select id from(
        select *,row_number() over (partition by "order_id" order by "id" desc) rn from payment_table)t where t.rn>1)'''))
                engine.execute(text('''delete from platform_table  where  id in (select id from(
        select *,row_number() over (partition by "store_id" order by "id" desc) rn from platform_table)t where t.rn>1)'''))
                engine.execute(text('''delete from users_table where id in (select id from( 
                select *,row_number() over (partition by "invoice_ref_num" order by "id" desc) rn from users_table)t where t.rn>1)'''))
                engine.execute(text('''delete from product_table where id in (select id from(
                select *,row_number() over (partition by "product_id" order by "id" desc) rn from product_table)t where t.rn>1)'''))
                print('duplicate fixed')  
        def get_tracking_number(self): 
            try:
                print('get_tracking_number....')
                partner_id='2004863' 
                
                shop_id=self.shop_id
                tmp_partner_key='4872506d726a7657535867436c416350576e6a6b416e4e49657177726b436f79'
                host = "https://partner.shopeemobile.com"
                path='/api/v2/logistics/get_tracking_number'
                data_tracking_number={}
                self.get_access_token()
                order_sn=self.list_order_sn  
                for j in order_sn:
                     
                    timest = int(time.time()) 
                    try:
                        sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                        params={'order_sn':j}
                        url = f"{host}{path}?access_token={self.access_token}&partner_id={partner_id}&shop_id={shop_id}&sign={sign}&timestamp={timest}"
                        response=requests.get(url,params=params).json()
                        data_tracking_number[j]=response['response']['tracking_number']  
                        
                    except:
                        for i in range(0,5): 
                            try: 
                                print('fail try')
                                params={'order_sn':j}
                                time.sleep(120)  
                                self.get_access_token() 
                                
                                timest=int(time.time())
                                sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                                url = f"{host}{path}?access_token={self.access_token}&partner_id={partner_id}&shop_id={shop_id}&sign={sign}&timestamp={timest}"
                                response=requests.get(url,params=params).json()
                                data_tracking_number[j]=response['response']['tracking_number']
                                break 
                            except:
                                print(f'will retry for {i+1} times')
                    print(f'check_prog_tracking_num {self.start_time}: {round(len(data_tracking_number)/len(order_sn)*100,2)} %',end='\r')
            except: 
                print(response,end='\r')
            print('get_tracking_number_done...',end='\r')
            return data_tracking_number 
        async def get_tracking_info(self):
            self.get_access_token() 
            print('getting tracking info....',end='\r') 
            if self.tracking_info_order_sn!=None:
                list_order=self.tracking_info_order_sn
            else:
                list_order=self.order_sn
            async def get_requests(url,params):
                async with aiohttp.ClientSession() as session:
                    async with session.get(url,params=params) as response:
                        data=await response.json()
                        return data
            async def get_order_tracking_bulky():
                path = "/api/v2/logistics/get_tracking_info"
                timest = int(time.time())
                sign=signature(self.partner_id,path,timest,self.access_token,self.shop_id,self.tmp_partner_key)
                url = f"{self.host}{path}?access_token={self.access_token}&partner_id={self.partner_id}&shop_id={self.shop_id}&sign={sign}&timestamp={timest}"
                tasks=[get_requests(url,{
                        'order_sn':push_sn
                    }) for push_sn in list_order]
                result=await asyncio.gather(*tasks) 
                return result
            async def bundling_data():
                data_orders=await get_order_tracking_bulky()
                dump=[data['response'] for data in data_orders]
                return dump
                # response=self.get_url().get(url,params=params).json()     
 
            test_dump=await bundling_data()
             
            data_tracking=pd.DataFrame.from_dict(test_dump)
            
            return data_tracking
        async def get_data_all(self):
            if self.tracking_info_order_sn==None:
                data_order= await self.get_order_detail()
                if data_order is not None:
                    data_payment = await self.get_data_payment()
                    data_tracking= await self.get_tracking_info()
                    data_order['order_sn']=data_order['order_sn'].astype(str)   
                    data_payment['order_sn']=data_payment['order_sn'].astype(str)
                    data_tracking['order_sn']=data_tracking['order_sn'].astype(str)
                    data_order=data_order.drop_duplicates(subset='order_sn',keep='first') 
                    data_final=pd.merge(data_payment,data_order,left_on='order_sn',right_on='order_sn',how='left',suffixes=('_x', '_y'))
                    data_final=pd.merge(data_final,data_tracking,left_on='order_sn',right_on='order_sn',how='left',suffixes=('_x', '_y'))
                else:
                    print('No Data !')
                    data_final=pd.DataFrame(data=None)
            else:
                local_timezone=pytz.timezone("Asia/Jakarta")  
                data_order= await self.get_order_detail()
                data_tracking= await self.get_tracking_info()
                data_final=pd.merge(data_order,data_tracking,left_on='order_sn',right_on='order_sn',how='left',suffixes=('_x', '_y')).drop_duplicates('no_resi')
                data_final=data_final[['order_sn','tracking_info','no_resi','updated_at']].drop_duplicates('no_resi')
                data_final['updated_at']=data_final['updated_at'].apply(lambda x:datetime.fromtimestamp(int(x))+ timedelta(hours=7))
                data_final['tracking_info']=data_final['tracking_info'].astype('str')
            return data_final
    import pytz  
    import asyncio
    from datetime import datetime 
    data_status_nmv=['COMPLETED','SHIPPED','PROCESSED','SHIPPED','IN_CANCEL']
    data_frame_final=pd.DataFrame() 
    from sqlalchemy import create_engine
    engine=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
    from datetime import timedelta
    import tqdm 
    import sqlalchemy
    from sqlalchemy import create_engine,text
    stores_id=[26,33,65,94,31,100,96,27,67,28]
    data_json_mapping=[ 
  {
    "26": "Shopee Philips Lighting Official Shop",
    "27": "Shopee Morin Official Shop",
    "28": "Shopee SKRINEER",
    "31": "Shopee IMeal Official Shop",
    "33": "Shopee Tefal Indonesia Official Shop",
    "65": "Shopee FiberCreme Official Shop",
    "67": "Shopee Fumakilla Indonesia Store",
    "94": "Shopee Yeos",
    "96": "Shopee WiZ Lighting ID",
    "100": "Shopee Nabati Official Store"
  },
  {
    "26": 30082703,
    "27": 912133875,
    "28": 13026781,
    "31": 280207604,
    "33": 179066421,
    "65": 42345408,
    "67": 1102188272,
    "94": 334913120, 
    "96": 1309152862,
    "100": 64601228
  } 
]   
    done=0
    # while done!=1:
    #     try:
    for store in stores_id: 
        store_id=data_json_mapping[1][str(store)] 
        print(store_id)
        with engine.connect() as conn:  
            with conn.begin():
                    get_invo=engine.execute(text(f'''select distinct invoice_ref_num from main_table mt left join tracking_info ti on mt.no_resi =ti.no_resi where mt.no_resi !='' and mt.no_resi is not null and ti.no_resi is null and mt.shop_id='{store_id}' ''')).fetchall()  
                    get_existed_invo=engine.execute(text(f'''select distinct invoice_ref_num  from main_table mt left join tracking_info ti on  mt.no_resi =ti.no_resi where ti.no_resi !='' and mt.updated_at!=ti.updated_at and mt.shop_id='{store_id}'  ''')).fetchall() 
        print('total length for get_invo:'+str(len(get_invo)))
        if len(get_invo)>0:
            data_final=ApiData('shopee',store,tracking_info_order_sn=[i[0] for i in get_invo]) 
            data_final=asyncio.run(data_final.get_data_all())
            local_timezone=pytz.timezone("Asia/Jakarta")  
            with engine.connect() as exec:  
                    with exec.begin() :    
                            data_final[['no_resi','tracking_info','updated_at']].to_sql('tracking_info',con=exec,if_exists='append',index=False)  
                            print('data uploaded!!')    
        print('total length for get_existed_invo:'+str(len(get_existed_invo)))
        if len(get_existed_invo)>0:
            data_final=ApiData('shopee',store,tracking_info_order_sn=[i[0]for i in get_existed_invo])
            data_final=asyncio.run(data_final.get_data_all())
            with engine.connect() as exec:  
                    with exec.begin() :    
                            for id,row in data_final.iterrows():
                                query_update=text('''update tracking_info set tracking_info=:tracking_info,updated_at=:updated_at where no_resi=:no_resi''')
                                exec.execute(query_update,{
                                'tracking_info':row['tracking_info'],'no_resi':row['no_resi'],'updated_at':row['updated_at']
                                })
            print('data existing tracking info updated!!')  
    else:
        print('No Data Skip')
       
        #     done=1
        # except:
        #     done=0     
                   
        # shopee_api.deleting_duplicate()

    return print('done!') 