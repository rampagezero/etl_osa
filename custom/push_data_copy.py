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
        def __init__(self,platform,code,order_status,time_status):
            import numpy as np
            import pandas as pd 
            from datetime import datetime
            from datetime import timedelta 
            from sqlalchemy import create_engine,text
            import pandas as pd
            from tqdm import tqdm
            import time  
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
            list_order=self.list_order_sn
            a=0
            i=50 
            j=len(list_order) #452
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
        def get_tracking_info(self):
                  
            return None   
        async def get_data_all(self):
            data_order= await self.get_order_detail()
            data_payment = await self.get_data_payment()
            data_order['order_sn']=data_order['order_sn'].astype(str)   
            data_payment['order_sn']=data_payment['order_sn'].astype(str)
            data_order=data_order.drop_duplicates(subset='order_sn',keep='first')
            data_final=pd.merge(data_payment,data_order,left_on='order_sn',right_on='order_sn',how='left',suffixes=('_x', '_y'))
            return data_final
    import pytz 
    import asyncio
    from datetime import datetime 
    data_status_nmv=['']
    data_frame_final=pd.DataFrame() 
    from sqlalchemy import create_engine
    engine=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
    from datetime import timedelta
    import tqdm 
    import sqlalchemy
    from sqlalchemy import create_engine,text
    stores_id=[33,26,31,67,100,96,65,27,28,94]
    for store in stores_id:
        str_start='2025/5/9'  
        str_end='2025/5/10'     
        local_timezone=pytz.timezone("Asia/Jakarta")  
        # str_start=(datetime.now(local_timezone)).strftime('%Y/%m/%d')     
        # str_end=(datetime.now(local_timezone)+timedelta(days=1)).strftime('%Y/%m/%d')      
        start=datetime.strptime(str_start,'%Y/%m/%d') 
        end=datetime.strptime(str_end,'%Y/%m/%d')
        print(store)
        while start<end:  
            for i in data_status_nmv: 
                # try: 
                    shopee_api=ApiData('shopee',store,i,'update_time')
                    get_start=start
                    get_end=get_start+timedelta(days=1)
                    print(f'getting data for :{get_start}-{get_end}') 
                    data_mar=shopee_api.data_invoice_list(str(get_start).split()[0].replace('-','/'),str(get_end).split()[0].replace('-','/'))
                    data_final=asyncio.run(shopee_api.get_data_all())
                    print('get_order_detail done !!')
                    print('get_data_payment done !') 
                    engine=create_engine('postgresql://orbiz_data:0rbiz123@192.168.33.182:5433/postgres') 
                    with engine.connect() as conn:   
                        with conn.begin():  
                            key_gather=tuple(data_final['key'].to_list()) 
                            query=text(f'select order_id,_key from main_table where _key  in {key_gather}') 
                            result=conn.execute(query) 
                            stored_key=result.fetchall()
                            key_=[j for i,j in stored_key]   
                            order_id_=tuple([i for i,j in stored_key]) 
                            order_id_map=dict((j,i)for i,j in stored_key)
                            print(order_id_map) 
                            if len(order_id_map)>0:
                                local_timezone = pytz.timezone("Asia/Jakarta")
                                existed=data_final[data_final['key'].isin(key_)] 
                                existed['order_id']=existed['key'].map(order_id_map) 
                                existed.rename(columns={'date':'_date','key':'_key','status':'_status','store_id':'shop_id',},inplace=True) 
                                existed['_date']=pd.to_datetime(existed['_date'],dayfirst=True)
                                existed.rename(columns={'order_sn':'invoice_ref_num'},inplace=True)
                                existed.rename(columns={'buyer_username':'_name','shopee_discount':'platform_discount','payment_method':'gateway_name','pay_time':'payment_date','discount_from_voucher_shopee':'discount_from_voucher_platform'},inplace=True)
                                existed['store_id']=store
                                existed.rename(columns={'discounted_price':'total_payment'},inplace=True) 
                                existed['payment_before_discount_platform']=existed['total_payment']-existed['platform_discount']
                                existed['final_payment']=None   
                                existed['product_weight']=None
                                existed['updated_at']=existed['updated_at'].apply(lambda x:datetime.utcfromtimestamp(x).replace(tzinfo=pytz.utc).astimezone(local_timezone)) 
                                invoice_ref_num_tup=tuple(existed['invoice_ref_num'].to_list())
                                no_resi_tup=tuple(existed['no_resi'].to_list())
                                # print(existed['order_id'].to_list()) 
                                        # data_final['total_payment']=data_final['final_payment']*data_final['quantity'] 
                                # existed.loc[:,['_date','store_id','order_id','preorder_date','shop_id','no_resi','invoice_ref_num','product_id','_status','platform_id','payment_before_discount_platform','logistic_service','platform_discount','_key','final_payment','updated_at','quantity','invoice_ref_num','total_payment','create_time','sku_bi','sku_name','product_weight','_name','province','city','full_address','store_name','payment_ref_number','payment_date','gateway_name','promotion_id','warehouse_city']].to_sql('temp_updates',con=engine,if_exists="replace",index=False)
                                if len(order_id_map)>1:
                                    query=text(f'''delete from users_table where invoice_ref_num in {invoice_ref_num_tup}  
                                    ''')
                                    result=conn.execute(query) 
                                    query=text(f'''delete from shipping_table where no_resi in {no_resi_tup} 
                                    ''')
                                    result=conn.execute(query)
                                    query=text(f'''delete from payment_table where order_id in {order_id_} 
                                    ''')
                                    result=conn.execute(query)  
                                    query=text(f'''delete from main_table where invoice_ref_num in {invoice_ref_num_tup} 
                                    ''')
                                    result=conn.execute(query)
                                else: 
                                    query=text(f'''delete from users_table where invoice_ref_num in ('{invoice_ref_num_tup[0]}')
                                    ''')
                                    result=conn.execute(query)
                                    query=text(f'''delete from shipping_table where no_resi in {no_resi_tup[0]} 
                                    ''')
                                    result=conn.execute(query)
                                    query=text(f'''delete from payment_table where order_id in ({order_id_[0]})
                                    ''') 
                                    result=conn.execute(query) 
                                    query=text(f'''delete from main_table where invoice_ref_num in ('{invoice_ref_num_tup[0]}')
                                    ''')
                                    result=conn.execute(query)
                            
                                existed.loc[:,['_date','preorder_date','shop_id','order_id','no_resi','product_id','_status','platform_id','payment_before_discount_platform','platform_discount','_key','final_payment','updated_at','quantity','invoice_ref_num','total_payment','create_time']].to_sql(name='main_table',index=False,con=engine,if_exists='append') 
                                existed.loc[:,['product_id','sku_bi','sku_name','product_weight','updated_at']].to_sql(name='product_table',index=False,con=engine,if_exists='append')  
                                existed.loc[:,['no_resi','meta_wms','warehouse_city','logistic_service','updated_at']].to_sql(name='shipping_table',index=False,con=engine,if_exists='append')
                                existed.loc[:,['_name','province','city','full_address','updated_at','invoice_ref_num']].to_sql(name='users_table',index=False,con=engine,if_exists='append')
                                existed.loc[:,['store_id','store_name','store_name','updated_at','shop_id']].to_sql(name='platform_table',index=False,con=conn,if_exists='append')
                                existed.loc[:,['order_id','payment_ref_number','payment_date','gateway_name','promotion_id','promotion_type']].to_sql(name='payment_table',index=False,con=engine,if_exists='append') 
                                existed.loc[:,['order_id','platform_discount','seller_discount','discount_from_coin','discount_from_voucher_seller','discount_from_voucher_platform','original_price','selling_price']].to_sql(name='temp_discount',index=False,con=engine,if_exists='append')
                                print(f'data updated for {datetime.now()}!') 
                    

                    with engine.connect() as exec: 
                            with exec.begin() :    
                                    query=text('select max(order_id) from main_table ')    
                                    result=exec.execute(query) 
                                    max_id=result.fetchall()    
                                    max_id=max_id[0][0]+1 
                                    if len(key_)!=0: 
                                        data_final=data_final[~(data_final['key'].isin(key_))]
                                    data_final.rename(columns={'date':'_date','key':'_key','status':'_status','store_id':'shop_id',},inplace=True)
                                    data_final['_date']=pd.to_datetime(data_final['_date'],dayfirst=True) 
                                    data_final.rename(columns={'order_sn':'invoice_ref_num'},inplace=True)
                                    data_final.rename(columns={'buyer_username':'_name','shopee_discount':'platform_discount','payment_method':'gateway_name','pay_time':'payment_date','discount_from_voucher_shopee':'discount_from_voucher_platform'},inplace=True)
                                    local_timezone = pytz.timezone("Asia/Jakarta")
                                    data_final['order_id']=[max_id+i for i in data_final.index] 
                                    data_final['store_id']=store
                                    data_final.rename(columns={'discounted_price':'total_payment'},inplace=True)
                                    data_final['payment_before_discount_platform']=data_final['total_payment']-data_final['platform_discount'] 
                                    data_final['final_payment']=None 
                                    data_final['product_weight']=None
                                    # data_final['total_payment']=data_final['final_payment']*data_final['quantity']  
                                    data_final['updated_at']=data_final['updated_at'].apply(lambda x:datetime.utcfromtimestamp(x).replace(tzinfo=pytz.utc).astimezone(local_timezone))
                                    data_final.loc[:,['_date','preorder_date','shop_id','order_id','no_resi','product_id','_status','platform_id','payment_before_discount_platform','platform_discount','_key','final_payment','updated_at','quantity','invoice_ref_num','total_payment','create_time']].to_sql(name='main_table',index=False,con=engine,if_exists='append') 
                                    data_final.loc[:,['product_id','sku_bi','sku_name','product_weight','updated_at']].to_sql(name='product_table',index=False,con=engine,if_exists='append')  
                                    data_final.loc[:,['no_resi','meta_wms','warehouse_city','logistic_service','updated_at']].to_sql(name='shipping_table',index=False,con=engine,if_exists='append')
                                    data_final.loc[:,['_name','province','city','full_address','updated_at','invoice_ref_num']].to_sql(name='users_table',index=False,con=engine,if_exists='append')
                                    data_final.loc[:,['store_id','store_name','store_name','updated_at','shop_id']].to_sql(name='platform_table',index=False,con=engine,if_exists='append')
                                    data_final.loc[:,['order_id','payment_ref_number','payment_date','gateway_name','promotion_id','promotion_type']].to_sql(name='payment_table',index=False,con=engine,if_exists='append')
                                    data_final.loc[:,['order_id','platform_discount','seller_discount','discount_from_coin','discount_from_voucher_seller','discount_from_voucher_platform','original_price','selling_price']].to_sql(name='temp_discount',index=False,con=engine,if_exists='append') 
                                    print('data uploaded!!')  
                                
            start=start+timedelta(days=1)    
        shopee_api.deleting_duplicate()  
    

    return print('done!') 