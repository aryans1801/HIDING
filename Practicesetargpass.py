#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
#from dotenv import load_dotenv
#import os
import argparse

parser=argparse.ArgumentParser()
parser.add_argument('access_token')
args=parser.parse_args()

#def configure():
    #load_dotenv()

#configure()

def get_data(url, num_records):
    headers={
    'accept': 'application/json',
    'access_token': args.access_token
}

    records_per_page = 100  
    num_pages = (num_records - 1) // records_per_page + 1

    all_records = []
    for page in range(1, num_pages + 1):
        params = {
            "limit": records_per_page,
            "offset": (page - 1) * records_per_page
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            all_records.extend(data["data"])
        else:
            print(f"Error: Failed to fetch data from page {page}. Status code: {response.status_code}")

    return all_records


api_url = "https://zucwflxqsxrsmwseehqvjmnx2u0cdigp.lambda-url.ap-south-1.on.aws/mentorskool/v1/sales"
num_records_to_retrieve = 500

data = get_data(api_url, num_records_to_retrieve)
print("Retrieved records:")
print(data)


# In[2]:


df = pd.json_normalize(data)
df


# In[6]:


df.info()


# In[7]:


#df['product.sizes'] = df['product_sizes'].str.split(',').apply(lambda x: [int(size) for size in x])


#df['range_size'] = df['product.sizes'].apply(lambda sizes:len(sizes))


# In[3]:


l=df[df["product.product_name"]=="Redi-Strip #10 Envelopes, 4 1/8 x 9 1/2"]


# In[5]:


l['product.sizes']


# In[6]:


def product_size(df,name):
    l=df[df["product.product_name"]==name]
    l.drop_duplicates(subset="product.product_name",inplace=True)
    size=l['product.sizes'].values[0]
    sizes=size.split(',')
    print(sizes)
    if(sizes[0]=='null'):
        return 0
    else:
        return len(sizes)


# In[7]:


r=product_size(df,"Mitel 5320 IP Phone VoIP phone")
r


# In[8]:


df


# In[9]:


df.info()


# In[10]:


df['order.order_purchase_date']=pd.to_datetime(df['order.order_purchase_date'])
df['month_name'] = df['order.order_purchase_date'].apply(lambda x: x.strftime('%B'))


# In[11]:


df['profit_amt'] = df['profit_amt'].replace('null',None)


# In[12]:


df.groupby('month_name')['profit_amt'].sum()


# In[13]:


df.groupby('month_name')['sales_amt'].sum()


# In[16]:


profits=df.groupby('month_name')['profit_amt'].sum().reset_index()
profits


# In[19]:


t=df['profit_amt'].sum()
t


# In[27]:


profits['percentage']=profits['profit_amt'].apply(lambda x : round((x/t)*100,2))


# In[28]:


profits


# In[29]:


df.info()


# In[32]:


#df['delay']=df['order.order_delivered_customer_date']-df['order.order_estimated_delivery_date']
df['order.order_delivered_customer_date']=pd.to_datetime(df['order.order_delivered_customer_date'],errors='coerce')
df['order.order_estimated_delivery_date']=pd.to_datetime(df['order.order_estimated_delivery_date'],errors='coerce')


# In[33]:


df.info()


# In[34]:


df['delay']=df['order.order_delivered_customer_date']-df['order.order_estimated_delivery_date']


# In[35]:


df


# In[40]:


def delay_status(x):
    if(x>pd.Timedelta(0)):
        return 'Delayed'
    else:
        return 'On Time'
    


# In[41]:


df['Status']=df['delay'].apply(delay_status)


# In[43]:


df.groupby('Status')['Status'].count()


# In[47]:


df.groupby(['order.vendor.VendorID','Status'])['Status'].count()


# In[ ]:




