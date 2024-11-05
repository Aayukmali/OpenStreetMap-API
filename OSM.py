#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import geopandas as gpd
import shapely
from shapely.geometry import Point, Polygon, LineString


# In[2]:


overpass_url = 'https://overpass-api.de/api/interpreter'
overpass_query = '''
[out:json];
(
node["maxspeed"] (40.38414618161208,-97.63000488281251,41.77927067287513,-95.14709472656251);
way["maxspeed"] (40.38414618161208,-97.63000488281251,41.77927067287513,-95.14709472656251);
);
out body;
>;

out skel qt;
'''
response = requests.get(overpass_url,params = {'data':overpass_query})
response


# In[3]:


data = response.json()
datas = data['elements']


# In[4]:


## Creating the nodes dataframe.

node = []

for i in range (len(datas)):
    if datas[i]['type'] == 'node' :
        node_dict = dict(
            node_id = datas[i]['id'],
            latitude = datas[i]['lat'],
            longitude = datas[i]['lon']
        )
        node.append(node_dict)
node_df = pd.DataFrame(node)

## Creating a Way df.

way = []

for i in range (len(datas)):
    if datas[i]['type'] == 'way' :
        way_dict = dict(
            way_id = datas[i]['id'],
            highway = datas[i]['tags'].get('highway','N/A'),
            lanes = datas[i]['tags'].get('lanes','N/A'),
            maxspeed = datas[i]['tags'].get('maxspeed','N/A'),
            name = datas[i]['tags'].get('name','N/A'),
            county = datas[i]['tags'].get('county','N/A'),
            oneway = datas[i]['tags'].get('oneway','N/A'),
            surface = datas[i]['tags'].get('surface','N/A'),
            expressway = datas[i]['tags'].get('expressway','N/A'),
            zip_right = datas[i]['tags'].get('tiger:zip_right','N/A'),
            zip_left = datas[i]['tags'].get('tiger:zip_left','N/A'),
            railway = datas[i]['tags'].get('railway','N/A'),
            node_list = datas[i]['nodes']
                    )
        way.append(way_dict)
way_df = pd.DataFrame(way)


## removing the mph value from the maxspeed column.

way_df['maxspeed'] = way_df['maxspeed'].str.replace('[a-zA-Z]', '', regex = True )

## Converting the string data type into integer

way_df['maxspeed'] =  pd.to_numeric(way_df['maxspeed'], errors = 'coerce').astype('Int64')



# In[12]:


## Creating a geometry list.

node_geometry = [Point(xy) for xy in zip(node_df.longitude,node_df.latitude)]


# In[13]:


## Coordinate Reference System:
crs = "EPSG:3857"
## Creating a geographic dataframe:

node_gdf = gpd.GeoDataFrame(node_df,crs = crs, geometry=node_geometry)


# In[15]:


node_gdf.plot()


# In[16]:


## removing the records for railway speeds.
way_df_filtered = way_df[way_df['railway']  != 'rail']
exploded_df = way_df_filtered.explode('node_list')


# In[17]:


exploded_df['node_list'] = pd.to_numeric(exploded_df['node_list'], errors = 'coerce').astype('Int64')


# In[18]:


way_df_filtered


## Joining the exploded way df with the node_df.

osm_df = exploded_df.merge(node_df, left_on = 'node_list', right_on = 'node_id', how = 'inner')


# In[20]:



geometry = [Point(xy) for xy in zip(osm_df.longitude, osm_df.latitude)]
osm_gdf = gpd.GeoDataFrame(osm_df, crs = "EPSG:3587", geometry = geometry)
osm_gdf.head(2)


# In[22]:


osm_gdf.drop(['node_list', 'latitude','longitude', 'railway'], axis = 1, inplace = True)


# In[24]:


osm_gdf1 = osm_gdf.groupby(['way_id', 'highway', 'lanes', 'maxspeed', 'name', 'county', 'oneway',
       'surface', 'expressway', 'zip_right', 'zip_left']).agg({'node_id': lambda x:x.tolist(), 'geometry': lambda x:x.tolist()}).reset_index()



# In[25]:


osm_gdf1['line'] = osm_gdf1.apply(lambda row: LineString(row['geometry']), axis = 1)


# In[26]:


osm_gdf2 = gpd.GeoDataFrame(osm_gdf1, crs = "EPSG:3857", geometry = osm_gdf1['line'])


osm_gdf2.plot()


# In[ ]:




