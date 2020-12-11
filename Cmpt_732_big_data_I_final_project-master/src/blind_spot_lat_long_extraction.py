import ast

import pandas as pd
mapbox= pd.read_csv('CSV_GRAPH_1_7/mapbox2.csv',header=None)
city=[]
street=[]
lat=[]
long=[]
for i in mapbox.iterrows():
    city_name=i[1][0]
    street_name= i[1][1]
    l = ast.literal_eval(i[1][3])
    for j in l:
        # print(j)
        for k in j:
            city.append(city_name)
            street.append(street_name)
            lat.append(k[0])
            long.append(k[1])

df = pd.DataFrame(list(zip(city,street, lat,long)),
               columns =['City','Street', 'lat','long'])

df.to_csv('CSV_GRAPH_1_7/mapbox_2.csv',index=False)


