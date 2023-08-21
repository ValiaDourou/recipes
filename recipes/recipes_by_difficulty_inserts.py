from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

cloud_config= {
  'secure_connect_bundle': 'C:/Users/user/Documents/GitHub/recipes/recipes/secure-connect-recipes.zip'
}
auth_provider = PlainTextAuthProvider("rbmuBBtqEGPLxfWOnynEEyaP", "Cy10zjFZGx9yBz_JJZhEpG2CB2GhiN2aWpscs2woRB_7Z63gROoPljbT+Ry7Fz,S11d0bxIPHXEPZ34_YOHRSGrnnZOsS15rs-0sere5ev2CIiKIgGp2+aywc7GAXF8L")
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.default_timeout = 60

total_r=0
total_diff=0
r=0

with open('recipes.json') as file:    
    data = json.load(file)
    for v in data:
     minutes=v['minutes']
     n_steps=v['n_steps']     
     diff=int(minutes)*int(n_steps)
     total_r=total_r+1
     total_diff=total_diff+diff

r=total_diff/total_r
cat1=r/4
cat2=cat1+r/4
cat3=cat2+r/4
print(cat1,cat2,cat3,r)


with open('merged.json') as data_file:    
    data = json.load(data_file)
    for v in data:
       recipe_id=v['recipe_id']
       name=v['name']
       n_steps=v['n_steps']
       minutes=v['minutes']
       time=int(minutes)*int(n_steps)
       if time<cat2:
          category='easy'
       elif time<cat3:
          category='moderate'
       elif time<r:
          category='hard'
       else:
          category='expert'
       ucount=0
       rt=0
       for p in v['users']:
          ucount=ucount+1
       for z in v['ratings']:
          rt=rt+int(z)
       rating=rt/ucount
       session.execute("""INSERT INTO recipes.recipes_by_difficulty (recipe_id,rating,n_steps,minutes,category,name)VALUES (%s,%s,%s,%s,%s,%s)""",(int(recipe_id),float(rating),int(n_steps),int(minutes),category, name)) 

cluster.shutdown()