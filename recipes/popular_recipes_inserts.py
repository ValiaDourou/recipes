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

with open('interactions.json') as data_file:    
    data = json.load(data_file)
    with open('recipes.json') as file2:    
     data2 = json.load(file2)
    for v in data:
       recipe_id=v['recipe_id']
       user_id=v['user_id']
       rating=v['rating']
       date=v['date']
       for f in data2:
         if(f['id']==recipe_id):
           name=f['name']
       session.execute("""INSERT INTO recipes.popular_recipes (recipe_id,user_id,rating,date,name)VALUES (%s,%s,%s,%s,%s)""",(int(recipe_id), int(user_id),float(rating),date, name)) 