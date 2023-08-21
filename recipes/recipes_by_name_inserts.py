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

with open('recipes.json') as file:    
    data = json.load(file)
    for v in data:
     recipe_id=v['id']
     name=v['name']
     if name=='':
      name='unknown'
     session.execute("""INSERT INTO recipes.recipes_by_name (recipe_id,name)VALUES (%s,%s)""",(int(recipe_id),name)) 

cluster.shutdown()