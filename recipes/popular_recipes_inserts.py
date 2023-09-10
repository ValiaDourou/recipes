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

with open('merged.json') as data_file:    
    data = json.load(data_file)
    for v in data:
       recipe_id=v['recipe_id']
       name=v['name']
       ucount=0
       rt=0
       userl=[]
       ratingl=[]
       datel=[]
       for p in v['users']:
          ucount=ucount+1
          userl.append(p)
       for z in v['ratings']:
          ratingl.append(z)
       for d in v['dates']:
          datel.append(d)
       for c in range(ucount):
          session.execute("""INSERT INTO recipes.popular_recipes (recipe_id,user_id,rating,date,name)VALUES (%s,%s,%s,%s,%s)""",(int(recipe_id), int(userl[c]),float(ratingl[c]),datel[c], name)) 

cluster.shutdown()