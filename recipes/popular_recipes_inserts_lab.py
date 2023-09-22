from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import json

cluster= Cluster(['192.168.1.201'])
session = cluster.connect()
session.set_keyspace('recipes')
session.default_timeout = 60

with open('lab_file.json') as data_file:    
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
          query = SimpleStatement("INSERT INTO recipes.popular_recipes (recipe_id,user_id,rating,date,name)VALUES (%s,%s,%s,%s,%s)",
          consistency_level=ConsistencyLevel.ALL)
          session.execute(query, (int(recipe_id), int(userl[c]),float(ratingl[c]),datel[c], name))

cluster.shutdown()