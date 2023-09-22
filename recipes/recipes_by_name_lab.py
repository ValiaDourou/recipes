from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import json

cluster= Cluster(['192.168.1.201'])
session = cluster.connect()
session.set_keyspace('recipes')
session.default_timeout = 60

with open('lab_file.json') as file:    
    data = json.load(file)
    for v in data:
     recipe_id=v['recipe_id']
     txt=v['name']
     name = txt.split()
     if txt=='':
      name.append('unknown')
     for s in name:
      query = SimpleStatement("INSERT INTO recipes.recipes_by_name (recipe_id,name)VALUES (%s,%s)",
      consistency_level=ConsistencyLevel.ALL)
      session.execute(query, (int(recipe_id),s),timeout=240)

cluster.shutdown()