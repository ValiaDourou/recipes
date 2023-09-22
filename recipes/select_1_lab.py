from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider 
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

cluster= Cluster(['192.168.1.201'])
session = cluster.connect()
session.set_keyspace('recipes')
session.default_timeout = 60


rows = session.execute("select recipe_id,name,avg(rating) as avgr from recipes.popular_recipes where date>='2012-01-01' and date<='2012-05-31' group by recipe_id ALLOW FILTERING;")
session.execute("drop table if exists recipes.temp_popular;")
session.execute("CREATE TABLE recipes.temp_popular (date_range TEXT, recipe_id INT, name TEXT, avgr DOUBLE, PRIMARY KEY(date_range ,avgr,recipe_id));")
for r in rows:
 session.execute("""INSERT INTO recipes.temp_popular (date_range,recipe_id,name,avgr)VALUES (%s,%s,%s,%s)""",('2012-01-01 - 2012-05-31',int(r.recipe_id), r.name, int(r.avgr))) 


query = SimpleStatement("select recipe_id,name,avgr from recipes.temp_popular WHERE date_range = '2012-01-01 - 2012-05-31' ORDER BY avgr DESC limit 30;",
consistency_level=ConsistencyLevel.ALL)
future = session.execute_async(query, trace=True)
result = future.result()
trace = future.get_query_trace()
  
for row in result:
 print("recipe_id:",row[0],", name:", row[1],", average rating:",row[2])

for e in trace.events:
  print (e.source_elapsed, e.description)

cluster.shutdown()