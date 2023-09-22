from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

cluster= Cluster(['192.168.1.201'])
session = cluster.connect()
session.set_keyspace('recipes')
session.default_timeout = 60

query = SimpleStatement("select recipe_id, name, rating from recipes.recipes_by_difficulty where category='easy' order by rating DESC;",
consistency_level=ConsistencyLevel.ALL)
future = session.execute_async(query, trace=True)
result = future.result()
trace = future.get_query_trace()

for row in result:
 print(row[0], row[1], row[2])

for e in trace.events:
  print (e.source_elapsed, e.description)

cluster.shutdown()