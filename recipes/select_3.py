from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config= {
  'secure_connect_bundle': 'C:/Users/user/Documents/GitHub/recipes/recipes/secure-connect-recipes.zip'
}
auth_provider = PlainTextAuthProvider("rbmuBBtqEGPLxfWOnynEEyaP", "Cy10zjFZGx9yBz_JJZhEpG2CB2GhiN2aWpscs2woRB_7Z63gROoPljbT+Ry7Fz,S11d0bxIPHXEPZ34_YOHRSGrnnZOsS15rs-0sere5ev2CIiKIgGp2+aywc7GAXF8L")
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.default_timeout = 60

future = session.execute_async("select recipe_id, name, rating from recipes.recipes_by_difficulty where category='easy' order by rating DESC;", trace=True)
result = future.result()
trace = future.get_query_trace()

for row in result:
 print(row[0], row[1], row[2])

for e in trace.events:
  print (e.source_elapsed, e.description)

cluster.shutdown()