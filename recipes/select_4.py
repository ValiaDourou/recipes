from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config= {
  'secure_connect_bundle': 'C:/Users/user/Documents/GitHub/recipes/recipes/secure-connect-recipes.zip'
}
auth_provider = PlainTextAuthProvider("rbmuBBtqEGPLxfWOnynEEyaP", "Cy10zjFZGx9yBz_JJZhEpG2CB2GhiN2aWpscs2woRB_7Z63gROoPljbT+Ry7Fz,S11d0bxIPHXEPZ34_YOHRSGrnnZOsS15rs-0sere5ev2CIiKIgGp2+aywc7GAXF8L")
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.default_timeout = 60

rows = session.execute("select recipe_id,name,submitted from recipes.recipes_by_tag where tags='crock-pot-slow-cooker';")
session.execute("drop table if exists recipes.temp_tags;")
session.execute("CREATE TABLE recipes.temp_tags (tag TEXT, recipe_id INT, name TEXT, submitted DATE, PRIMARY KEY(tag ,submitted,recipe_id));")
for r in rows:
 session.execute("""INSERT INTO recipes.temp_tags (tag,recipe_id,name,submitted)VALUES (%s,%s,%s,%s)""",('crock-pot-slow-cooker',int(r.recipe_id), r.name, r.submitted)) 

future = session.execute_async("select recipe_id,name,submitted from recipes.temp_tags where tag='crock-pot-slow-cooker' order by submitted DESC;", trace=True)
result = future.result()
trace = future.get_query_trace()

for row in result:
 print(row[0], row[1], row[2])

for e in trace.events:
  print (e.source_elapsed, e.description)

cluster.shutdown()