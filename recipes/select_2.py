from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config= {
  'secure_connect_bundle': 'C:/Users/user/Documents/GitHub/recipes/recipes/secure-connect-recipes.zip'
}
auth_provider = PlainTextAuthProvider("rbmuBBtqEGPLxfWOnynEEyaP", "Cy10zjFZGx9yBz_JJZhEpG2CB2GhiN2aWpscs2woRB_7Z63gROoPljbT+Ry7Fz,S11d0bxIPHXEPZ34_YOHRSGrnnZOsS15rs-0sere5ev2CIiKIgGp2+aywc7GAXF8L")
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.default_timeout = 60

sname="chic greek salad"
name = sname.split()
sq1 = session.prepare("select recipe_id from recipes.recipes_by_name where name=?;")
temp=[]
finalarr=[]
cnt=0

for s in name:
 sq = session.execute(sq1, [s])
 for sr in sq:
  if(cnt==0):
   finalarr.append(sr[0])
  else:
   temp.append(sr[0])
 if cnt>0:
  finalarr =  [value for value in finalarr if value in temp]

 cnt=cnt+1


if finalarr:
 for rec_id in finalarr:
  st = session.prepare("select name,recipe_id,category,contributor_id,submitted,rating,tags,steps,nutrition,ingredients,n_steps,minutes,n_ingredients,description from recipes.recipe_details where recipe_id=?;")
  future = session.execute_async(st, [rec_id], trace=True)
  result = future.result()
  trace = future.get_query_trace()
  for e in trace.events:
   print (e.source_elapsed, e.description)
  for row in result:
   if "".join(row[0].split())=="".join(sname.split()):
    print(row[0].strip(), row[1], row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13])

else:
 print("There is no recipe with that name.")

cluster.shutdown()