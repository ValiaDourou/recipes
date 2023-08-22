from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

cloud_config= {
  'secure_connect_bundle': 'C:/Users/user/Documents/GitHub/recipes/recipes/secure-connect-recipes.zip'
}
auth_provider = PlainTextAuthProvider("rbmuBBtqEGPLxfWOnynEEyaP", "Cy10zjFZGx9yBz_JJZhEpG2CB2GhiN2aWpscs2woRB_7Z63gROoPljbT+Ry7Fz,S11d0bxIPHXEPZ34_YOHRSGrnnZOsS15rs-0sere5ev2CIiKIgGp2+aywc7GAXF8L")
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.default_timeout = 60

start = time.time()

fq = session.execute("select category,contributor_id,submitted,rating,tags,steps,nutrition,ingredients,n_steps,minutes,n_ingredients,description from recipes.recipe_details where name='chic greek salad';")

for row in fq:
 print(row[0], row[1], row[2])

end = time.time()
print('Execution time:')
print(end - start)

cluster.shutdown()