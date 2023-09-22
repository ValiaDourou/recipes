from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import json

cluster= Cluster(['192.168.1.201'])
session = cluster.connect()
session.set_keyspace('recipes')
session.default_timeout = 60

total_r=0
total_diff=0
r=0

with open('lab_file.json') as file:    
    data = json.load(file)
    for v in data:
     minutes=v['minutes']
     n_steps=v['n_steps']     
     diff=int(minutes)*int(n_steps)
     total_r=total_r+1
     total_diff=total_diff+diff

r=total_diff/total_r
cat1=r/4
cat2=cat1+r/4
cat3=cat2+r/4
print(cat1,cat2,cat3,r)


with open('lab_file.json') as data_file:    
    data = json.load(data_file)
    for v in data:
       recipe_id=v['recipe_id']
       name=v['name']
       if name=='':
        name='unknown'
       contributor_id=v['contributor_id']
       submitted=v['submitted']
       tags=v['tags']
       n_steps=v['n_steps']
       minutes=v['minutes']
       n_ingredients=v['n_ingredients']
       description=v['description']
       steps=v['steps']
       ingredients=v['ingredients']
       nutrition=v['nutrition']
       time=int(minutes)*int(n_steps)
       if time<cat2:
          category='easy'
       elif time<cat3:
          category='moderate'
       elif time<r:
          category='hard'
       else:
          category='expert'
       ucount=0
       rt=0
       for p in v['users']:
          ucount=ucount+1
       for z in v['ratings']:
          rt=rt+int(z)
       rating=rt/ucount
       query = SimpleStatement("INSERT INTO recipes.recipe_details (recipe_id,name,contributor_id,submitted,rating,tags,n_steps,minutes,steps,n_ingredients,ingredients,nutrition,category,description)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
       consistency_level=ConsistencyLevel.ALL)
       try:
         session.execute(query, (int(recipe_id),name,int(contributor_id),submitted,float(rating),tags,int(n_steps),int(minutes),steps,int(n_ingredients),ingredients,nutrition,category,description), None)
       except:
          print('Timeout')
          cluster.shutdown()
cluster.shutdown()