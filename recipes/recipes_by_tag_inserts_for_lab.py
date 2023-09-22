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
       submitted=v['submitted']
       ucount=0
       rt=0
       for p in v['users']:
          ucount=ucount+1
       for z in v['ratings']:
          rt=rt+int(z)
       rating=rt/ucount
       taglist=[]
       found=0
       txt=v['tags']
       if txt=="['']":
        aList.append('unknown')
       else:
        l=len(txt)
        s=txt[1:l-1]
        aList = s.split(", ")
       for t in aList:
          for a in taglist:
             if a==t:
                found=1
          if found==0:
               t=t[1:len(t)-1]
               taglist.append(t)
               query = SimpleStatement("INSERT INTO recipes.recipes_by_tag (recipe_id,rating,tags,name,submitted)VALUES (%s,%s,%s,%s,%s)",
               consistency_level=ConsistencyLevel.ALL)
               session.execute(query, (int(recipe_id),float(rating),t,name,submitted))
          found=0

cluster.shutdown()