from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

cloud_config= {
  'secure_connect_bundle': 'C:/Users/user/Documents/GitHub/recipes/recipes/secure-connect-recipes.zip'
}
auth_provider = PlainTextAuthProvider("rbmuBBtqEGPLxfWOnynEEyaP", "Cy10zjFZGx9yBz_JJZhEpG2CB2GhiN2aWpscs2woRB_7Z63gROoPljbT+Ry7Fz,S11d0bxIPHXEPZ34_YOHRSGrnnZOsS15rs-0sere5ev2CIiKIgGp2+aywc7GAXF8L")
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.default_timeout = 60


with open('merged.json') as data_file:    
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
               session.execute("""INSERT INTO recipes.recipes_by_tag (recipe_id,rating,tags,name,submitted)VALUES (%s,%s,%s,%s,%s)""",(int(recipe_id),float(rating),t,name,submitted)) 
          found=0

cluster.shutdown()