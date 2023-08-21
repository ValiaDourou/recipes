import json

jsonArray = []

with open('recipes.json') as data_file:    
    data = json.load(data_file)
    with open('interactions.json') as file2:    
     data2 = json.load(file2)
    for v in data:
       jsonUA = []
       jsonRA = []
       jsonDA = []
       recipe_id=v['id']
       name=v['name']
       minutes=v['minutes']
       contributor_id=v['contributor_id']
       submitted=v['submitted']
       tags=v['tags']
       nutrition=v['nutrition']
       n_steps=v['n_steps']
       steps=v['steps']
       description=v['description']
       ingredients=v['ingredients']
       n_ingredients=v['n_ingredients']
       for f in data2:
         if(f['recipe_id']==recipe_id):
           user_id=f['user_id']
           rating=f['rating']
           date=f['date']
           jsonUA.append(user_id)
           jsonRA.append(rating)
           jsonDA.append(date)
       row={"name": name, 
        "recipe_id": recipe_id, 
        "minutes": minutes, 
        "contributor_id": contributor_id, 
        "submitted": submitted, 
        "tags": tags,
        "nutrition": nutrition,
        "n_steps": n_steps,
        "steps": steps,
        "description": description,
        "ingredients": ingredients,
        "n_ingredients": n_ingredients,
        "users": jsonUA,
        "ratings": jsonRA,
        "dates": jsonDA,
        }
       jsonArray.append(row)
       
       
with open("merged.json", 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)
