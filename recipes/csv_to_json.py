import csv
import json
 
def make_json(csvF, jsonF):
    jsonArray = []
      
    #read csv file
    with open(csvF, encoding='utf-8') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 

        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            jsonArray.append(row)
  
    #convert python jsonArray to JSON String and write to file
    with open(jsonF, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)
         

c1 = r'RAW_recipes.csv'
j1 = r'recipes.json'
c2 = r'RAW_interactions.csv'
j2 = r'interactions.json'

make_json(c1, j1)
make_json(c2, j2)