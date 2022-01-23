import json
import pandas as pd


data=[]
with open('/Users/jai_dev/project/internal/retail_poc/data/test_data/part-00000','rb') as f:
    lines=f.read()

with open("retail_j.json", 'w') as rj:
    json.dump(lines.decode("utf-8"),rj)


df = pd.read_json('retail_j.json', lines=True)
print(df.describe())


"""
# Data to be written
dictionary = {
    "name": "sathiyajith",
    "rollno": 56,
    "cgpa": 8.6,
    "phonenumber": "9976770500"
}

with open("sample.json", "w") as outfile:
    json.dump(dictionary, outfile)

import json

# Data to be written
dictionary = {
    "name": "sathiyajith",
    "rollno": 56,
    "cgpa": 8.6,
    "phonenumber": "9976770500"
}

with open("sample.json", "w") as outfile:
    json.dump(dictionary, outfile)

"""




for x in range(1,10):
    print(x)