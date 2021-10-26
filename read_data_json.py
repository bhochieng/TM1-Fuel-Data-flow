import json

#read mongo docs.. Code to change with the new docs ## purpose was to test decoding the mongo files
with open('mongodb_docs/data.json') as json_data:
    data = json.load(json_data)

id = data['_id']
firstname = data['firstname']
lastname = data['lastname']
street = data['address']['street']
city = data['address']['city']

print(id + firstname + lastname + street+city)

hobbies = data['hobbies']
print(hobbies)

x = hobbies[0]
x1 = hobbies[1]
print("x ="+x)
print("x1 ="+x1)
