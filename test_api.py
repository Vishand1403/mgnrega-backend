import requests

api_url = "https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722?api-key=579b464db66ec23bdd0000012ebcd9f379884598719738fb876815ef&format=json&limit=5&filters[state_name]=TAMIL%20NADU"
response = requests.get(api_url)
data = response.json()

for record in data.get("records", []):
    print(record)
    print("\n-----------------\n")
