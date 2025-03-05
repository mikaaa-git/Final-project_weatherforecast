import requests
import json

api_url = "https://api.airgradient.com/public/api/v1/locations/measures/current?token=ba29ff46-468c-4a4e-a1bc-7f308aace0b2" 

try:
    response = requests.get(api_url)
    response.raise_for_status() # raise an exception for bad status codes.
    data = response.json()
    beautified_json = json.dumps(data, indent=4)
    print(beautified_json)

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")