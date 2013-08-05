"""
WibiDota - get_heroes. Get an uptodate list of Dota 2 heroes
and the mapping to hero_id. 
Requires environmental variable 'DOTA2_API_KEY' to be set.
"""


import json
import os
import requests

API_KEY = os.environ.get("DOTA2_API_KEY")
LANG='en_us'
BASE_URL = "https://api.steampowered.com/IEconDOTA2_570/GetHeroes/v0001/"

# Where to save the data in JSON form
OUTPUT_JSON = 'src/main/resources/com/wibidata/wibidota/heroes.json'

# Where to save the data in csv form
OUTPUT_CSV = 'src/main/resources/com/wibidata/wibidota/heroes.csv'

def request_heros():
    """
    Requests the hero listing from the DOTA 2 api, returns the result
    as a json object.
    """
    params = dict(key=API_KEY, language=LANG)
    resp = requests.get(BASE_URL, params=params)
    if resp.status_code != requests.codes.ok:
        print "Bad status code for request: %s" % resp.status_code
        exit()
    return json.loads(resp.content)['result']
    
if __name__ == "__main__":
    result = request_heros()
    print("Downloaded: " + str(result['count']) + " heroes")
    f = open(OUTPUT_JSON, 'w')
    f.write(json.dumps(result))
    f.close()
    print("Saved json to: " + OUTPUT_JSON)

    heroes = result['heroes']
