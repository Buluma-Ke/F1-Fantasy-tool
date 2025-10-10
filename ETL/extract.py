import requests
import json

url = 'https://f1fantasytools.com/api/statistics/2024'

def fetch_f1_fantasy_data(url):

        """
        Fetch data from F1 Fantasy Tools API and return as JSON.
        Args   : API url
        Return : JSON file
        """

        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=1, i',
            'referer': 'https://f1fantasytools.com/statistics',
            "sec-ch-ua":'"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
        }


        try:
            response = requests.get(url, headers=headers)

            #print(response)
            data = response.json()

            # Print or inspect
            #print(data)

            #print(json.dumps(data, indent=2)[:2000])

            return data
        except:
              print(f"{response} Error parsing request!!")


fetch_f1_fantasy_data(url)
