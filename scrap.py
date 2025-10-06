import requests


# url = "https://f1fantasytools.com/api/statistics/2024"


def f1_fantasy_data_recuest(url):

    """
    input API key,url
    returns a JSON file of the data
    """

    headers = {
    "Accept": "application/json",
    "User-Agent": "MyTestClient/1.0"
    }
    resp = requests.get(url, headers=headers, timeout=10)

    print("Status code:", resp.status_code)
    try:
        return print("JSON response:", resp.json())
    except ValueError:
        print("Non_JSON response:")
        return print(resp.text)
