import requests
import os
import json
from requests.exceptions import HTTPError

from config import Config

def get_token(config):
    r = requests.post(url=config['url'], data=json.dumps(config['data']), headers=config['headers'])
    r.raise_for_status()
    return "JWT " + r.json()['access_token']

def get_api(config, token):
    config['headers']['Authorization'] = token
    r = requests.get(url=config['url'], data=json.dumps(config['data']), headers=config['headers'])
    r.raise_for_status()
    return r.json()

if __name__ == '__main__':

    try:
        config = Config('config.yaml')
        token = get_token(config.get_config_api('auth_param'))

        api_data = config.get_config_api('get_param')['data']
        partition_date = api_data['date']

        directory_path = os.path.join('.', partition_date, partition_date)
        os.makedirs(directory_path, exist_ok=True)
        data= get_api(config.get_config_api('get_param'), token)

        with open(os.path.join(directory_path, partition_date +'.json'), 'w') as json_file:
           json.dump(data, json_file)

    except HTTPError:
        print(f"Error with processing {partition_date} data")

