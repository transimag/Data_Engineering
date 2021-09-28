import yaml

class Config:
    def __init__(self, path):
        with open(path, 'r') as cfg_file:
            self.config = yaml.safe_load(cfg_file)

    def get_config_api(self, get_api ):
        return self.config[get_api]
