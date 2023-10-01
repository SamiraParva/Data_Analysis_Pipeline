import os
from dotenv import dotenv_values

class ConfigManager:
    def __init__(self):
        self.CONFIG = dotenv_values(".env")
        if not self.CONFIG:
            self.CONFIG = os.environ

    def get_config(self):
        return self.CONFIG