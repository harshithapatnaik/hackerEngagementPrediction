import yaml
from sys import argv

config = None
file_path = argv[1] if len(argv) == 2 else "config.yaml"


# config should only need to be called once then can be retrieved by any file.
def get_config_all(self):
    if not self.config:
        init_config(self)

    return self.config


def get_config(self, section):
    if not self.config:
        init_config(self)

    return self.config.get(section)


def init_config(self):
    with open(self.file_path, "r") as f:
        self.config = yaml.safe_load(f)
