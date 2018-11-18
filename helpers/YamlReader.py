from ruamel.yaml import YAML

class YamlReader:
    def ymlConfig(path):
        yaml = YAML(typ='safe')
        with open(path, "r") as ymlFile:
            config = yaml.load(ymlFile)
        return config