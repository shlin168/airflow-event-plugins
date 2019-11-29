import ConfigParser


def read_config(conf_file):
    config = ConfigParser.ConfigParser()
    config.read(conf_file)
    return config
