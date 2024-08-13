from configparser import ConfigParser


def load_config(filename='settings.ini', section=None):
    if section is None:
        raise ValueError("Section must be specified")

    parser = ConfigParser()
    parser.read(filename)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return config


def db_config(filename='settings.ini', section='postgresql'):
    return load_config(filename, section)


def connection_pool_config(filename='settings.ini', section='connection_pool'):
    return load_config(filename, section)


def server_data(filename='settings.ini', section='server_data'):
    return load_config(filename, section)


def stress_test(filename='settings.ini', section='stress_test'):
    return load_config(filename, section)