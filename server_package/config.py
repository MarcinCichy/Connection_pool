from configparser import ConfigParser


def load_config(filename='settings.ini', section=None):
    if section is None:
        raise ValueError("Section must be specified")

    parser = ConfigParser()
    parser.read(filename)

    if not parser.has_section(section):
        raise Exception(f'Section {section} not found in the {filename} file')

    return {param[0]: param[1] for param in parser.items(section)}


def db_config(filename='settings.ini'):
    return load_config(filename, 'postgresql')


def connection_pool_config(filename='settings.ini'):
    return load_config(filename, 'connection_pool')


def server_data(filename='settings.ini'):
    return load_config(filename, 'server_data')


def stress_test(filename='settings.ini'):
    return load_config(filename, 'stress_test')