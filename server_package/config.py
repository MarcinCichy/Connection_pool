from configparser import ConfigParser


def db_config(filename='settings.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return db


def connection_pool_config(filename='settings.ini', section='connection_pool'):
    parser = ConfigParser()
    parser.read(filename)

    cp = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            cp[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return cp


def server_data(filename='settings.ini', section='server_data'):
    parser = ConfigParser()
    parser.read(filename)

    sd = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            sd[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return sd


def stress_test(filename='settings.ini', section='stress_test'):
    parser = ConfigParser()
    parser.read(filename)

    st = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            st[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return st