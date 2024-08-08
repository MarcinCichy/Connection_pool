from configparser import ConfigParser


def db_config(filename='database.ini', section='postgresql'):
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


def connection_pool_config(filename='database.ini', section='connection_pool'):
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