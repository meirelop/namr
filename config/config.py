import configparser

config = configparser.ConfigParser()
config.read('config/params.ini')

class Config():
    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read('params.ini')
        self.db = config['postgresql']['db']
        self.db_user = config['postgresql']['user']
        self.db_passwd = config['postgresql']['passwd']
        self.db_tables = {
            'BUILDINGS2': config['files']['buildings'].split(),
            'TREES2': config['files']['trees'].split(',')
        }