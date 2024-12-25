# config.py
import configparser
import os

def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), config_file)
    if not config.read(config_path):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
    
    # Database configuration
    db_config = {
        'host': config['database']['host'],
        'name': config['database']['name'],
        'user': config['database']['user'],
        'password': config['database']['password']
    }

    # Google Sheets configuration
    gs_config = {
        'service_account_file': config['google_sheets']['service_account_file']
    }

    return db_config, gs_config
