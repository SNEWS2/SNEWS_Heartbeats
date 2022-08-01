"""
Example initial dosctring
"""
from dotenv import load_dotenv
from datetime import datetime
import configparser
import os


def set_env(env_path=None):
    """ Set environment parameters

    Parameters
    ----------
    env_path : `str`, (optional)
        path for the environment file.
        Use default settings if not given

    """
    dirname = os.path.dirname(__file__)
    default_env_path = os.path.join(dirname, '/auxiliary/test-config.env')
    env = env_path or default_env_path
    load_dotenv(env)


class TimeStuff:
    """ SNEWS format datetime objects

    """
    def __init__(self, env_path=None):
        set_env(env_path)
        self.snews_t_format = os.getenv("TIME_STRING_FORMAT")
        self.hour_fmt = "%H:%M:%S"
        self.date_fmt = "%y_%m_%d"
        self.get_datetime = datetime.utcnow()
        self.get_snews_time = lambda fmt=self.snews_t_format: datetime.utcnow().strftime(fmt)
        self.get_hour = lambda fmt=self.hour_fmt: datetime.utcnow().strftime(fmt)
        self.get_date = lambda fmt=self.date_fmt: datetime.utcnow().strftime(fmt)

    def str_to_datetime(self, nu_time, fmt='%y/%m/%d %H:%M:%S:%f'):
        """ string to datetime object """
        return datetime.strptime(nu_time, fmt)

    def str_to_hr(self, nu_time, fmt='%H:%M:%S:%f'):
        """ string to datetime hour object """
        return datetime.strptime(nu_time, fmt)


def get_config(conf_path=None):
    dirname = os.path.dirname(__file__)
    if conf_path is None:
        conf_path = os.path.join(dirname, "../heartbeats_config.conf")
    config = configparser.ConfigParser()
    config.read(conf_path)
    return config

#
# def update_config(updates, _config):
#     """ Update configuration file locally
#         Takes and updates dictionary
#         Searches for the configuration keys, and replaces
#         them with the updated ones
#     :return:
#     """
#
#     config_string = io.StringIO()
#     _config.write(config_string)
#     # We must reset the buffer ready for reading.
#     config_string.seek(0)
#     new_config = configparser.ConfigParser()
#     new_config.read_file(config_string)
#
#     if updates is None:
#         return new_config
#     config_dict = {s: dict(new_config.items(s)) for s in new_config.sections()}
#     for section, section_dict in updates.items():
#         if section not in config_dict.keys():
#             raise KeyError(f"{section} is not a valid configuration section!")
#         for key, value in section_dict.items():
#             if key not in config_dict[section].keys():
#                 raise KeyError(f"{key} is not a valid configuration key in {section}!")
#             new_config[section][key] = value
#     return new_config
#
