"""

Contains constants and other common variables

"""

import os
import sys
import inspect
import re


class Common:
    def __init__(self, args):
        """
        Debugging mode
        """
        self.H2O_DEBUG = True if '--debug' in args else False
        self.VERBOSE = True if '--verbose' in args else False
        self.TEST_H2O = True if '--test_h2o' in args else False
        self.DELETE_RESOURCE_FILES = True if '--delete_existing_resource_files' in args else False
        self.SET_RESOURCES_PUBLIC = True if '--make_resources_public' in args else False
        self.SKIP_QUERIES = True if '--skip_queries' in args else False
        self.SKIP_HYDROSHARE = True if '--skip_hydroshare' in args else False

        """
        General constants and non-class variables
        """
        settings_file = 'operations_file.json'
        for item in args:
            if '--settings_file=' in item:
                settings_file = item.split('--settings_file=')[1]

        self.IS_WINDOWS = 'nt' in os.name
        self.PROJECT_DIR = str(os.path.dirname(os.path.realpath(__file__)))
        self.SETTINGS_FILE_NAME = self.PROJECT_DIR + '/' + settings_file
        self.DATASET_DIR = '{}/H2O_dataset_files/'.format(self.PROJECT_DIR)
        self.LOGFILE_DIR = '{}/logs/'.format(self.PROJECT_DIR)
        self.GUI_MODE = False

        """
        H2O-specific constants
        """
        self.CSV_COLUMNS = ["LocalDateTime", "UTCOffset", "DateTimeUTC"]
        self.QUERY_CHUNK_SIZE = 250000 if not self.TEST_H2O else 10
        self.DATAVALUES_TIMEOUT = 6
        self.SERIES_TIMEOUT = 5

        """
        Setup sys and other args
        """
        sys.path.append(os.path.dirname(self.PROJECT_DIR))

    def dump_settings(self):
        for key in self.__dict__:
            print '{:<35} {}'.format(str(key) + ':', self.__dict__[key])


"""
Functions used among H2O Services
"""


def GetSeriesColumnName(series):
    return '{} & {} & QC {}'.format(series.site_code, series.variable_code, series.quality_control_level_code)


def InitializeDirectories(directory_list):
    for dir_name in directory_list:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)


def PRINT_NAME_VALUE(name, var):
    print '{}: {}'.format(name, var)


# noinspection PyUnusedLocal
def varname(p):
    for line in inspect.getframeinfo(inspect.currentframe().f_back)[3]:
        m = re.search(r'\bvarname\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)', line)
        if m:
            return m.group(1)


def print_metadata(value):
    print '\nHydroShare metadata:'
    print print_recursive(value)


def print_recursive(value, indent=0):
    tabs = lambda count: '' + str('    ' * (indent + count))
    if isinstance(value, dict):
        to_print = '{}{}'.format(tabs(1), '{')
        for key, item in value.iteritems():
            to_print += '\n{}{}:\n{}'.format(tabs(2), key, print_recursive(item, indent + 2))
        return to_print + '{}{}'.format('\n' + tabs(1) if len(value) > 0 else ' ', '}')
    if isinstance(value, list):
        to_print = '{}['.format(tabs(1))
        for item in value:
            to_print += '\n' + print_recursive(item, indent + 1)
        return to_print + '{}{}'.format('\n' + tabs(1) if len(value) > 0 else ' ', ']')
    if isinstance(value, str) or isinstance(value, unicode):
        return tabs(1) + '\'' + value + '\''
    if len(str(value)) > 0:
        return tabs(1) + str(value) + ''
    return ''

APP_SETTINGS = Common(sys.argv)
