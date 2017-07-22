"""

Contains constants and other common variables

"""

import os
import sys
import inspect
import re
import datetime

from Utilities.H2OServices import *

class Common:
    def __init__(self, args):
        """
        Debugging mode
        """
        self.H2O_DEBUG = True if '--debug' in args else False
        self.use_debug_file_naming_conventions = self.H2O_DEBUG

        """
        General constants and non-class variables
        """
        self.IS_WINDOWS = 'nt' in os.name
        self.SETTINGS_FILE_NAME = './operations_file.json'.format()
        self.PROJECT_DIR = str(os.path.dirname(os.path.realpath(__file__)))
        self.DATASET_DIR = '{}/H2O_dataset_files/'.format(self.PROJECT_DIR)
        self.LOGFILE_DIR = '{}/logs/'.format(self.PROJECT_DIR)

        """
        H2O-specific constants
        """
        self.CSV_COLUMNS = ["LocalDateTime", "UTCOffset", "DateTimeUTC"]


        """
        Setup sys and other args
        """
        sys.path.append(os.path.dirname(self.PROJECT_DIR))

    def dump_settings(self):
        for key in self.__dict__:
            print '{:<22}: {}'.format(key, self.__dict__[key])

"""
Functions used among H2O Services
"""
def GetSeriesColumnName(series):
    return '{} & {} & QC {}'.format(series.site_code, series.variable_code, series.quality_control_level_code)

def GetYearList_Inclusive(year_1, year_2):
    return range(year_1, year_2 + 1)

def PRINT_NAME_VALUE(name, var):
    print '{}: {}'.format(name, var)

def varname(p):
  for line in inspect.getframeinfo(inspect.currentframe().f_back)[3]:
    m = re.search(r'\bvarname\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)', line)
    if m:
      return m.group(1)



APP_SETTINGS = Common(sys.argv)

"""
Run this file to perform quick, trivial tests
"""
if __name__ == '__main__':
    print '-----------\nRunning H2O Common debugging tests\n-----------'
    PRINT_NAME_VALUE(varname(APP_SETTINGS), APP_SETTINGS)
    APP_SETTINGS.dump_settings()
    print '-----------\nTests Completed\n-----------'
