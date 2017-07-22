"""

Tool for running H2OServices in headless mode

"""

import datetime
import os
import re
import smtplib
import sys
import json
from Utilities.DatasetUtilities import *
from Utilities.H2OServices import *
from pubsub import pub


# __title__ = 'iUtahUtilities Update Tool'
WINDOWS_OS = 'nt' in os.name
DIR_SYMBOL = '\\' if WINDOWS_OS else '/'
PROJECT_DIR = '{}'.format(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(PROJECT_DIR))

from GAMUTRawData.CSVDataFileGenerator import *
from exceptions import IOError
from Utilities.HydroShareUtility import HydroShareUtility, HydroShareException, HydroShareUtilityException


file_path = '{root}{slash}GAMUT_CSV_Files{slash}'.format(root=PROJECT_DIR, slash=DIR_SYMBOL)
log_file = '{file_path}csvgenerator.log'.format(file_path=file_path)
series_dump_location = file_path + 'SeriesFiles/{series}/'


class Arguments:
    """
    Class for defining and parsing command line arguments
    """

    def __init__(self, args):
        self.verbose = False
        self.debug = False
        self.op_file_path = './operations_file.json'
        for arg in args:
            if '--verbose' in arg:
                self.verbose = True
            elif '--file=' in arg:
                self.op_file_path = arg.split('--file=')[1]
            elif '--debug' in arg:
                self.debug = True


    def print_usage_info(self):
        help_string = ("\nLoadCKAN Tool" +
                       "\n   --file=<path>                  Absolute or relative path of operations file" +
                       "\n   --debug                        Not currently used" +
                       "\n   --verbose                      Prints to stdout as well as to log file")
        original_output = None
        if not sys.__stdout__ == sys.stdout:
            print(help_string)
            original_output = sys.stdout
            sys.stdout = sys.__stdout__
        print(help_string)
        print(sys.argv)
        if original_output is not None:
            sys.stdout = original_output


class Logger(object):
    """
    Overrides Python print function and maintains the program log file
    """

    def __init__(self, logfile, overwrite=False):
        self.terminal = sys.stdout
        if overwrite or not os.path.exists(logfile):
            mode = 'w'
        else:
            mode = 'a'
        self.log = open(logfile, mode=mode)

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)


def build_dirs(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


if __name__ == "__main__":
    service = H2OService()
    service.LoadData()
    service.GenerateDatasetFiles(blocking=True)
