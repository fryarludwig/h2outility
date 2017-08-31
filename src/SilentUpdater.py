"""

Tool for running H2OServices in headless mode

"""

from Utilities.H2OServices import *

WINDOWS_OS = 'nt' in os.name
DIR_SYMBOL = '\\' if WINDOWS_OS else '/'
PROJECT_DIR = '{}'.format(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(PROJECT_DIR))

from Common import APP_SETTINGS

if __name__ == "__main__":
    APP_SETTINGS.GUI_MODE = False

    print 'Starting Silent updater'
    service = H2OService()
    service.LoadData()
    service.StartOperations(blocking=True)
    print 'Processing completed'
