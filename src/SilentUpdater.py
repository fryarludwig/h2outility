"""

Tool for running H2OServices in headless mode

"""

from Utilities.H2OServices import *
from Common import APP_SETTINGS


if __name__ == "__main__":
    APP_SETTINGS.GUI_MODE = False
    print 'Starting Silent updater'
    service = H2OService()
    service.LoadData()
    service.StartOperations(blocking=True)
    print 'Processing completed'
