import sys
import os
import logging
import datetime
import pandas
import pyodbc
import jsonpickle
from sqlalchemy.exc import InvalidRequestError
from multiprocessing import Process, Queue
from time import sleep
from sqlalchemy.exc import InvalidRequestError


from GAMUTRawData.odmdata import Series
from GAMUTRawData.odmdata import Site
from GAMUTRawData.odmdata import SpatialReference
from GAMUTRawData.odmdata import Qualifier
from GAMUTRawData.odmdata import DataValue
from GAMUTRawData.odmservices import ServiceManager

this_file = os.path.realpath(__file__)
directory = os.path.dirname(os.path.dirname(this_file))

sys.path.insert(0, directory)

time_format = '%Y-%m-%d'
formatString = '%s  %s: %s'
service_manager = ServiceManager()

class FileDetails(object):
    def __init__(self, site_code="", site_name="", file_path="", file_name="", variable_names=None):
        self.coverage_start = None
        self.coverage_end = None
        self.file_path = file_path
        self.file_name = file_name
        self.site_code = site_code
        self.site_name = site_name
        self.variable_names = [] if variable_names is None else variable_names
        self.is_empty = True
        self.created = False

    def __str__(self):
        fd_str = '{site} - {s_name} - {f_name}'
        return fd_str.format(site=self.site_code, s_name=self.site_name, f_name=self.file_name)

class H2ODataset:
    def __init__(self, name='', odm_series=None, destination_resource='', hs_account_name='', odm_db_name='',
                 create_resource=False, single_file=False, chunk_by_year=False):
        self.name = name  # type: str
        self.odm_series = odm_series if odm_series is not None else {}  # type: dict[int, H20Series]
        self.destination_resource = destination_resource  # type: str
        self.hs_account_name = hs_account_name  # type: str
        self.odm_db_name = odm_db_name  # type: str
        self.create_resource = create_resource  # type: bool
        self.single_file = single_file  # type: bool
        self.chunk_by_year = chunk_by_year  # type: bool

    def __dict__(self):
        return {'name': self.name, 'odm_series': self.odm_series, 'destination_resource': self.destination_resource,
                'hs_account_name': self.hs_account_name, 'create_resource': self.create_resource,
                'single_file': self.single_file, 'chunk_by_year': self.chunk_by_year, 'odm_db_name': self.odm_db_name}

    def __str__(self):
        return 'Dataset {} with {} series and destination resource {}'.format(self.name, len(self.odm_series),
                                                                              self.destination_resource)

def _OdmDatabaseConnectionTestTimed(queue):
    db_auth = queue.get(True)
    if service_manager.test_connection(db_auth):
        queue.put(True)
    else:
        queue.put(False)


class OdmDatasetConnection:
    def __init__(self, values=None):
        self.name = ""
        self.engine = ""
        self.user = ""
        self.password = ""
        self.address = ""
        self.database = ""
        self.port = ""

        if values is not None:
            self.name = values['name'] if 'name' in values else ""
            self.engine = values['engine'] if 'engine' in values else ""
            self.user = values['user'] if 'user' in values else ""
            self.password = values['password'] if 'password' in values else ""
            self.address = values['address'] if 'address' in values else ""
            self.database = values['db'] if 'db' in values else ""
            self.port = values['port'] if 'port' in values else ""

    def __str__(self):
        return 'Dataset connection details {}'.format(self.name)

    def VerifyConnection(self):
        queue = Queue()
        result = False
        process = Process(target=_OdmDatabaseConnectionTestTimed, args=(queue,))
        try:
            process.start()
            queue.put(self.ToDict())
            sleep(2)
            result = queue.get(True, 8)
        except Exception as exc:
            print exc

        if process.is_alive():
            process.terminate()
            process.join()
        return result

    def ToDict(self):
        return {'engine': self.engine, 'user': self.user, 'password': self.password, 'address': self.address,
                'db': self.database}
