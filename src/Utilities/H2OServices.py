import datetime
import os
import re
import smtplib
import sys
import json

import jsonpickle

from pubsub import pub
from threading import Thread
from exceptions import IOError

from GAMUTRawData.odmservices import ServiceManager
from GAMUTRawData.odmdata import Series
from Utilities.DatasetUtilities import FileDetails, H2OManagedResource, OdmDatasetConnection, BuildCsvFiles
from Utilities.HydroShareUtility import HydroShareUtility, HydroShareException, HydroShareUtilityException, \
    HydroShareAccountDetails, ResourceTemplate
from H2OSeries import H2OSeries, OdmSeriesHelper
from Common import *

__title__ = 'H2O Service'


class H2OService:
    GUI_PUBLICATIONS = {
        'logger': lambda message: {'message': message},
        'Datasets_Completed': lambda completed, total: {'completed': completed, 'total': total},
        'File_Failed': lambda filename, message: {'filename': filename, 'message': message},
        'Dataset_Started': lambda resource, done, total: {'started': ((done * 100) / total) - 1, 'resource': resource},
        'Dataset_Generated': lambda resource, done, total: {'completed': (done * 100) / total, 'resource': resource},
        'Files_Uploaded': lambda resource, done, total: {'started': ((done * 100) / total) - 1, 'resource': resource},
        'Uploads_Completed': lambda resource, done, total: {'completed': (done * 100) / total, 'resource': resource}
    }

    def __init__(self, hydroshare_connections=None, odm_connections=None, resource_templates=None, subscriptions=None,
                 managed_resources=None):
        self.HydroShareConnections = hydroshare_connections if hydroshare_connections is not None else {}  # type: dict[str, HydroShareAccountDetails]
        self.DatabaseConnections = odm_connections if odm_connections is not None else {}  # type: dict[str, OdmDatasetConnection]
        self.ResourceTemplates = resource_templates if resource_templates is not None else {}  # type: dict[str, ResourceTemplate]
        self.ManagedResources = managed_resources if managed_resources is not None else {}  # type: dict[str, H2OManagedResource]
        self.Subscriptions = subscriptions if subscriptions is not None else []  # type: list[str]

        InitializeDirectories([APP_SETTINGS.DATASET_DIR, APP_SETTINGS.LOGFILE_DIR])
        sys.stdout = H2OLogger(log_to_gui='logger' in self.Subscriptions)

        self.ThreadedFunction = None  # type: Thread
        self.ThreadKiller = ['Continue']

        self.ActiveHydroshare = None  # type: HydroShareUtility

        self.csv_indexes = ["LocalDateTime", "UTCOffset", "DateTimeUTC"]
        self.qualifier_columns = ["QualifierID", "QualifierCode", "QualifierDescription"]
        self.csv_columns = ["DataValue", "LocalDateTime", "UTCOffset", "DateTimeUTC"]

    def _thread_checkpoint(self):
        return self.ThreadKiller[0] == 'Continue'

    def _generate_datasets(self):
        dataset_count = len(self.ManagedResources)
        current_dataset = 0
        try:
            for resource in self.ManagedResources.itervalues():
                self._thread_checkpoint()
                if resource.resource is None:
                    print 'Error encountered: resource details for resource {} are missing'.format(resource.resource_id)
                    continue

                current_dataset += 1
                self.NotifyVisualH2O('Dataset_Started', resource.resource.title, current_dataset, dataset_count)
                self._thread_checkpoint()

                odm_service = ServiceManager()
                odm_service._current_connection = self.DatabaseConnections[resource.odm_db_name].ToDict()
                series_service = odm_service.get_series_service()
                self._thread_checkpoint()

                chunks = OdmSeriesHelper.DetermineForcedSeriesChunking(resource)
                print '\n -- {} has {} chunks {}'.format(resource.resource.title, len(chunks),
                                                         'per year' if resource.chunk_years else '')
                for chunk in chunks:
                    self._thread_checkpoint()
                    odm_series = [OdmSeriesHelper.GetOdmSeriesFromH2OSeries(series_service, h2o) for h2o in chunk]
                    failed_files = []
                    resource.associated_files += BuildCsvFiles(series_service, odm_series, resource.chunk_years,
                                                               failed_files)
                    for filename, message in failed_files:
                        self.NotifyVisualH2O('File_Failed', filename, message)
                self.NotifyVisualH2O('Dataset_Generated', resource.resource.title, current_dataset, dataset_count)
            print 'Dataset generation completed without error'
        except TypeError as e:
            print 'Dataset generation stopped without finishing'
            if APP_SETTINGS.H2O_DEBUG:
                print 'Exception encountered while running thread: {}'.format(e)
        except Exception as e:
            print 'Exception encountered while generating datasets:\n{}'.format(e)
        self.NotifyVisualH2O('Datasets_Completed', current_dataset, dataset_count)

    def _upload_files(self):
        dataset_count = len(self.ManagedResources)
        current_dataset = 0
        current_account_name = 'None'
        resource_names = []
        for resource in self.ManagedResources.values():
            current_dataset += 1
            self._thread_checkpoint()
            print 'Uploading files to resource {}'.format(resource.resource.title)
            try:
                if self.ActiveHydroshare is None or current_account_name != resource.hs_account_name:
                    print 'Connecting to HydroShare account {}'.format(resource.hs_account_name)
                    self.ConnectToHydroShareAccount(resource.hs_account_name)
                    current_account_name = resource.hs_account_name

                response = self.ActiveHydroshare.updateResourceMetadata(resource.resource)
                if APP_SETTINGS.VERBOSE:
                    print response
                self.ActiveHydroshare.UploadFiles(resource.associated_files, resource.resource_id)
                resource_names.append(resource.resource.title)
                self.NotifyVisualH2O('Files_Uploaded', resource.resource.title, current_dataset, dataset_count)
            except TypeError as e:
                print 'File uploading stopped without finishing'
                if APP_SETTINGS.H2O_DEBUG:
                    print 'Exception encountered while running thread: {}'.format(e)
                break
            except Exception as e:
                print e
        self.NotifyVisualH2O('Uploads_Completed', resource_names, current_dataset, dataset_count)

    def ConnectToHydroShareAccount(self, account_name):
        connection_message = 'Unable to authenticate HydroShare account - please check your credentials'
        connected = False
        try:
            account = self.HydroShareConnections[account_name]
            self.ActiveHydroshare = HydroShareUtility()
            if self.ActiveHydroshare.authenticate(**account.to_dict()):
                connection_message = 'Successfully authenticated HydroShare account details'
                connected = True
        except Exception as e:
            connection_message = 'Unable to authenticate - An exception occurred: {}'.format(e)

        self.NotifyVisualH2O('logger', 'H2OService: ' + str(connection_message))
        return connected

    def FetchResources(self):
        try:
            resources = self.ActiveHydroshare.getAllResources()
            return resources
        except Exception as e:
            connection_message = 'Unable to fetch resources - An exception occurred: {}'.format(e)
            self.NotifyVisualH2O('logger', 'H2OService: ' + str(connection_message))
            return None

    def StopActions(self):
        if self.ThreadedFunction is not None:
            self.ThreadedFunction.join(1)
            self.ThreadKiller = None

    def _start_as_thread(self, thread_func):
        if self.ThreadedFunction is not None and self.ThreadedFunction.is_alive():
            self.ThreadedFunction.join(3)
        self.ThreadKiller = ['Continue']
        self.ThreadedFunction = Thread(target=thread_func)
        self.ThreadedFunction.start()

    def _threaded_operations(self):
        print 'Starting CSV file generation'
        self._generate_datasets()
        print '\nStarting CSV file upload'
        self._upload_files()

    def StartOperations(self, blocking=False):
        if blocking:
            return self._threaded_operations()
        else:
            return self._start_as_thread(self._threaded_operations)

    def NotifyVisualH2O(self, pub_key, *args):
        try:
            if not APP_SETTINGS.GUI_MODE and pub_key in H2OService.GUI_PUBLICATIONS.keys():
                print 'No subscriber for message: {}'.format(H2OService.GUI_PUBLICATIONS[pub_key](*args))
            elif pub_key in self.Subscriptions and pub_key in H2OService.GUI_PUBLICATIONS.keys():
                result = H2OService.GUI_PUBLICATIONS[pub_key](*args)
                pub.sendMessage(pub_key, **result)
            else:
                print 'Nowhere to go for message: {}'.format(H2OService.GUI_PUBLICATIONS[pub_key](*args))
                print 'GUI Mode: {}'.format(APP_SETTINGS.GUI_MODE)
                print 'Pub Key: {}; Exists: {}'.format(pub_key, pub_key in H2OService.GUI_PUBLICATIONS.keys())
        except Exception as e:
            print 'Exception: {}\nUnknown key {} or invalid args {}'.format(e, pub_key, args)

    def to_json(self):
        return {'odm_connections': self.DatabaseConnections,
                'hydroshare_connections': self.HydroShareConnections,
                'resource_templates': self.ResourceTemplates,
                'managed_resources': self.ManagedResources}

    def SaveData(self, output_file=None):
        if output_file is None:
            output_file = APP_SETTINGS.SETTINGS_FILE_NAME
        try:
            json_out = open(output_file, 'w')
            json_out.write(jsonpickle.encode(self.to_json()))
            json_out.close()
            print('Dataset information successfully saved to {}'.format(output_file))
            return True
        except IOError as e:
            print 'Error saving to disk - file name {}\n{}'.format(output_file, e)
            return False

    def LoadData(self, input_file=None):
        if input_file is None:
            input_file = APP_SETTINGS.SETTINGS_FILE_NAME
        try:
            json_in = open(input_file, 'r')
            data = jsonpickle.decode(json_in.read())
            if data is not None:
                self.HydroShareConnections = data['hydroshare_connections'] if 'hydroshare_connections' in data else {}
                self.DatabaseConnections = data['odm_connections'] if 'odm_connections' in data else {}
                self.ResourceTemplates = data['resource_templates'] if 'resource_templates' in data else {}
                self.ManagedResources = data['managed_resources'] if 'managed_resources' in data else {}
            json_in.close()
            print 'Dataset information loaded from {}'.format(input_file)
            return True
        except IOError as e:
            print 'Error reading input file data from {}:\n\t{}'.format(input_file, e)
            return False

    def CreateResourceFromTemplate(self, template):
        """
        :type template: ResourceTemplate
        """
        print 'Creating resource {}'.format(template)
        resource = self.ActiveHydroshare.createNewResource(template)
        return resource


class H2OLogger:
    def __init__(self, logfile_dir=None, log_to_gui=False):
        if logfile_dir is None:
            logfile_dir = APP_SETTINGS.LOGFILE_DIR
        self.log_to_gui = log_to_gui
        self.terminal = sys.stdout
        if APP_SETTINGS.H2O_DEBUG:
            file_name = '{}/H2O_Log_{}.csv'.format(logfile_dir, 'TestFile')
        else:
            file_name = '{}/H2O_Log_{}.csv'.format(logfile_dir, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        self.LogFile = open(file_name, mode='w')

        sys.stdout = self
        if not APP_SETTINGS.H2O_DEBUG:
            sys.stderr = self

    def write(self, message):
        if len(message) > 0 and not message.isspace():
            self.terminal.write(self.prefix_date(message))
            self.LogFile.write(self.prefix_date(message))
            if APP_SETTINGS.GUI_MODE and APP_SETTINGS.VERBOSE:
                pub.sendMessage('logger', message='H2OService: ' + str(message))

    def prefix_date(self, message):
        date_string = datetime.datetime.now().strftime('%H-%M-%S')
        return '{date}: {message}\n'.format(date=date_string, message=message)

    def flush(self):
        pass
