import datetime
from exceptions import IOError
from threading import Thread

import jsonpickle
import sys
from wx.lib.pubsub import pub
# from pubsub import pub

from GAMUTRawData.odmservices import ServiceManager
from H2OSeries import OdmSeriesHelper
from Common import APP_SETTINGS, InitializeDirectories
from Utilities.DatasetUtilities import BuildCsvFile, GetSeriesYearRange, H2OManagedResource, OdmDatasetConnection
from Utilities.HydroShareUtility import HydroShareAccountDetails, HydroShareUtility, ResourceTemplate

__title__ = 'H2O Service'


class H2OService:
    class StopThreadException(Exception):
        def __init__(self, args):
            super(H2OService.StopThreadException, self).__init__(args)

    GUI_PUBLICATIONS = {
        'logger': lambda message: {'message': message},
        'Operations_Stopped': lambda message: {'message': message},
        'Datasets_Completed': lambda completed, total: {'completed': completed, 'total': total},
        'File_Failed': lambda filename, message: {'filename': filename, 'message': message},
        'Dataset_Started': lambda resource, done, total: {'started': ((done * 100) / total) - 1, 'resource': resource},
        'Dataset_Generated': lambda resource, done, total: {'completed': ((done * 100) / total) - 1, 'resource': resource},
        'Files_Uploaded': lambda resource, done, total: {'started': ((done * 100) / total) - 1, 'resource': resource},
        'Uploads_Completed': lambda resource, done, total: {'completed': ((done * 100) / total) - 1, 'resource': resource}
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
        self.StopThread = False

        self.ActiveHydroshare = None  # type: HydroShareUtility

        self.csv_indexes = ["LocalDateTime", "UTCOffset", "DateTimeUTC"]
        self.qualifier_columns = ["QualifierID", "QualifierCode", "QualifierDescription"]
        self.csv_columns = ["DataValue", "LocalDateTime", "UTCOffset", "DateTimeUTC"]

    def RunTests(self):
        dataset_count = len(self.ManagedResources)
        current_dataset = 0
        resource_names = []
        hs_account = None  # type: HydroShareAccount

        for resource in self.ManagedResources.values():
            if APP_SETTINGS.SKIP_HYDROSHARE:
                continue
            print('Uploading files to resource {}'.format(resource.resource.title))
            try:
                if hs_account is None or hs_account.name != resource.hs_account_name:
                    print('Connecting to HydroShare account {}'.format(resource.hs_account_name))
                    try:
                        account = self.HydroShareConnections[resource.hs_account_name]
                        self.ActiveHydroshare = HydroShareUtility()

                        self.ActiveHydroshare.authenticate(**account.to_dict())

                        connection_message = 'Successfully authenticated HydroShare account details'

                    except Exception as e:
                        connection_message = 'Unable to authenticate - An exception occurred: {}'.format(e)
                    finally:
                        print(connection_message)

                resource_files = self.ActiveHydroshare.getResourceFileList(resource.resource_id)
                print('Resource {} has {} files:'.format(resource.resource.title, len(resource_files)))
                for res_file in resource_files:
                    print(res_file)

                resource_names.append(resource.resource.title)
                current_dataset += 1
                self.NotifyVisualH2O('Files_Uploaded', resource.resource.title, current_dataset, dataset_count)
            except H2OService.StopThreadException as e:
                print('File upload stopped: {}'.format(e))
                break
            except Exception as e:
                print(e)
        self.NotifyVisualH2O('Uploads_Completed', resource_names, current_dataset, dataset_count)

    def _thread_checkpoint(self):
        if self.StopThread:
            raise H2OService.StopThreadException(("Thread stopped by user",))
        else:
            return True

    def _generate_datasets(self, resource=None):
        dataset_count = len(self.ManagedResources)
        current_dataset = 0
        odm_service = ServiceManager()

        database_resource_dict = {}

        if resource is not None:

            database_resource_dict[resource.odm_db_name] = [resource]

        else:

            for rsrc in self.ManagedResources.itervalues():

                if not rsrc.odm_db_name:
                    continue

                if rsrc.odm_db_name.lower() == 'no saved connections':
                    continue

                if rsrc.odm_db_name not in database_resource_dict:
                    database_resource_dict[rsrc.odm_db_name] = []

                database_resource_dict[rsrc.odm_db_name].append(rsrc)


        for db_dame in database_resource_dict.keys():
            self._thread_checkpoint()

            conn = self.DatabaseConnections.get(db_dame, None)

            if conn is not None:
                odm_service._current_connection = conn.ToDict()
            else:
                continue

            series_service = odm_service.get_series_service()
            for rsrc in database_resource_dict[db_dame]:

                # Reset the associated files so they don't keep getting uploaded over, and over, and over, and over, and over, and...
                rsrc.associated_files = []

                try:
                    self._thread_checkpoint()
                    if rsrc.resource is None:
                        print('Error encountered: resource {} is missing values'.format(rsrc.resource_id))
                        continue

                    current_dataset += 1
                    self.NotifyVisualH2O('Dataset_Started', rsrc.resource.title, current_dataset, dataset_count)
                    self._thread_checkpoint()

                    chunks = OdmSeriesHelper.DetermineForcedSeriesChunking(rsrc)
                    print('\n -- {} has {} chunks {}'.format(rsrc.resource.title, len(chunks),
                                                             'per year' if rsrc.chunk_years else ''))
                    for chunk in chunks:
                        self._thread_checkpoint()
                        failed_files = []
                        odm_series_list = []
                        for h2o_series in chunk:
                            result_series = series_service.get_series_from_filter(h2o_series.SiteID,
                                                                                  h2o_series.VariableID,
                                                                                  h2o_series.QualityControlLevelID,
                                                                                  h2o_series.SourceID,
                                                                                  h2o_series.MethodID)
                            if result_series is None:
                                msg = 'Error: Unable to fetch ODM series {} from database {}'.format(
                                    h2o_series, db_dame)

                                self.NotifyVisualH2O('Operations_Stopped', msg)

                            else:
                                odm_series_list.append(result_series)

                        if rsrc.chunk_years:

                            for year in GetSeriesYearRange(odm_series_list):
                                self._thread_checkpoint()

                                result_file = BuildCsvFile(series_service, odm_series_list, year, failed_files)
                                if result_file is not None:
                                    rsrc.associated_files.append(result_file)

                        else:
                            self._thread_checkpoint()

                            result_file = BuildCsvFile(series_service, odm_series_list, failed_files=failed_files)
                            if result_file is not None:
                                rsrc.associated_files.append(result_file)

                        for filename, message in failed_files:
                            self.NotifyVisualH2O('File_Failed', filename, message)

                    self.NotifyVisualH2O('Dataset_Generated', rsrc.resource.title, current_dataset, dataset_count)

                    return dataset_count

                except H2OService.StopThreadException as e:
                    print('Dataset generation stopped: {}'.format(e))
                    return 0
                except Exception as e:
                    self.NotifyVisualH2O('Operations_Stopped',
                                         'Exception encountered while generating datasets:\n{}'.format(e))
                    return 0

        print('Dataset generation completed without error')

        self.NotifyVisualH2O('Datasets_Completed', current_dataset, dataset_count)

    def _upload_files(self, resource=None):
        dataset_count = len(self.ManagedResources)
        current_dataset = 0
        resource_names = []

        if resource is not None:
            resources = [resource]
        else:
            resources = self.ManagedResources.values()

        for resource in resources:
            self._thread_checkpoint()

            if APP_SETTINGS.SKIP_HYDROSHARE:
                continue

            if not len(resource.associated_files):
                # If there are no files to upload, continue
                continue

            print('Uploading files to resource {}'.format(resource.resource.title))

            try:
                if self.ActiveHydroshare is None or 'None' != resource.hs_account_name:
                    print('Connecting to HydroShare account {}'.format(resource.hs_account_name))
                    self.ConnectToHydroShareAccount(resource.hs_account_name)

                if APP_SETTINGS.H2O_DEBUG:
                    resource_files = self.ActiveHydroshare.getResourceFileList(resource.resource_id)
                    print('Resource {} has {} files:'.format(resource.resource.title, len(resource_files)))
                    for res_file in resource_files:
                        print(res_file)

                self._thread_checkpoint()

                response = self.ActiveHydroshare.updateResourceMetadata(resource.resource)

                if APP_SETTINGS.VERBOSE and APP_SETTINGS.H2O_DEBUG:
                    print(response)

                self._thread_checkpoint()

                if APP_SETTINGS.DELETE_RESOURCE_FILES:
                    self.ActiveHydroshare.deleteFilesInResource(resource.resource_id)

                self.ActiveHydroshare.UploadFiles(resource.associated_files, resource.resource)

                if APP_SETTINGS.SET_RESOURCES_PUBLIC:
                    self.ActiveHydroshare.setResourcesAsPublic([resource.resource_id])

                resource_names.append(resource.resource.title)

                current_dataset += 1

                self.NotifyVisualH2O('Files_Uploaded', resource.resource.title, current_dataset, dataset_count)

            except H2OService.StopThreadException as e:
                print('File upload stopped: {}'.format(e))
                break
            except Exception as e:
                print(e)
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
            self.StopThread = True
            self.ThreadedFunction.join(3)
        else:
            self.NotifyVisualH2O('Operations_Stopped', 'Script was not running')

    def _start_as_thread(self, thread_func, resource=None):
        if self.ThreadedFunction is not None and self.ThreadedFunction.is_alive():
            self.ThreadedFunction.join(3)
        self.StopThread = False
        self.ThreadedFunction = Thread(target=thread_func, kwargs={'resource': resource})
        self.ThreadedFunction.start()

    def _threaded_operations(self, resource=None):  # type: (H2OManagedResource) -> None
        try:
            print('Generating CSV file(s)')
            dataset_count = self._generate_datasets(resource=resource)

            # If the dataset count is not 0, attempt to upload the files.
            if dataset_count:
                print('\nStarting CSV file upload')
                self._upload_files(resource=resource)
                self.NotifyVisualH2O('Operations_Stopped', 'CSV file upload complete')
            else:
                self.NotifyVisualH2O('Operations_Stopped', 'No datasets found for the selected series.')

        except H2OService.StopThreadException as e:
            print('File generation and uploads stopped: {}'.format(e))
            self.NotifyVisualH2O('Operations_Stopped', 'Script stopped by user')

    def StartSeriesFileUpload(self, resource, blocking=False):  # type: (H2OManagedResource, bool) -> any
        self.StartOperations(resource=resource, blocking=blocking)

    def StartOperations(self, resource=None, blocking=False):  # type: (H2OManagedResource, bool) -> any
        if blocking:
            return self._threaded_operations(resource)
        else:
            return self._start_as_thread(self._threaded_operations, resource)

    def NotifyVisualH2O(self, pub_key, *args):
        try:
            if not APP_SETTINGS.GUI_MODE and pub_key in H2OService.GUI_PUBLICATIONS.keys():
                print('No subscriber for message: {}'.format(H2OService.GUI_PUBLICATIONS[pub_key](*args)))
            elif pub_key in self.Subscriptions and pub_key in H2OService.GUI_PUBLICATIONS.keys():
                result = H2OService.GUI_PUBLICATIONS[pub_key](*args)
                pub.sendMessage(pub_key, **result)
            else:
                print('No destination for message: {}'.format(H2OService.GUI_PUBLICATIONS[pub_key](*args)))
                print('GUI Mode: {}'.format(APP_SETTINGS.GUI_MODE))
                print('Pub Key: {}; Exists: {}'.format(pub_key, pub_key in H2OService.GUI_PUBLICATIONS.keys()))
        except Exception as e:
            print('{} Exception: {}\nUnknown key {} or invalid args {}'.format(type(e), e, pub_key, args))

    def to_json(self):
        return {'odm_connections': self.DatabaseConnections,
                'hydroshare_connections': self.HydroShareConnections,
                'resource_templates': self.ResourceTemplates,
                'managed_resources': self.ManagedResources}

    def SaveData(self, output_file=None):
        if output_file is None:
            output_file = APP_SETTINGS.SETTINGS_FILE_NAME
        try:

            data = jsonpickle.encode(self.to_json())

            with open(output_file, 'w') as fout:
                fout.write(data)

            print('Dataset information successfully saved to {}'.format(output_file))
            return True
        except IOError as e:
            print('Error saving to disk - file name {}\n{}'.format(output_file, e))
            return False

    def LoadData(self, input_file=None):
        if input_file is None:
            input_file = APP_SETTINGS.SETTINGS_FILE_NAME
        try:
            with open(input_file, 'r') as fin:

                data = jsonpickle.decode(fin.read())

                if data is not None:
                    self.HydroShareConnections = data.get('hydroshare_connections', {})
                    self.DatabaseConnections = data.get('odm_connections', {})
                    self.ResourceTemplates = data.get('resource_templates', {})
                    self.ManagedResources = data.get('managed_resources', {})

            print('Dataset information loaded from {}'.format(input_file))

            return data

        except IOError:
            json_out = open(input_file, 'w')
            json_out.write(jsonpickle.encode(self.to_json()))
            json_out.close()
            print('Settings file does not exist - creating: {}'.format(input_file))
            return None

    def CreateResourceFromTemplate(self, template):
        """
        :type template: ResourceTemplate
        """
        print('Creating resource {}'.format(template))

        return self.ActiveHydroshare.createNewResource(template)

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
            self.terminal.write(H2OLogger.prefix_date(message))
            self.LogFile.write(H2OLogger.prefix_date(message))
            self.LogFile.flush()
            if APP_SETTINGS.GUI_MODE and APP_SETTINGS.VERBOSE:
                pub.sendMessage('logger', message='H2OService: ' + str(message))

    @staticmethod
    def prefix_date(message):
        date_string = datetime.datetime.now().strftime('%H-%M-%S')
        return '{date}: {message}\n'.format(date=date_string, message=message)

    def flush(self):
        pass
