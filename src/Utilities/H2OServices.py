import datetime
import os
import re
import smtplib
import sys
import json
from pubsub import pub
import time
import wx
import pandas

from threading import Thread
from exceptions import IOError

from GAMUTRawData.odmservices import ServiceManager
from GAMUTRawData.odmdata import Series
from Utilities.DatasetUtilities import FileDetails, H2OManagedResource, OdmDatasetConnection
from HydroShareUtility import HydroShareAccountDetails, HydroShareUtility, ResourceTemplate
from H2OSeries import H2OSeries

from GAMUTRawData.CSVDataFileGenerator import *
from Utilities.HydroShareUtility import HydroShareUtility, HydroShareException, HydroShareUtilityException
from H2OSeries import H2OSeries, OdmSeriesHelper
from Common import *

__title__ = 'H2O Service'


class H2OService:
    GUI_PUBLICATIONS = {
        'logger': lambda message: {'message': message},
        'Datasets_Completed': lambda completed, total: {'completed': completed, 'total': total},
        'Dataset_Started': lambda resource, done, total: {'started': ((done * 100) / total) - 1, 'resource': resource},
        'Dataset_Generated': lambda resource, done, total: {'completed': (done * 100) / total, 'resource': resource}
    }

    def __init__(self, hydroshare_connections=None, odm_connections=None, resource_templates=None, datasets=None,
                 subscriptions=None, managed_resources=None):
        self.HydroShareConnections = hydroshare_connections if hydroshare_connections is not None else {}  # type: dict[str, HydroShareAccountDetails]
        self.DatabaseConnections = odm_connections if odm_connections is not None else {}  # type: dict[str, OdmDatasetConnection]
        self.ResourceTemplates = resource_templates if resource_templates is not None else {}  # type: dict[str, ResourceTemplate]
        self.ManagedResources = managed_resources if managed_resources is not None else {}  # type: dict[str, H2OManagedResource]
        self.Subscriptions = subscriptions if subscriptions is not None else []  # type: list[str]

        self._initialize_directories([APP_SETTINGS.DATASET_DIR, APP_SETTINGS.LOGFILE_DIR])
        sys.stdout = H2OLogger(log_to_gui='logger' in self.Subscriptions)

        self.ThreadedFunction = None  # type: Thread
        self.ThreadKiller = ['Continue']

        self.ActiveHydroshare = None  # type: HydroShareUtility

        self.csv_indexes = ["LocalDateTime", "UTCOffset", "DateTimeUTC"]
        self.qualifier_columns = ["QualifierID", "QualifierCode", "QualifierDescription"]
        self.csv_columns = ["DataValue", "LocalDateTime", "UTCOffset", "DateTimeUTC"]

    def _get_year_range(self, series_service, series_list):
        start_date = None
        end_date = None
        for series in series_list:
            odm_series = series_service.get_series_from_filter(series.SiteID, series.VariableID,
                                                               series.QualityControlLevelID, series.SourceID,
                                                               series.MethodID)
            if start_date is None or start_date > odm_series.begin_date_time:
                start_date = odm_series.begin_date_time
            if end_date is None or end_date < odm_series.end_date_time:
                end_date = odm_series.end_date_time
        return APP_SETTINGS.GET_YEARS_INCLUSIVE(start_date.year, end_date.year)

    def _process_csv_file(self, series_service, managed_resource, site_id, qc_id, source_id, series_list, year=None):
        # Perform the query for the data we want
        """

        :type managed_resource: H2OManagedResource
        """
        vars = set([series.VariableID for series in series_list])
        methods = set([series.MethodID for series in series_list])

        if managed_resource.single_file:
            dataframe = series_service.get_values_by_filters(site_id, qc_id, source_id, methods, vars, year)
            if len(dataframe) == 0:
                return

            # CSV file generation
            site_code = series_list[0].SiteCode
            if managed_resource.chunk_by_year:
                csv_str = '{}ODM_Series_at_{}_Source_{}_QC_Code_{}_{}.csv'.format(APP_SETTINGS.DATASET_DIR, site_code,
                                                                                  source_id, qc_id, year)
            else:
                csv_str = '{}ODM_Series_at_{}_Source_{}_QC_Code_{}.csv'.format(APP_SETTINGS.DATASET_DIR, site_code,
                                                                               source_id, qc_id)
            file_out = OdmSeriesHelper.createFile(csv_str)
            if file_out is None:
                print('Unable to create output file for {}'.format(managed_resource.name))

            # Set up our table and prepare for CSV output
            csv_table = pandas.pivot_table(dataframe, index=["LocalDateTime", "UTCOffset", "DateTimeUTC"],
                                           columns="VariableCode", values="DataValue")
            del dataframe

            # Generate header for the CSV file

            # Write data to CSV file
            csv_table.to_csv(file_out)
            file_out.close()
        else:
            for series in series_list:
                self._thread_checkpoint()
                # CSV file generation
                dataframe = series_service.get_values_by_filters(site_id, qc_id, source_id, methods, [series.VariableID], year)
                if len(dataframe) == 0:
                    return

                if managed_resource.chunk_by_year:
                    csv_str = '{}ODM_Series_{}_at_{}_Source_{}_QC_Code_{}_{}.csv'.format(APP_SETTINGS.DATASET_DIR, series.VariableCode,
                                                                                         series.SiteCode, source_id, qc_id, year)
                else:
                    csv_str = '{}ODM_Series_{}_at_{}_Source_{}_QC_Code_{}.csv'.format(APP_SETTINGS.DATASET_DIR, series.VariableCode,
                                                                                      series.SiteCode, source_id, qc_id)
                file_out = OdmSeriesHelper.createFile(csv_str)
                if file_out is None:
                    print('Unable to create output file for {}'.format(managed_resource.name))

                # # Get the qualifiers that we use in this series, merge it with our DataValue set
                # q_list = [[q.id, q.code, q.description] for q in series_service.get_qualifiers_by_series_id(series.id)]
                # q_df = pandas.DataFrame(data=q_list, columns=self.qualifier_columns)
                # dv_set = dv_raw.merge(q_df, how='left', on="QualifierID")  # type: pandas.DataFrame
                # del dv_raw
                # dv_set.set_index(self.csv_indexes, inplace=True)
                #
                # # Drop the columns that we aren't interested in, and correct any names afterwards
                # for column in dv_set.columns.tolist():
                #     if column not in self.csv_columns:
                #         dv_set.drop(column, axis=1, inplace=True)
                # dv_set.rename(columns={"DataValue": series.variable_code}, inplace=True)

                # Set up our table and prepare for CSV output
                csv_table = pandas.pivot_table(dataframe, index=["LocalDateTime", "UTCOffset", "DateTimeUTC"],
                                               columns="VariableCode", values="DataValue")
                del dataframe

                # Generate headers for each CSV file

                # Write data to CSV file

                csv_table.to_csv(file_out)
                file_out.close()


    def _threaded_dataset_generation(self):
        generated_files = []  # type: list[FileDetails]
        dataset_count = len(self.ManagedResources)
        current_dataset = 0
        try:
            for name, resource in self.ManagedResources.iteritems():
                self._thread_checkpoint()

                if resource.resource is None:
                    print 'Error encountered: resource details for resource {} are missing'.format(name)
                    continue

                current_dataset += 1
                self.NotifyVisualH2O('Dataset_Started', resource.resource.title, current_dataset, dataset_count)

                chunks = OdmSeriesHelper.DetermineForcedSeriesChunking(resource.odm_series)

                for csv_file, series_list in chunks.iteritems():
                    self._thread_checkpoint()
                    if len(series_list) == 0:
                        print 'Unable to process csv file {}'.format(csv_file)
                        continue

                    odm_service = ServiceManager()
                    odm_service._current_connection = self.DatabaseConnections[resource.odm_db_name].ToDict()
                    series_service = odm_service.get_series_service()

                    if resource.chunk_by_year:
                        years = self._get_year_range(series_service, series_list)
                        for year in years:
                            self._thread_checkpoint()
                            self._process_csv_file(series_service, resource, csv_file[0], csv_file[2], csv_file[1], series_list, year=year)
                    else:
                        self._process_csv_file(series_service, resource, csv_file[0], csv_file[2], csv_file[1], series_list)

                self.NotifyVisualH2O('Dataset_Generated', resource.resource.title, current_dataset, dataset_count)
        except TypeError as e:
            print 'Dataset generation stopped'
        except Exception as e:
            print 'Exception encountered while generating datasets:\n{}'.format(e)
        self.NotifyVisualH2O('Datasets_Completed', current_dataset, dataset_count)

    def ConnectToHydroShareAccount(self, account_name):
        connection_message = 'Unable to authenticate HydroShare account - please check your credentials'
        resources = {}
        try:
            account = self.HydroShareConnections[account_name]
            self.ActiveHydroshare = HydroShareUtility()
            if self.ActiveHydroshare.authenticate(**account.to_dict()):
                resources = self.ActiveHydroshare.getAllResources()
                connection_message = 'Successfully authenticated HydroShare account details'
        except:
            connection_message = 'Unable to authenticate HydroShare account - An exception occurred'

        self.NotifyVisualH2O('logger', 'H2OService: ' + str(connection_message))
        return resources

    def StopActions(self):
        if self.ThreadedFunction is not None:
            self.ThreadedFunction.join(1)
            self.ThreadKiller = None

    def GenerateDatasetFiles(self, blocking=False):
        if blocking:
            return self._threaded_dataset_generation()
        if self.ThreadedFunction is not None and self.ThreadedFunction.is_alive():
            self.ThreadedFunction.join(3)
        self.ThreadKiller = ['Continue']
        self.ThreadedFunction = Thread(target=self._threaded_dataset_generation)
        self.ThreadedFunction.start()

    def UploadDatasetsToHydroShare(self, blocking=False):
        if self.ThreadedFunction is not None and self.ThreadedFunction.is_alive():
            self.ThreadedFunction.join(3)
        self.ThreadKiller = ['Continue']
        self.ThreadedFunction = Thread(target=self._threaded_dataset_generation)
        self.ThreadedFunction.start()

    def NotifyVisualH2O(self, pub_key, *args):
        try:
            if pub_key in self.Subscriptions and pub_key in H2OService.GUI_PUBLICATIONS.keys():
                result = H2OService.GUI_PUBLICATIONS[pub_key](*args)
                pub.sendMessage(pub_key, **result)
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
            print('Dataset information loaded from {}'.format(input_file))
            return True
        except IOError as e:
            print 'Error reading input file data from {}:\n\t{}'.format(input_file, e)
            return False

    def CreateResourceFromTemplate(self, template):
        """
        :type template: ResourceTemplate
        """
        print 'Creating reasource {}'.format(template)
        pass

    def _initialize_directories(self, directory_list):
        for dir_name in directory_list:
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)

    # def GetValidHydroShareConnections(self):
    #     valid_hydroshare_connections = {}
    #     for name, account in self.HydroShareConnections.iteritems():
    #         hydroshare = HydroShareUtility()
    #         if hydroshare.authenticate(**account.to_dict()):
    #             valid_hydroshare_connections[name] = account
    #     return valid_hydroshare_connections

    def GetValidOdmConnections(self):
        valid_odm_connections = {}
        for name, connection in self.DatabaseConnections.iteritems():
            if connection.VerifyConnection():
                valid_odm_connections[name] = connection
        return valid_odm_connections

    def _thread_checkpoint(self):
        return self.ThreadKiller[0] == 'Continue'


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

    def write(self, message):
        self.terminal.write(message)
        self.LogFile.write(message)
        if APP_SETTINGS.H2O_DEBUG and not (message is None or len(message) <= 0 or message.isspace()):
            pub.sendMessage('logger', message='H2OService: ' + str(message))

