import datetime
import os
import re
import smtplib
import sys
import json
# from Utilities.DatasetGenerator import *
from pubsub import pub
import time
import wx
import pandas

from threading import Thread

from GAMUTRawData.odmservices import ServiceManager
from GAMUTRawData.odmdata import Series
from Utilities.DatasetUtilities import OdmDatasetConnection, H2ODataset
from Utilities.HydroShareUtility import HydroShareAccountDetails, HydroShareUtility, ResourceTemplate
# from Utilities.Odm2Wrapper import *

from GAMUTRawData.CSVDataFileGenerator import *
# from Utilities.DatasetGenerator import *
from exceptions import IOError
from Utilities.HydroShareUtility import HydroShareUtility, HydroShareException, HydroShareUtilityException

__title__ = 'H2O Service'
WINDOWS_OS = 'nt' in os.name
DIR_SYMBOL = '\\' if WINDOWS_OS else '/'
PROJECT_DIR = '{}'.format(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(PROJECT_DIR))

use_debug_file_naming_conventions = True


class H2ODefaults:
    SETTINGS_FILE_NAME = './operations_file.json'.format()
    PROJECT_DIR = '{}'.format(os.path.dirname(os.path.realpath(__file__)))
    DATASET_DIR = '{}/H2O_dataset_files/'.format(PROJECT_DIR)
    LOGFILE_DIR = '{}/../logs/'.format(PROJECT_DIR)
    SERIES_COLUMN_NAME = lambda series: '{} & {} & QC {}'.format(series.site_code, series.variable_code,
                                                                 series.quality_control_level_code)
    CSV_COLUMNS = ["LocalDateTime", "UTCOffset", "DateTimeUTC"]
    
    GUI_PUBLICATIONS = {
        'logger': lambda message: {'message': message},
        'Dataset_Started': lambda resource, done, total: {'started': ((done * 100) / total) - 1, 'resource': resource},
        'Dataset_Generated': lambda resource, done, total: {'completed': (done * 100) / total, 'resource': resource}
    }


class CSV_DEFAULTS:
    DEFAULT_COLS = ['LocalDateTime', 'UTCOffset', 'DateTimeUTC']


class H2OLogger:
    def __init__(self, logfile_dir=H2ODefaults.LOGFILE_DIR, log_to_gui=False):
        self.log_to_gui = log_to_gui
        self.terminal = sys.stdout
        if use_debug_file_naming_conventions:
            file_name = '{}/H2O_Log_{}.csv'.format(logfile_dir, 'TestFile')
        else:
            file_name = '{}/H2O_Log_{}.csv'.format(logfile_dir, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        self.LogFile = open(file_name, mode='w')

    def write(self, message):
        self.terminal.write(message)
        self.LogFile.write(message)
        # if self.log_to_gui:
        #     pub.sendMessage('logger', message='H2OService: ' + str(message))


class OdmSeriesHelper:
    RE_STRING_PARSER = re.compile(r'^(?P<SiteCode>\w+) +(?P<VariableCode>\S+) +QC (?P<QualityControlLevelCode>\S+) +'
                                  r'(?P<SourceID>[\d.]+) +(?P<MethodID>[\d.]+)$', re.I)
    MATCH_ON_ATTRIBUTE = {
        'Site': lambda first_series, second_series: first_series.SiteCode == second_series.SiteCode,
        'Variable': lambda first_series, second_series: first_series.VariableCode == second_series.VariableCode,
        'QC Code': lambda first_series, second_series: first_series.QualityControlLevelCode ==
                                                       second_series.QualityControlLevelCode,
        'Source': lambda first_series, second_series: first_series.SourceID == second_series.SourceID,
        'Method': lambda first_series, second_series: first_series.MethodID == second_series.MethodID
    }
    FORMAT_STRING = '{:<22} {:<27} QC {:<7} {:<5} {}'

    @staticmethod
    def SeriesToString(series):
        format_string = OdmSeriesHelper.FORMAT_STRING
        if isinstance(series, H20Series):
            return format_string.format(series.SiteCode, series.VariableCode, series.QualityControlLevelCode,
                                           series.SourceID, series.MethodID)
        elif isinstance(series, Series):
            return format_string.format(series.site_code, series.variable_code, series.quality_control_level_code,
                                        series.source_id, series.method_id)
        return 'Unable to create string from object type {}'.format(type(series))


    @staticmethod
    def OdmSeriesToString(series):
        if series is not None:
            return str(OdmSeriesHelper.CreateH2OSeriesFromOdmSeries(series))
        else:
            return "A series cannot be type (None)"

    @staticmethod
    def CreateH2OSeriesFromOdmSeries(series):
        """
        :type series: Series
        """
        return H20Series(SeriesID=series.id, SiteID=series.site_id, VariableID=series.variable_id,
                         MethodID=series.method_id, SourceID=series.source_id, VariableCode=series.variable_code,
                         QualityControlLevelID=series.quality_control_level_id, SiteCode=series.site_code,
                         QualityControlLevelCode=series.quality_control_level_code)

    @staticmethod
    def HashOdmSeriesObject(series):
        """
        :type series: Series
        """
        return hash(str(series))

    @staticmethod
    def PopulateH2OSeriesFromString(series_string):
        regex_results = OdmSeriesHelper.RE_STRING_PARSER.match(series_string)
        if regex_results is not None:
            return H20Series(**regex_results.groupdict())
        else:
            return None


class CsvFileHelper:
    @staticmethod
    def DetermineForcedSeriesChunking(series_list):
        """

        :type series_list: list[H2OSeries]
        """
        csv_files = {}
        for series in series_list:
            series_tuple = (series.SiteID, series.SourceID, series.QualityControlLevelID)
            if series_tuple not in csv_files.keys():
                csv_files[series_tuple] = []
            csv_files[series_tuple].append(series)
        return csv_files

    @staticmethod
    def createFile(filepath):
        try:
            print 'Creating new file {}'.format(filepath)
            return open(filepath, 'w')
        except Exception as e:
            print('---\nIssue encountered while creating a new file: \n{}\n{}\n---'.format(e, e.message))
            return None

class H20Series:
    def __init__(self, SeriesID=None, SiteID=None, SiteCode=None, VariableID=None, VariableCode=None, MethodID=None,
                 SourceID=None, QualityControlLevelID=None, QualityControlLevelCode=None):
        self.SeriesID = SeriesID if SeriesID is not None else -1  # type: int
        self.SiteID = SiteID if SiteID is not None else -1  # type: int
        self.SiteCode = SiteCode if SiteCode is not None else ""  # type: str
        self.VariableID = VariableID if VariableID is not None else -1  # type: int
        self.VariableCode = VariableCode if VariableCode is not None else ""  # type: str
        self.MethodID = MethodID if MethodID is not None else -1  # type: int
        self.SourceID = SourceID if SourceID is not None else -1  # type: int
        self.QualityControlLevelID = QualityControlLevelID if QualityControlLevelID is not None else -1  # type: int
        self.QualityControlLevelCode = QualityControlLevelCode if QualityControlLevelCode is not None else -1  # type: float

    def __hash__(self):
        return hash((self.SiteCode, self.VariableCode, self.MethodID, self.SourceID, self.QualityControlLevelCode))

    def __str__(self):
        return OdmSeriesHelper.SeriesToString(self)

    def __eq__(self, other):
        if isinstance(other, str):
            return str(self) == str(other)
        else:
            comp_tuple = (self.SiteCode, self.VariableCode, self.MethodID, self.SourceID, self.QualityControlLevelCode)
            other_tuple = None
            if isinstance(other, H20Series):
                other_tuple = (other.SiteCode, other.VariableCode, other.MethodID, other.SourceID,
                               other.QualityControlLevelCode)
            elif isinstance(other, Series):
                other_tuple = (other.site_code, other.variable_code, other.method_id, other.source_id,
                               other.quality_control_level_code)
            elif isinstance(other, dict):
                other_tuple = (other.get('SiteCode', None), other.get('VariableCode', None),
                               other.get('MethodID', None), other.get('SourceID', None),
                               other.get('QualityControlLevelCode', None))
            if other_tuple is None:
                print('Object types are not similar enough to match anything. "other" was type {}'.format(type(other)))
            return comp_tuple == other_tuple

    def __ne__(self, other):
        return not (self == other)


class H2OService:
    def __init__(self, hydroshare_connections=None, odm_connections=None, resource_templates=None, datasets=None,
                 subscriptions=None):
        self.HydroShareConnections = hydroshare_connections if hydroshare_connections is not None else {}  # type: dict[str, HydroShareAccountDetails]
        self.DatabaseConnections = odm_connections if odm_connections is not None else {}  # type: dict[str, OdmDatasetConnection]
        self.ResourceTemplates = resource_templates if resource_templates is not None else {}  # type: dict[str, ResourceTemplate]
        self.Datasets = datasets if datasets is not None else {}  # type: dict[str, H2ODataset]
        self.Subscriptions = subscriptions if subscriptions is not None else []  # type: list[str]

        self._initialize_directories([H2ODefaults.DATASET_DIR, H2ODefaults.LOGFILE_DIR])
        sys.stdout = H2OLogger(log_to_gui='logger' in self.Subscriptions)

        self.ThreadedFunction = None  # type: Thread
        self.ThreadKiller = ['Continue']

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
        return range(start_date.year, end_date.year + 1)

    def _process_csv_file(self, series_service, dataset, site_id, qc_id, source_id, series_list, year=None):
        # Perform the query for the data we want
        vars = set([series.VariableID for series in series_list])
        methods = set([series.MethodID for series in series_list])

        if dataset.single_file:
            dataframe = series_service.get_values_by_filters(site_id, qc_id, source_id, methods, vars, year)
            if len(dataframe) == 0:
                return

            # CSV file generation
            site_code = series_list[0].SiteCode
            if dataset.chunk_by_year:
                csv_str = '{}ODM_Series_at_{}_Source_{}_QC_Code_{}_{}.csv'.format(H2ODefaults.DATASET_DIR, site_code,
                                                                                  source_id, qc_id, year)
            else:
                csv_str = '{}ODM_Series_at_{}_Source_{}_QC_Code_{}.csv'.format(H2ODefaults.DATASET_DIR, site_code,
                                                                               source_id, qc_id)
            file_out = CsvFileHelper.createFile(csv_str)
            if file_out is None:
                print('Unable to create output file for {}'.format(dataset.name))

            # Set up our table and prepare for CSV output
            csv_table = pandas.pivot_table(dataframe, index=["LocalDateTime", "UTCOffset", "DateTimeUTC"],
                                           columns="VariableCode", values="DataValue")
            del dataframe

            # Generate header for the CSV file

            csv_table.to_csv(file_out)
            file_out.close()
        else:
            for series in series_list:
                self._thread_checkpoint()
                # CSV file generation
                dataframe = series_service.get_values_by_filters(site_id, qc_id, source_id, methods, [series.VariableID], year)
                if len(dataframe) == 0:
                    return

                if dataset.chunk_by_year:
                    csv_str = '{}ODM_Series_{}_at_{}_Source_{}_QC_Code_{}_{}.csv'.format(H2ODefaults.DATASET_DIR, series.VariableCode,
                                                                                         series.SiteCode, source_id, qc_id, year)
                else:
                    csv_str = '{}ODM_Series_{}_at_{}_Source_{}_QC_Code_{}.csv'.format(H2ODefaults.DATASET_DIR, series.VariableCode,
                                                                                      series.SiteCode, source_id, qc_id)
                file_out = CsvFileHelper.createFile(csv_str)
                if file_out is None:
                    print('Unable to create output file for {}'.format(dataset.name))

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

                csv_table.to_csv(file_out)
                file_out.close()


    def _threaded_dataset_generation(self):
        generated_files = []  # type: list[FileDetails]
        try:
            dataset_count = len(self.Datasets)
            current_dataset = 0
            for name, dataset in self.Datasets.iteritems():
                self._thread_checkpoint()

                current_dataset += 1
                self.NotifyVisualH20('Dataset_Started', dataset.destination_resource, current_dataset, dataset_count)

                chunks = CsvFileHelper.DetermineForcedSeriesChunking(dataset.odm_series)

                for csv_file, series_list in chunks.iteritems():
                    self._thread_checkpoint()
                    if len(series_list) == 0:
                        print 'Unable to process csv file {}'.format(csv_file)
                        continue

                    odm_service = ServiceManager()
                    odm_service._current_connection = self.DatabaseConnections[dataset.odm_db_name].ToDict()
                    series_service = odm_service.get_series_service()

                    if dataset.chunk_by_year:
                        years = self._get_year_range(series_service, series_list)
                        for year in years:
                            self._thread_checkpoint()
                            self._process_csv_file(series_service, dataset, csv_file[0], csv_file[2], csv_file[1], series_list, year=year)
                    else:
                        self._process_csv_file(series_service, dataset, csv_file[0], csv_file[2], csv_file[1], series_list)

                self.NotifyVisualH20('Dataset_Generated', dataset.destination_resource, current_dataset, dataset_count)
        except TypeError as e:
            print 'Dataset generation stopped'
        except Exception as e:
            print 'Exception encountered while generating datasets:\n{}'.format(e)


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

    def NotifyVisualH20(self, pub_key, *args):
        if pub_key in self.Subscriptions and pub_key in H2ODefaults.GUI_PUBLICATIONS.keys():
            result = H2ODefaults.GUI_PUBLICATIONS[pub_key](*args)
            pub.sendMessage(pub_key, **result)

    def to_json(self):
        return {'odm_connections': self.DatabaseConnections,
                'hydroshare_connections': self.HydroShareConnections,
                'resource_templates': self.ResourceTemplates,
                'datasets': self.Datasets}

    def SaveData(self, output_file=H2ODefaults.SETTINGS_FILE_NAME):
        try:
            json_out = open(output_file, 'w')
            json_out.write(jsonpickle.encode(self.to_json()))
            json_out.close()
            print('Dataset information successfully saved to {}'.format(output_file))
            return True
        except IOError as e:
            print 'Error saving to disk - file name {}\n{}'.format(output_file, e)
            return False

    def LoadData(self, input_file=H2ODefaults.SETTINGS_FILE_NAME):
        try:
            json_in = open(input_file, 'r')
            data = jsonpickle.decode(json_in.read())
            if data is not None:
                self.HydroShareConnections = data['hydroshare_connections'] if 'hydroshare_connections' in data else {}
                self.DatabaseConnections = data['odm_connections'] if 'odm_connections' in data else {}
                self.ResourceTemplates = data['resource_templates'] if 'resource_templates' in data else {}
                self.Datasets = data['datasets'] if 'datasets' in data else {}

            json_in.close()
            print('Dataset information loaded from {}'.format(input_file))
            return True
        except IOError as e:
            print 'Error reading input file data from {}:\n\t{}'.format(input_file, e)
            return False

    def _initialize_directories(self, directory_list):
        for dir_name in directory_list:
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)

    def GetValidHydroShareConnections(self):
        valid_hydroshare_connections = {}
        for name, account in self.HydroShareConnections.iteritems():
            hydroshare = HydroShareUtility()
            if hydroshare.authenticate(**account.to_dict()):
                valid_hydroshare_connections[name] = account
        return valid_hydroshare_connections

    def GetValidOdmConnections(self):
        valid_odm_connections = {}
        for name, connection in self.DatabaseConnections.iteritems():
            if connection.VerifyConnection():
                valid_odm_connections[name] = connection
        return valid_odm_connections

    def _thread_checkpoint(self):
        return self.ThreadKiller[0] == 'Continue'
