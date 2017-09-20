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

# from HydroShareUtility import HydroShareResource
# from H2OServices import H2OSeries
from GAMUTRawData.odmdata import Series
from GAMUTRawData.odmdata import Site
from GAMUTRawData.odmdata import SpatialReference
from GAMUTRawData.odmdata import Qualifier
from GAMUTRawData.odmdata import DataValue
from GAMUTRawData.odmservices import ServiceManager, SeriesService
from Common import *

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

class H2OManagedResource:
    def __init__(self, resource=None, odm_series=None, resource_id='', hs_account_name='', odm_db_name='',
                 single_file=False, chunk_years=False, associated_files=None):
        self.resource_id = resource_id  # type: str
        self.resource = resource   # type: HydroShareResource
        self.selected_series = odm_series if odm_series is not None else {}  # type: dict[int, H2OSeries]
        self.hs_account_name = hs_account_name  # type: str
        self.odm_db_name = odm_db_name  # type: str
        self.single_file = single_file  # type: bool
        self.chunk_years = chunk_years  # type: bool
        self.associated_files = associated_files if associated_files is not None else []  # type: list[str]

    def __dict__(self):
        return {'resource': self.resource, 'selected_series': self.selected_series,
                'hs_account_name': self.hs_account_name, 'resource_id': self.resource_id,
                'single_file': self.single_file, 'chunk_years': self.chunk_years,
                'odm_db_name': self.odm_db_name, 'associated_files': self.associated_files}

    def __str__(self):
        if self.resource is not None:
            return 'Managed resource {} with {} series'.format(self.resource.title, len(self.selected_series))
        else:
            return 'Managed resource with ID {} and {} series'.format(self.resource_id, len(self.selected_series))

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


# HEADER_LINE = '# '
DELIMITER = '# {}'.format('-' * 90)
CSV_FILENAME_YEAR = '{}ODM_Series_at_{}_Source_{}_QC_Code_{}_{}.csv'
CSV_FILENAME_DEFAULT = '{}ODM_Series_at_{}_Source_{}_QC_Code_{}.csv'


def createFile(filepath):
    try:
        print 'Creating new file {}'.format(filepath)
        return open(filepath, 'w')
    except Exception as e:
        print('---\nIssue encountered while creating a new file: \n{}\n{}\n---'.format(e, e.message))
        return None


def GetTimeSeriesDataframe(series_service, series_list, site_id, qc_id, source_id, methods, variables, year=None):
    csv_table = None
    dataframe = series_service.get_values_by_filters(site_id, qc_id, source_id, methods, variables, year)
    if dataframe is None:
        pass
    elif len(dataframe) == 0:
        pass
    elif qc_id == 0 or len(variables) != 1 or len(methods) != 1:
        csv_table = pandas.pivot_table(dataframe, index=["LocalDateTime", "UTCOffset", "DateTimeUTC"],
                                       columns="VariableCode", values="DataValue")
        del dataframe
    else:
        # Get the qualifiers that we use in this series, merge it with our DataValue set
        qualifier_columns = ["QualifierID", "QualifierCode", "QualifierDescription"]
        q_list = [[q.id, q.code, q.description] for q in
                  series_service.get_qualifiers_by_series_details(site_id, qc_id, source_id, methods[0], variables[0])]
        q_df = pandas.DataFrame(data=q_list, columns=qualifier_columns)
        csv_table = dataframe.merge(q_df, how='left', on="QualifierID")  # type: pandas.DataFrame
        del dataframe
        csv_table.set_index(["LocalDateTime", "UTCOffset", "DateTimeUTC"], inplace=True)
        for column in csv_table.columns.tolist():
            if column not in ["DataValue", "CensorCode", "QualifierCode"]:
                csv_table.drop(column, axis=1, inplace=True)
        csv_table.rename(columns={"DataValue": series_list[0].variable.code}, inplace=True)
    return csv_table


def BuildCsvFiles(series_service, series_list, chunk_years, failed_files=[]):
    # type: (SeriesService, list[Series], bool) -> list[str]
    file_list = []
    base_name = '{}ODM_Series_'.format(APP_SETTINGS.DATASET_DIR)
    variables = set([series.variable_id for series in series_list])
    methods = set([series.method_id for series in series_list])
    qc_ids = set([series.quality_control_level_id for series in series_list])
    site_ids = set([series.site_id for series in series_list])
    source_ids = set([series.source_id for series in series_list])

    if len(qc_ids) != 1 or len(site_ids) != 1 or len(source_ids) != 1 and len(series_list) > 0:
        print 'Cannot create a file that contains multiple QC, Site, or Source IDs'
        print '{}: {}'.format(varname(qc_ids), qc_ids)
        print '{}: {}'.format(varname(site_ids), site_ids)
        print '{}: {}\n'.format(varname(source_ids), source_ids)
        return file_list
    else:
        site = series_list[0].site                  # type: Site
        source = series_list[0].source              # type: Source
        qc = series_list[0].quality_control_level   # type: QualityControlLevel
        variables = list(variables)
        methods = list(methods)

        if len(variables) == 1:
            base_name += '{}_'.format(series_list[0].variable_code)
        base_name += 'at_{}_Source_{}_QC_Code_{}'.format(site.code, source.id, qc.code)
        if chunk_years:
            base_name += '_{}'
        base_name += '.csv'
    if len(variables) == 0 or len(methods) == 0:
        print 'Cannot generate series with no {}'.format(varname(variables if len(variables) == 0 else methods))

        return file_list

    print 'Starting to generate files'
    if chunk_years:
        years = GetSeriesYearRange(series_list)
        for year in years:
            name_with_year = base_name.format(year)
            dataframe = GetTimeSeriesDataframe(series_service, series_list, site.id, qc.id, source.id, methods, variables, year)
            if dataframe is None:
                print 'No data values exist for site {} and year {}'.format(site.code, year)
                failed_files.append((name_with_year, 'No data values found for file'))
                continue
            headers = BuildSeriesFileHeader(series_list, site, source)
            if WriteSeriesToFile(name_with_year, dataframe, headers):
                file_list.append(name_with_year)
            else:
                print 'Unable to write series to file {}'.format(name_with_year)
                failed_files.append((name_with_year, 'Unable to write series to file'))
    else:
        print 'Dataframe for {}'.format(variables)
        dataframe = GetTimeSeriesDataframe(series_service, series_list, site.id, qc.id, source.id, methods, variables)
        if dataframe is not None:
            headers = BuildSeriesFileHeader(series_list, site, source)
            if WriteSeriesToFile(base_name, dataframe, headers):
                file_list.append(base_name)
            else:
                print 'Unable to write series to file {}'.format(base_name)
                failed_files.append((base_name, 'Unable to write series to file'))
        else:
            print 'No data values exist for this dataset'
            failed_files.append((base_name, 'No data values found for file'))
    return file_list


def WriteSeriesToFile(csv_name, dataframe, headers):
    if dataframe is None:
        print('No dataframe is available to write to file {}'.format(csv_name))
        return False
    file_out = createFile(csv_name)
    if file_out is None:
        print('Unable to create output file {}'.format(csv_name))
        return False
    else:
        # Write data to CSV file
        print('Writing datasets to file: {}'.format(csv_name))
        file_out.write(headers)
        dataframe.to_csv(file_out)
        file_out.close()
    return True

def GetSeriesYearRange(series_list):
    start_date = None
    end_date = None
    for odm_series in series_list:
        if start_date is None or start_date > odm_series.begin_date_time:
            start_date = odm_series.begin_date_time
        if end_date is None or end_date < odm_series.end_date_time:
            end_date = odm_series.end_date_time
    return range(start_date.year, end_date.year + 1)


def BuildSeriesFileHeader(series_list, site, source):
    """

    :type series_service: SeriesService
    """
    header = ''

    if len(series_list) == 1:
        var_data = ExpandedVariableData(series_list[0].variable, series_list[0].method)
    else:
        var_data = CompactVariableData()
        for series in series_list:
            var_data.addData(series.variable, series.method)

    source_info = SourceInfo()
    source_info.setSourceInfo(source.organization, source.description, source.link, source.contact_name, source.phone,
                              source.email, source.citation)
    header += generateSiteInformation(site)
    header += var_data.printToFile() + '#\n'
    header += source_info.outputSourceInfo() + '#\n'
    return header


def generateSiteInformation(site):
    """

    :param site: Site for which to generate the header string
    :type site: Site
    :rtype: str
    """
    file_str = ""
    file_str += "# Site Information\n"
    file_str += "# ----------------------------------\n"
    file_str += "# SiteCode: " + str(site.code) + "\n"
    file_str += "# SiteName: " + str(site.name) + "\n"
    file_str += "# Latitude: " + str(site.latitude) + "\n"
    file_str += "# Longitude: " + str(site.longitude) + "\n"
    file_str += "# LatLonDatum: " + str(site.spatial_ref.srs_name) + "\n"
    file_str += "# Elevation_m: " + str(site.elevation_m) + "\n"
    file_str += "# ElevationDatum: " + str(site.vertical_datum) + "\n"
    file_str += "# State: " + str(site.state) + "\n"
    file_str += "# County: " + str(site.county) + "\n"
    file_str += "# Comments: " + str(site.comments) + "\n"
    file_str += "# SiteType: " + str(site.type) + "\n"
    file_str += "#\n"
    return file_str


class SourceInfo:
    def __init__(self, use_citation=True):
        self.organization = ""
        self.sourceDescription = ""
        self.sourceLink = ""
        self.contactName = ""
        self.phone = ""
        self.email = ""
        self.citation = ""
        self.use_citation = use_citation

    def setSourceInfo(self, org, srcDesc, srcLnk, cntctName, phn, email, citn):
        self.organization = org
        self.sourceDescription = srcDesc
        self.sourceLink = srcLnk
        self.contactName = cntctName
        self.phone = phn
        self.email = email
        self.citation = citn

    def outputSourceInfo(self):
        outputStr = "# Source Information\n# ------------------\n"
        outputStr += self.sourceOutHelper("Organization", self.organization)
        outputStr += self.sourceOutHelper("SourceDescription", self.sourceDescription)
        outputStr += self.sourceOutHelper("SourceLink", self.sourceLink)
        outputStr += self.sourceOutHelper("ContactName", self.contactName)
        outputStr += self.sourceOutHelper("Phone", self.phone)
        outputStr += self.sourceOutHelper("Email", self.email)
        if self.use_citation:
            outputStr += self.sourceOutHelper("Citation", self.citation)
        return outputStr

    def sourceOutHelper(self, title, value):
        if isinstance(title, unicode):
            title = title.encode('utf-8').strip()
        if isinstance(value, unicode):
            value = value.encode('utf-8').strip()
        return '# {}: {} \n'.format(title, value)


class ExpandedVariableData:
    def __init__(self, var, method):
        self.varCode = var.code
        self.varName = var.name
        self.valueType = var.value_type
        self.dataType = var.data_type
        self.gralCategory = var.general_category
        self.sampleMedium = var.sample_medium
        self.varUnitsName = var.variable_unit.name
        self.varUnitsType = var.variable_unit.type
        self.varUnitsAbbr = var.variable_unit.abbreviation
        self.noDV = int(var.no_data_value) if var.no_data_value.is_integer() else var.no_data_value
        self.timeSupport = var.time_support
        self.timeSupportUnitsAbbr = var.time_unit.abbreviation
        self.timeSupportUnitsName = var.time_unit.name
        self.timeSupportUnitsType = var.time_unit.type
        self.methodDescription = method.description
        self.methodLink = method.link if method.link is not None else "None"
        if not self.methodLink[-1:].isalnum():
            self.methodLink = self.methodLink[:-1]

    def printToFile(self):
        formatted = ""
        formatted += "# Variable and Method Information\n"
        formatted += "# ---------------------------\n"
        formatted += self.formatHelper("VariableCode", self.varCode)
        formatted += self.formatHelper("VariableName", self.varName)
        formatted += self.formatHelper("ValueType", self.valueType)
        formatted += self.formatHelper("DataType", self.dataType)
        formatted += self.formatHelper("GeneralCategory", self.gralCategory)
        formatted += self.formatHelper("SampleMedium", self.sampleMedium)
        formatted += self.formatHelper("VariableUnitsName", self.varUnitsName)
        formatted += self.formatHelper("VariableUnitsType", self.varUnitsType)
        formatted += self.formatHelper("VariableUnitsAbbreviation", self.varUnitsAbbr)
        formatted += self.formatHelper("NoDataValue", self.noDV)
        formatted += self.formatHelper("TimeSupport", self.timeSupport)
        formatted += self.formatHelper("TimeSupportUnitsAbbreviation", self.timeSupportUnitsAbbr)
        formatted += self.formatHelper("TimeSupportUnitsType", self.timeSupportUnitsType)
        formatted += self.formatHelper("TimeSupportUnitsName", self.timeSupportUnitsName)
        formatted += self.formatHelper("MethodDescription", self.methodDescription)
        formatted += self.formatHelper("MethodLink", self.methodLink)
        return formatted

    def formatHelper(self, title, var):
        if isinstance(title, unicode):
            title = title.encode('utf-8').strip()
        if isinstance(var, unicode):
            var = var.encode('utf-8').strip()
        return '# {}: {} \n'.format(title, var)


class CompactVariableData:
    def __init__(self):
        self.var_dict = {}
        self.method_dict = {}

    def addData(self, var, method):
        self.var_dict[var] = method

    def printToFile(self):
        # if not isinstance(vars_to_print, str) or len(vars_to_print) == 0:
        #     return ""
        formatted = ""
        formatted += "# Variable and Method Information\n"
        formatted += "# ---------------------------\n"
        for variable, method in self.var_dict.iteritems():
            if method.link is None:
                tempVarMethodLink = "None"
            else:
                tempVarMethodLink = method.link if method.link[-1:].isalnum() else method.link[-1:]

            formatted += "# "
            formatted += self.formatHelper("VariableCode", variable.code)
            formatted += self.formatHelper("VariableName", variable.name)
            formatted += self.formatHelper("ValueType", variable.value_type)
            formatted += self.formatHelper("DataType", variable.data_type)
            formatted += self.formatHelper("GeneralCategory", variable.general_category)
            formatted += self.formatHelper("SampleMedium", variable.sample_medium)
            formatted += self.formatHelper("VariableUnitsName", variable.variable_unit.name)
            formatted += self.formatHelper("VariableUnitsType", variable.variable_unit.type)
            formatted += self.formatHelper("VariableUnitsAbbreviation", variable.variable_unit.abbreviation)
            formatted += self.formatHelper("NoDataValue", variable.no_data_value)
            formatted += self.formatHelper("TimeSupport", variable.time_support)
            formatted += self.formatHelper("TimeSupportUnitsAbbreviation", variable.time_unit.abbreviation)
            formatted += self.formatHelper("TimeSupportUnitsName", variable.time_unit.name)
            formatted += self.formatHelper("TimeSupportUnitsType", variable.time_unit.type)
            formatted += self.formatHelper("MethodDescription", method.description)
            formatted += self.formatHelper("MethodLink", tempVarMethodLink)[:-2] + "\n"
        return formatted

    def formatHelper(self, title, var):
        if isinstance(title, unicode):
            title = title.encode('utf-8').strip()
        if isinstance(var, unicode):
            var = var.encode('utf-8').strip()
        return '{}: {} | '.format(title, var)
