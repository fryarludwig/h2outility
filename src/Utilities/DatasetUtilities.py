import datetime
from multiprocessing import Process, Queue
from time import sleep

import pandas

from Common import *
from GAMUTRawData.odmdata import QualityControlLevel, Series, Site, Source, Qualifier, Variable, Method
from GAMUTRawData.odmservices import SeriesService, ServiceManager

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
        self.resource = resource  # type: HydroShareResource
        self.selected_series = odm_series if odm_series is not None else {}  # type: dict[int, H2OSeries]
        self.hs_account_name = hs_account_name  # type: str
        self.odm_db_name = odm_db_name  # type: str
        self.single_file = single_file  # type: bool
        self.chunk_years = chunk_years  # type: bool
        self.associated_files = associated_files if associated_files is not None else []  # type: list[str]

    @property
    def public(self):
        if self.resource and hasattr(self.resource, 'public'):
            return getattr(self.resource, 'public')
        return False

    @property
    def subjects(self):
        if self.resource:
            if hasattr(self.resource, 'subjects'):
                return getattr(self.resource, 'subjects')
            elif hasattr(self.resource, 'keywords'):
                return getattr(self.resource, 'keywords')
        return []

    @property
    def keywords(self):
        return self.subjects

    def __dict__(self):
        return {'resource': self.resource, 'selected_series': self.selected_series,
                'hs_account_name': self.hs_account_name, 'resource_id': self.resource_id,
                'single_file': self.single_file, 'chunk_years': self.chunk_years,
                'odm_db_name': self.odm_db_name, 'associated_files': self.associated_files}

    def to_dict(self):
        return self.__dict__()

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
            print(exc)

        if process.is_alive():
            process.terminate()
            process.join()
        return result

    def ToDict(self):
        return {'engine': self.engine, 'user': self.user, 'password': self.password, 'address': self.address,
                'db': self.database, 'port': self.port}


DELIMITER = '# {}'.format('-' * 90)


def createFile(filepath):
    try:
        print('Creating new file {}'.format(filepath))
        return open(filepath, 'w')
    except Exception as e:
        print('---\nIssue encountered while creating a new file: \n{}\n{}\n---'.format(e, e.message))
        return None


def GetTimeSeriesDataframe(series_service, series_list, site_id, qc_id, source_id, methods, variables, starting_date,
                           year=None):
    q_list = []
    censor_list = []

    if APP_SETTINGS.SKIP_QUERIES:
        dataframe = None
    else:
        dataframe = series_service.get_values_by_filters(site_id, qc_id, source_id, methods, variables, year,
                                                         starting_date=starting_date,
                                                         chunk_size=APP_SETTINGS.QUERY_CHUNK_SIZE,
                                                         timeout=APP_SETTINGS.DATAVALUES_TIMEOUT)

    variables_len = len(variables)
    methods_len = len(methods)

    if variables_len < methods_len:
        columns = ['MethodID', 'VariableCode']
    else:
        columns = 'VariableCode'

    if qc_id == 0 or len(variables) != 1 or len(methods) != 1:
        csv_table = pandas.pivot_table(dataframe,
                                       index=["LocalDateTime", "UTCOffset", "DateTimeUTC"],
                                       columns=columns,
                                       values='DataValue',
                                       fill_value=series_list[0].variable.no_data_value)
        del dataframe

    else:
        method = next(iter(methods))
        variable = next(iter(variables))

        q_list = [[q.id, q.code, q.description] for q in
                  series_service.get_qualifiers_by_series_details(site_id, qc_id, source_id, method, variable)]

        # Get the qualifiers that we use in this series, merge it with our DataValue set
        q_df = pandas.DataFrame(data=q_list, columns=["QualifierID", "QualifierCode", "QualifierDescription"])

        csv_table = dataframe.merge(q_df, how='left', on="QualifierID")  # type: pandas.DataFrame

        del dataframe

        csv_table.set_index(["LocalDateTime", "UTCOffset", "DateTimeUTC"], inplace=True)
        for column in csv_table.columns.tolist():

            if column not in ["DataValue", "CensorCode", "QualifierCode", 'VariableCode']:
                csv_table.drop(column, axis=1, inplace=True)

        csv_table.rename(columns={"DataValue": series_list[0].variable.code}, inplace=True)

        if 'CensorCode' in csv_table:
            censor_list = set(csv_table['CensorCode'].tolist())

    return csv_table, q_list, censor_list  # don't ask questions... just let it happen


def BuildCsvFile(series_service, series_list, year=None, failed_files=list()):  # type: (SeriesService, list[Series], int, list[tuple(str)]) -> str | None
    try:
        if len(series_list) == 0:
            print('Cannot generate a file for no series')
            return None
        variables = set([series.variable_id for series in series_list if series is not None])
        methods = set([series.method_id for series in series_list if series is not None])
        qc_ids = set([series.quality_control_level_id for series in series_list if series is not None])
        site_ids = set([series.site_id for series in series_list if series is not None])
        source_ids = set([series.source_id for series in series_list if series is not None])

        if len(qc_ids) == 0 or len(site_ids) == 0 or len(source_ids) == 0:
            print('Series provided are empty or invalid')
        elif len(qc_ids) > 1 or len(site_ids) > 1 or len(source_ids) > 1:
            print('Cannot create a file that contains multiple QC, Site, or Source IDs')
            print('{}: {}'.format(varname(qc_ids), qc_ids))
            print('{}: {}'.format(varname(site_ids), site_ids))
            print('{}: {}\n'.format(varname(source_ids), source_ids))
        elif len(variables) == 0 or len(methods) == 0:
            print('Cannot generate series with no {}'.format(varname(variables if len(variables) == 0 else methods)))
        else:
            try:
                site = series_list[0].site  # type: Site
            except Exception:
                site = Site(site_code=series_list[0].site_code, site_name=series_list[0].site_name)



            source = series_list[0].source  # type: Source
            qc = series_list[0].quality_control_level  # type: QualityControlLevel
            variables = list(variables)
            methods = list(methods)

            base_name = os.path.join(APP_SETTINGS.DATASET_DIR, '%s_' % site.code)
            if len(variables) == 1:
                base_name += '{}_'.format(series_list[0].variable_code)
            base_name += 'QC_{}_Source_{}'.format(qc.code, source.id)
            if year is not None:
                base_name += '_{}'.format(year)
            file_name = base_name + '.csv'

            # if os.path.exists(file_name):
            #     csv_data = parseCSVData(file_name)
            #     csv_end_datetime = csv_data.localDateTime
            # else:
            #     csv_end_datetime = None
            csv_end_datetime = None

            stopwatch_timer = None
            if APP_SETTINGS.VERBOSE:
                stopwatch_timer = datetime.datetime.now()
                print('Querying values for file {}'.format(file_name))

            dataframe, qualifier_codes, censorcodes = GetTimeSeriesDataframe(series_service, series_list, site.id, qc.id, source.id, methods, variables, csv_end_datetime, year)

            if APP_SETTINGS.VERBOSE:
                print('Query execution took {}'.format(datetime.datetime.now() - stopwatch_timer))

            if dataframe is not None:
                if csv_end_datetime is None:
                    dataframe.sort_index(inplace=True)
                    headers = BuildSeriesFileHeader(series_list, site, source, qualifier_codes, censorcodes)
                    if WriteSeriesToFile(file_name, dataframe, headers):
                        return file_name
                    else:
                        print('Unable to write series to file {}'.format(file_name))
                        failed_files.append((file_name, 'Unable to write series to file'))
                else:
                    if AppendSeriesToFile(file_name, dataframe):
                        return file_name
                    else:
                        print('Unable to append series to file {}'.format(file_name))
                        failed_files.append((file_name, 'Unable to append series to file'))
            elif APP_SETTINGS.SKIP_QUERIES:
                headers = BuildSeriesFileHeader(series_list, site, source, qualifier_codes, censorcodes)
                if WriteSeriesToFile(file_name, dataframe, headers):
                    return file_name
            elif dataframe is None and csv_end_datetime is not None:
                print('File exists but there are no new data values to write')
                # return file_name
            else:
                print('No data values exist for this dataset')
                failed_files.append((file_name, 'No data values found for file'))
    except TypeError as e:
        print('Exception encountered while building a csv file: {}'.format(e))
    return None


def AppendSeriesToFile(csv_name, dataframe):
    if dataframe is None and not APP_SETTINGS.SKIP_QUERIES:
        print('No dataframe is available to write to file {}'.format(csv_name))
        return False
    elif dataframe is None and APP_SETTINGS.SKIP_QUERIES:
        print('Writing test datasets to file: {}'.format(csv_name))
        return True
    try:
        file_out = open(csv_name, 'a')
        print('Writing datasets to file: {}'.format(csv_name))
        dataframe.to_csv(file_out, header=None)
        file_out.close()
    except Exception as e:
        print('---\nIssue encountered while appending to file: \n{}\n{}\n---'.format(e, e.message))
        return False
    return True


def WriteSeriesToFile(csv_name, dataframe, headers):
    if dataframe is None and not APP_SETTINGS.SKIP_QUERIES:
        print('No dataframe is available to write to file {}'.format(csv_name))
        return False
    elif dataframe is None and APP_SETTINGS.SKIP_QUERIES:
        print('Writing test datasets to file: {}'.format(csv_name))
        return True
    file_out = createFile(csv_name)
    if file_out is None:
        print('Unable to create output file {}'.format(csv_name))
        return False
    else:
        # Write data to CSV file
        print('Writing datasets to file: {}'.format(csv_name))
        file_out.write(headers)
        import csv
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


def BuildSeriesFileHeader(series_list, site, source, qualifier_codes=[], censorcodes=set()):
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
    if len(censorcodes):
        header += generateCensorCodes()
    header += generateQualifierCodes(qualifier_codes) + '#\n'

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


def generateCensorCodes():

    return "# Censor Codes\n" + \
           "# ----------------------------------\n" + \
           "# nc: not censored\n" + \
           "#\n"


def generateQualifierCodes(codes):  # type: ([(int, str, str)]) -> str

    if not len(codes):
        return ""

    header = '# Qualifier Codes\n# ----------------------------------\n'

    for code in codes:
        _, abrv, definition = code
        header += '# %s: %s\n' % (abrv, definition)

    return header + '#\n'


def parseCSVData(filePath):
    csvFile = open(filePath, "r")
    lastLine = getLastLine(csvFile)
    csvFile.close()
    return getDateAndNumCols(lastLine)


def getLastLine(targetFile):
    firstCharSeek = ''
    readPosition = -3
    prevLine = result = ""
    while firstCharSeek != '\n':
        targetFile.seek(readPosition, os.SEEK_END)
        readPosition -= 1
        result = prevLine #last line was being deleted. So I saved a temp to keep it
        prevLine = targetFile.readline()
        firstCharSeek = prevLine[0]
    return result


def getDateAndNumCols(lastLine):
    strList = lastLine.split(",")
    dateTime = datetime.datetime.strptime(strList[0], '%Y-%m-%d %H:%M:%S')
    count = 0
    for value in strList:
        isValueCorrect = strList.index(value) > 2 and value != " \n"
        if isValueCorrect:
            count += 1
    return ReturnValue(dateTime, count)


class ReturnValue:
    def __init__(self, dateTime, noOfVars):
        self.localDateTime = dateTime
        self.numCols = noOfVars


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


class VariableFormatter(object):
    """
    Abstract class - basically here to make it clear inherited classes
    need to implement the methods in this class.
    """
    def __init__(self):
        pass

    def formatHelper(self, label, value):
        raise NotImplemented

    def printToFile(self):
        raise NotImplemented


class ExpandedVariableData(VariableFormatter):
    def __init__(self, var, method):
        super(ExpandedVariableData, self).__init__()
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

            if ',' in var:
                return '"# {}: {}"\n'.format(title, var)

        return '# {}: {} \n'.format(title, var)


class CompactVariableData(VariableFormatter):
    def __init__(self):
        super(CompactVariableData, self).__init__()
        self.var_dict = {}
        self.method_dict = {}

    def addData(self, var, method):
        self.var_dict[var] = method

    def printToFile(self):
        # if not isinstance(vars_to_print, str) or len(vars_to_print) == 0:
        #     return ""
        header = "# Variable and Method Information\n"
        header += "# ---------------------------\n"
        # formatted = ""
        # formatted += "# Variable and Method Information\n"
        rows = []
        for variable, method in self.var_dict.iteritems():  # type: (Variable, Method)

            definitions = []

            if method.link is None:
                tempVarMethodLink = "None"
            else:
                tempVarMethodLink = method.link if method.link[-1:].isalnum() else method.link[-1:]

            definitions.append(self.formatHelper("VariableCode", variable.code))
            definitions.append(self.formatHelper("VariableName", variable.name))
            definitions.append(self.formatHelper("ValueType", variable.value_type))
            definitions.append(self.formatHelper("DataType", variable.data_type))
            definitions.append(self.formatHelper("GeneralCategory", variable.general_category))
            definitions.append(self.formatHelper("SampleMedium", variable.sample_medium))
            definitions.append(self.formatHelper("VariableUnitsName", variable.variable_unit.name))
            definitions.append(self.formatHelper("VariableUnitsType", variable.variable_unit.type))
            definitions.append(self.formatHelper("VariableUnitsAbbreviation", variable.variable_unit.abbreviation))
            definitions.append(self.formatHelper("NoDataValue", variable.no_data_value))
            definitions.append(self.formatHelper("TimeSupport", variable.time_support))
            definitions.append(self.formatHelper("TimeSupportUnitsAbbreviation", variable.time_unit.abbreviation))
            definitions.append(self.formatHelper("TimeSupportUnitsName", variable.time_unit.name))
            definitions.append(self.formatHelper("TimeSupportUnitsType", variable.time_unit.type))
            definitions.append(self.formatHelper("MethodDescription", method.description))
            definitions.append(self.formatHelper("MethodLink", tempVarMethodLink)[:-2])

            rows.append(definitions)

        definitions = "\n".join(['"# %s"' % ' | '.join(row) for row in rows])

        return '%s%s' % (header, definitions)

    def formatHelper(self, title, var):
        if isinstance(title, unicode):
            title = title.encode('utf-8').strip()
        if isinstance(var, unicode):
            var = var.encode('utf-8').strip()
        return '{0}: {1}'.format(title, var)
