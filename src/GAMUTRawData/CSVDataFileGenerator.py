import sys
import os
import logging
import datetime
from time import sleep

import pandas
import pyodbc
import jsonpickle
from multiprocessing import Process, Queue
from sqlalchemy.exc import InvalidRequestError

from odmdata import Series
from odmdata import Site
from odmdata import SpatialReference
from odmdata import Qualifier
from odmdata import DataValue
from odmservices import ServiceManager

this_file = os.path.realpath(__file__)
directory = os.path.dirname(os.path.dirname(this_file))

sys.path.insert(0, directory)

time_format = '%Y-%m-%d'
formatString = '%s  %s: %s'
service_manager = ServiceManager()
# UPDATE_CACHE = True
UPDATE_CACHE = False

issues = []

QC1_RESOURCE_ABSTRACT = 'This dataset contains quality control level 1 (QC1) data for all of the variables ' \
                        'measured for the iUTAH GAMUT Network {site_name} ({site_code}). Each file contains all ' \
                        'available QC1 data for a specific variable. Files will be updated as new data become ' \
                        'available, but no more than once daily. These data have passed QA/QC procedures such as ' \
                        'sensor calibration and visual inspection and removal of obvious errors. These data are ' \
                        'approved by Technicians as the best available version of the data. See published script ' \
                        'for correction steps specific to this data series. Each file header contains detailed ' \
                        'metadata for site information, variable and method information, source information, and ' \
                        'qualifiers referenced in the data.'

RAW_RESOURCE_ABSTRACT = 'This dataset contains raw data for all of the variables ' \
                        'measured for the iUTAH GAMUT Network {site_name} ({site_code}). Each file contains a ' \
                        'calendar year of data. The file for the current year is updated on a daily basis. ' \
                        'The data values were collected by a variety of sensors at 15 minute intervals. ' \
                        'The file header contains detailed metadata for site and the variable and method ' \
                        'of each column.'

contributors = [
    {"contributor": {"name": "Zach Aanderud", "organization": "Brigham Young University"}},
    {"contributor": {"name": "Michelle Baker", "organization": "Utah State University"}},
    {"contributor": {"name": "Dave Bowling", "organization": "University of Utah"}},
    {"contributor": {"name": "Jobie Carlile", "organization": "Utah State University"}},
    {"contributor": {"name": "Chris Cox", "organization": "Utah State University"}},
    {"contributor": {"name": "Joe Crawford", "organization": "Brigham Young University"}},
    {"contributor": {"name": "Dylan Dastrup", "organization": "Brigham Young University"}},
    {"contributor": {"name": "Jim Ehleringer", "organization": "University of Utah"}},
    {"contributor": {"name": "Dave Eiriksson", "organization": "University of Utah"}},
    {"contributor": {"name": "Jeffery S. Horsburgh", "organization": "Utah State University", "address": "Utah US",
                     "phone": "(435) 797-2946"}},
    {"contributor": {"name": "Amber Spackman Jones", "organization": "Utah State University"}},
    {"contributor": {"name": "Scott Jones", "organization": "Utah State University"}}]


def _OdmDatabaseConnectionTestTimed(queue):
    db_auth = queue.get(True)
    if service_manager.test_connection(db_auth):
        queue.put(True)
    else:
        queue.put(False)

class OdmDatabaseDetails:
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

class GenericResourceDetails:
    def __init__(self):
        self.resource_name = ''
        self.abstract = ''
        self.keywords = []
        self.creators = []
        self.metadata = []
        self.temporal_start = None
        self.temporal_end = None
        self.coord_units = 'Decimal Degrees'
        self.geo_projection = None
        self.lat = None
        self.lon = None

        self.credits = None

    def getMetadata(self):
        # return self.metadata.encode('utf-8').replace('\'', '"')
        return str(self.metadata).replace('\'', '"')


def getNewQC1ResourceInformation(site_code, valid_files=None):
    """

    :param site_code: The site code, used to get site details from the iutahdbs server
    :type site_code: str
    :param valid_files: File Details for the files we will be uploading to the resource
    :type valid_files: list of FileDetails
    :return:
    :rtype:
    """
    db_code = site_code.split('_')[0]
    service_manager._current_connection = {'engine': 'mssql', 'user': 'webapplication', 'password': 'W3bAppl1c4t10n!',
                                           'address': 'iutahdbs.uwrl.usu.edu', 'db': DB_CODE_LOOKUP[db_code]}
    series_service = service_manager.get_series_service()
    site = series_service.get_site_by_code(site_code)  # type: Site
    new_resource = GenericResourceDetails()
    new_resource.resource_name = "iUTAH GAMUT Network Quality Control Level 1 Data at " \
                                 "{site_name} ({site_code})".format(site_name=site.name, site_code=site.code)
    new_resource.abstract = QC1_RESOURCE_ABSTRACT.format(site_name=site.name, site_code=site.code)
    new_resource.keywords = [site.name, site.type, 'time series', 'iUTAH', 'GAMUT', 'Quality Controlled Level 1']
    if valid_files is not None and len(valid_files) > 0:
        variables = set([v.variable_names.replace(',', ' -') for v in valid_files if len(v.variable_names) > 0])
        new_resource.keywords.extend(list(variables))
        coverage_start_list = [v.coverage_start for v in valid_files if len(v.variable_names) > 0]
        coverage_end_list = [v.coverage_end for v in valid_files if len(v.variable_names) > 0]
        start_cov = min(coverage_start_list) if len(coverage_start_list) > 0 else None
        end_cov = max(coverage_end_list) if len(coverage_end_list) > 0 else None
        if start_cov is not None and end_cov is not None:
            temporal_data = {"coverage":
                             {"type": "period",
                              "value": {"start": start_cov.strftime(time_format),
                                        "end": end_cov.strftime(time_format)}}}
            new_resource.metadata.append(temporal_data)

    # Add Credits
    credit_dict = {'fundingagency': {'agency_name': 'National Science Foundation',
                                     'award_title': 'iUTAH-innovative Urban Transitions and Aridregion '
                                                    'Hydro-sustainability',
                                     'award_number': '1208732',
                                     'agency_url': 'http://www.nsf.gov'}}
    new_resource.metadata.append(credit_dict)

    authors = {"creator": {"organization": 'iUTAH GAMUT Working Group'}}
    new_resource.metadata.append(authors)

    spatial_coverage = dict(coverage={'type': 'point',
                                      'value': {
                                          'east': '{}'.format(site.longitude),
                                          'north': '{}'.format(site.latitude),
                                          'units': 'Decimal degrees',
                                          'name': '{}'.format(site.name),
                                          'elevation': '{}'.format(site.elevation_m),
                                          'projection': '{}'.format(site.spatial_ref.srs_name)
                                      }})
    new_resource.metadata.append(spatial_coverage)

    for contrib in contributors:
        new_resource.metadata.append(contrib)

    return new_resource


def getNewRawDataResourceInformation(site_code, valid_files=None):
    """

    :param site_code: The site code, used to get site details from the iutahdbs server
    :type site_code: str
    :param valid_files: File Details for the files we will be uploading to the resource
    :type valid_files: list of FileDetails
    :return:
    :rtype:
    """
    db_code = site_code.split('_')[0]
    service_manager._current_connection = {'engine': 'mssql', 'user': 'webapplication', 'password': 'W3bAppl1c4t10n!',
                                           'address': 'iutahdbs.uwrl.usu.edu', 'db': DB_CODE_LOOKUP[db_code]}
    series_service = service_manager.get_series_service()
    site = series_service.get_site_by_code(site_code)  # type: Site
    new_resource = GenericResourceDetails()
    new_resource.resource_name = "iUTAH GAMUT Network Raw Data at {site_name} ({site_code})".format(site_name=site.name,
                                                                                                    site_code=site.code)
    new_resource.abstract = RAW_RESOURCE_ABSTRACT.format(site_name=site.name, site_code=site.code)
    new_resource.keywords = [site.name, site.type, 'time series', 'iUTAH', 'GAMUT', 'raw data']
    if valid_files is not None and len(valid_files) > 0:
        variables = set([v.variable_names.replace(',', ' -') for v in valid_files if len(v.variable_names) > 0])
        new_resource.keywords.extend(list(variables))
        coverage_start_list = [v.coverage_start for v in valid_files if len(v.variable_names) > 0]
        coverage_end_list = [v.coverage_end for v in valid_files if len(v.variable_names) > 0]
        start_cov = min(coverage_start_list) if len(coverage_start_list) > 0 else None
        end_cov = max(coverage_end_list) if len(coverage_end_list) > 0 else None
        if start_cov is not None and end_cov is not None:
            temporal_data = {"coverage":
                                 {"type": "period",
                                  "value": {"start": start_cov.strftime(time_format),
                                            "end": end_cov.strftime(time_format)}}}
            new_resource.metadata.append(temporal_data)

    # Add Credits
    credit_dict = {'fundingagency': {'agency_name': 'National Science Foundation',
                                     'award_title': 'iUTAH-innovative Urban Transitions and Aridregion '
                                                    'Hydro-sustainability',
                                     'award_number': '1208732',
                                     'agency_url': 'http://www.nsf.gov'}}
    new_resource.metadata.append(credit_dict)

    authors = {"creator": {"organization": 'iUTAH GAMUT Working Group'}}
    # authors = {"creator": {"name": 'iUTAH GAMUT Working Group', 'organization': 'iUtah'}}
    new_resource.metadata.append(authors)

    spatial_coverage = dict(coverage={'type': 'point',
                                      'value': {
                                          'east': '{}'.format(site.longitude),
                                          'north': '{}'.format(site.latitude),
                                          'units': 'Decimal degrees',
                                          'name': '{}'.format(site.name),
                                          'elevation': '{}'.format(site.elevation_m),
                                          'projection': '{}'.format(site.spatial_ref.srs_name)
                                      }})
    new_resource.metadata.append(spatial_coverage)

    for contrib in contributors:
        new_resource.metadata.append(contrib)

    return new_resource


class FileDetails(object):
    def __init__(self, site_code=None, site_name=None, file_path=None, file_name=None, variable_names=None):
        self.coverage_start = None
        self.coverage_end = None
        self.file_path = "" if file_path is None else file_path
        self.file_name = "" if file_name is None else file_name
        self.site_code = "" if site_code is None else site_code
        self.site_name = "" if site_name is None else site_name
        self.variable_names = [] if variable_names is None else variable_names
        self.is_empty = True
        self.created = False

    def __str__(self):
        fd_str = '{site} - {s_name} - {f_name}'
        return fd_str.format(site=self.site_code, s_name=self.site_name, f_name=self.file_name)


def dataParser(dump_loc, data_type, year):
    """

    :param dump_loc:
    :type dump_loc: str
    :param data_type:
    :type data_type: str
    :param year:
    :type year: str
    :return:
    :rtype: dict of list of FileDetails
    """
    all_files = {}
    stored_cache = {}
    updated_files = {}
    if data_type.lower() == 'raw':
        cache_file_name = dump_loc + 'cache_raw.json'
    else:
        cache_file_name = dump_loc + 'cache_qc1.json'
    try:
        json_in = open(cache_file_name, 'r')
        stored_cache = jsonpickle.decode(json_in.read())
        json_in.close()
        if not UPDATE_CACHE:
            return stored_cache
    except IOError as e:
        print 'Error reading cached file data - Clearing files and recreating cache.\n{}'.format(e)

    print("\n========================================================\n")
    all_files.update(handleDatabaseConnection('iUTAH_Logan_OD', 'Logan', dump_loc, year, data_type, stored_cache))
    all_files.update(handleDatabaseConnection('iUTAH_Provo_OD', 'Provo', dump_loc, year, data_type, stored_cache))
    all_files.update(handleDatabaseConnection('iUTAH_RedButte_OD', 'RedButte', dump_loc, year, data_type, stored_cache))
    print("\n========================================================\n")

    try:
        json_out = open(cache_file_name, 'w')
        json_out.write(jsonpickle.encode(all_files))
        json_out.close()
    except IOError as e:
        print 'Error saving cache to disk - cache will not be used the next time this program runs\n{}'.format(e)

    for site_code in all_files.keys():
        site_files_changed = [f for f in all_files[site_code] if f.created or f.is_empty]
        if len(site_files_changed) > 0:
            updated_files[site_code] = site_files_changed
    return updated_files


def cachedVersionIsOutdated(cached_file, new_file):
    """

    :param cached_file: File cached on local disk
    :type cached_file: FileDetails
    :param new_file: Current file being created from database
    :type new_file: FileDetails
    :return: Returns true if a new file should be generated
    :rtype: bool
    """
    if cached_file is None or cached_file.coverage_start is None or cached_file.coverage_end is None:
        return True
    elif new_file.coverage_start is not None and new_file.coverage_start < cached_file.coverage_start:
        return True
    elif new_file.coverage_end is not None and new_file.coverage_end > cached_file.coverage_end:
        return True
    else:
        return False


def handleDatabaseConnection(database, location, dump_location, year, data_type, file_cache):
    """

    :param data_type:
    :type data_type: str
    :param database: Database schema to select ('iUTAH_Logan_OD', 'iUTAH_Provo_OD', 'iUTAH_RedButte_OD')
    :type database: str
    :param location: GAMUT data network ('Logan', 'Provo', or 'RedButte')
    :type location: str
    :param dump_location: Path to folder used to store CSV files
    :type dump_location: str
    :param year: Year to constrain data to
    :type year: str
    :return: Issues encountered during file generation process
    :rtype: dict of list of FileDetails
    """
    print('Started getting sites from {} at {}'.format(database, location))
    # Getting the data
    service_manager._current_connection = {'engine': 'mssql', 'user': 'webapplication', 'password': 'W3bAppl1c4t10n!',
                                           'address': 'iutahdbs.uwrl.usu.edu', 'db': database}
    series_service = service_manager.get_series_service()
    all_sites = series_service.get_all_sites()
    site_files = {}
    for site in all_sites:
        if data_type.lower() == 'raw':
            local_dataset = RawDataCsvLocalDataset(dump_location, location, site, year, file_cache)
            series = series_service.get_series_by_site_code_year(site.code, year)
            site_files[site.code] = local_dataset.writeToFile(series_service, series)
        else:
            local_dataset = QC1_CsvLocalDataset(dump_location, location, site, year, file_cache)
            series = series_service.get_series_by_site_and_qc_level(site.code, 1)
            site_files[site.code] = local_dataset.writeToFile(series_service, series)
    return site_files


def outputValues(ss, dvObjects, site, header_str, dump_location):
    timeIndexes = ss.get_all_local_date_times_by_siteid(site.id)
    currentYear = 1900
    # gotta optimize this for loop somehow.

    if len(timeIndexes) > 0:
        for time in timeIndexes:
            outputStr = ""
            if time.local_date_time.year != currentYear:
                if currentYear != 1900:
                    file_name = "iUTAH_GAMUT_{site}_RawData_{yr}.csv".format(site=site.code, yr=currentYear)
                    text_file.close()
                    print "{} Finished creating {}".format(datetime.datetime.now, file_name)
                currentYear = time.local_date_time.year
                text_file = open(dump_location + file_name, "w")
                text_file.write(header_str)

            outputStr += str(time[0]) + ", " + str(time[1]) + ", " + str(time[2]) + ", "
            counter = 0

            for var in dvObjects.varCode:
                var_print = next((dv for dv in dvObjects.dataValues[counter] if dv.local_date_time == time[0]), None)
                if var_print != None:
                    outputStr += str(var_print.data_value) + ", "
                    dvObjects.dataValues[counter].remove(var_print)
                else:
                    outputStr += ", "

                counter += 1

            ouputStr = outputStr[:-2]
            outputStr += "\n"

            text_file.write(outputStr)

        text_file.close()


class QC1_CsvLocalDataset:
    def __init__(self, dump_location, location, site, year, file_cache=None):
        # type: (str, str, str, str, dict) -> QC1_CsvLocalDataset
        self.site = site
        self.network = location
        self.csv_name = 'iUTAH_GAMUT_{site}_Quality_Control_Level_1_{var}.csv'
        self.csv_site_dir = dump_location + '{site}/'
        self.file_cache = file_cache

        self.year = year
        self.start_date = '{y}-01-01 00:00:00'.format(y=year)
        self.end_date = datetime.datetime(int(year), 12, 31, 23, 55, 59)
        self.column_count = 0

        self.csv_indexes = ["LocalDateTime", "UTCOffset", "DateTimeUTC"]
        self.qualifier_columns = ["QualifierID", "QualifierCode", "QualifierDescription"]
        self.csv_columns = ["DataValue", "CensorCode", "QualifierCode"]

        self.exception_msg = " SiteName: {site}, year: {year}, Error : {error}"

    def createFile(self, filepath):
        try:
            file_out = open(filepath, 'w')
            return file_out
        except Exception as e:
            print('---\nIssue encountered while creating a new file: \n{}\n{}\n---'.format(e, e.message))
            return None

    def writeToFile(self, series_service, series_list):
        """

        :param series_service: The database connection used to retrieve additional data
        :type series_service: SeriesService
        :param series_list: Series for which we attempt to make a CSV file
        :type series_list: list of Series
        :return: Issues encountered during file generation process
        :rtype: list of FileDetails
        """
        site_files = []
        try:
            # Ensure the filesystem path exists
            site_path = self.csv_site_dir.format(site=self.site.code)
            if not os.path.exists(site_path):
                os.makedirs(site_path)

            for series in series_list:
                file_name = self.csv_name.format(site=series.site_code, var=series.variable_code)
                file_info = FileDetails(site_code=series.site_code, site_name=series.site_name, file_name=file_name,
                                        file_path=site_path + file_name, variable_names=series.variable_name)

                # Get all of the DataValues for our series
                dv_raw = series_service.get_variables_by_site_id_qc(series.variable_id, series.site_id, 1)  # type:
                if not len(dv_raw) > 0:
                    print("No data value sets found for {}, {}".format(series.site_code, self.site.name))
                    continue

                # Store the coverage data so we can use it later
                file_info.coverage_start = dv_raw["LocalDateTime"].iloc[0]
                file_info.coverage_end = dv_raw["LocalDateTime"].iloc[-1]

                recreate_file = True
                cached_file = None
                if series.site_code in self.file_cache:
                    for cached in self.file_cache[series.site_code]:
                        if cached.file_path == file_info.file_path:
                            cached_file = cached
                            break
                    recreate_file = cachedVersionIsOutdated(cached_file, file_info)

                if recreate_file or True:
                    site_files.append(file_info)
                    print 'Recreating file {}'.format(file_info.file_name)
                else:
                    cached_file.created = False
                    site_files.append(cached_file)
                    print 'We are using the cached version: {}'.format(cached_file.file_name)
                    continue

                file_out = self.createFile(file_info.file_path)
                if file_out is None:
                    print('Unable to create output file for {}, {}'.format(series.site_code, series.variable_code))
                    continue
                file_info.created = True

                # Get the qualifiers that we use in this series, merge it with our DataValue set
                q_list = [[q.id, q.code, q.description] for q in series_service.get_qualifiers_by_series_id(series.id)]
                q_df = pandas.DataFrame(data=q_list, columns=self.qualifier_columns)
                dv_set = dv_raw.merge(q_df, how='left', on="QualifierID")  # type: pandas.DataFrame
                del dv_raw
                dv_set.set_index(self.csv_indexes, inplace=True)

                # Drop the columns that we aren't interested in, and correct any names afterwards
                for column in dv_set.columns.tolist():
                    if column not in self.csv_columns:
                        dv_set.drop(column, axis=1, inplace=True)
                dv_set.rename(columns={"DataValue": series.variable_code}, inplace=True)

                # Getting and organizing all the data
                var_data = ExpandedVariableData(series.variable, series.method)
                sourceInfo = SourceInfo()
                sourceInfo.setSourceInfo(series.source.organization, series.source.description, series.source.link,
                                         series.source.contact_name, series.source.phone, series.source.email,
                                         series.source.citation)

                # Write the header and data to the file
                file_str = self.generateHeader()
                file_str += generateSiteInformation(self.site, self.network)
                file_str += var_data.printToFile() + "#\n"
                file_str += sourceInfo.outputSourceInfo() + "#\n"
                file_str += self.generateQualifierHeader(q_list) + "#\n"
                file_out.write(file_str)
                dv_set.to_csv(file_out)
                file_out.close()
                file_info.is_empty = False
                print ('{} handleConnection: Success - created {}'.format(datetime.datetime.now(), file_info.file_name))
        except KeyError as e:
            print('---\nIssue encountered while formatting data:\nType: {}, Value: {}\n---'.format(type(e), e.message))
            print(self.exception_msg.format(site=self.site, year=self.year, error=e))
        except IOError as e:
            print('---\nIssue encountered during file operations:\nType: {}, Value: {}\n---'.format(type(e), e.message))
            print(self.exception_msg.format(site=self.site, year=self.year, error=e))
        except Exception as e:
            print('---\nUnexpected issue while gathering data:\nType: {}, Value: {}\n---'.format(type(e), e.message))
            print(self.exception_msg.format(site=self.site, year=self.year, error=e))

        return site_files


class RawDataCsvLocalDataset:
    def __init__(self, dump_location, location, site, year, file_cache=None):
        self.site = site
        self.network = location
        self.csv_filename = 'iUTAH_GAMUT_{site}_RawData_{yr}.csv'.format(site=site.code, yr=year)
        self.csv_filepath = "{path}{name}".format(path=dump_location, name=self.csv_filename)
        self.file_cache = file_cache

        self.year = year
        self.start_date = '{y}-01-01 00:00:00'.format(y=year)
        self.end_date = datetime.datetime(int(year), 12, 31, 23, 55, 59)
        self.column_count = 0

    def createFile(self, filepath):
        """

        :param file_path:
        :type file_path:
        :return:
        :rtype:
        """
        try:
            print formatString % (datetime.datetime.now(), "handleConnection", "Creating a new file " + filepath)
            file_out = open(filepath, 'w')
            return file_out
        except Exception as e:
            print('---\nIssue encountered while creating a new file: \n{}\n{}\n---'.format(e, e.message))
            return None

    def writeToFile(self, series_service, series_list, filepath=None):
        print('Processing site {} with {} series items'.format(self.site.code, len(series_list)))
        if filepath is None:
            filepath = self.csv_filepath
        site_files = []
        file_name = self.csv_filename.format(site=self.site.code, yr=self.year)
        file_info = FileDetails(site_code=self.site.code, site_name=self.site.name, file_path=filepath,
                                file_name=file_name)

        cached_file = None
        if self.site.code in self.file_cache:
            for cached in self.file_cache[self.site.code]:
                if cached.file_path == file_info.file_path:
                    cached_file = cached
                    break

        dvs = series_service.get_all_values_by_site_id_date(self.site.id, self.start_date, self.end_date)
        try:
            if len(dvs) > 0:
                file_info.coverage_start = dvs["LocalDateTime"].iloc[0]
                file_info.coverage_end = dvs["LocalDateTime"].iloc[-1]

                if cachedVersionIsOutdated(cached_file, file_info):
                    site_files.append(file_info)
                    print 'Recreating file {}'.format(file_info.file_name)
                else:
                    cached_file.created = False
                    site_files.append(cached_file)
                    print 'We are using the cached version: {}'.format(cached_file.file_name)
                    return site_files

                file_out = self.createFile(file_info.file_path)
                if file_out is None:
                    print('Unable to create output file for {}'.format(file_info.site_code))
                    return site_files
                file_info.created = True

                df = pandas.pivot_table(dvs, index=["LocalDateTime", "UTCOffset", "DateTimeUTC"],
                                        columns="VariableCode", values="DataValue")

                var_data = CompactVariableData()
                for s in series_list:
                    var_data.addData(s.variable, s.method)

                sourceInfo = SourceInfo()
                if len(series_list) > 0:
                    source = series_list[0].source
                    sourceInfo.setSourceInfo(source.organization, source.description, source.link,
                                             source.contact_name, source.phone, source.email, source.citation)

                # generate header
                file_str = self.generateHeader()
                file_str += generateSiteInformation(self.site, self.network)
                file_str += var_data.printToFile([var for var in df if isinstance(var, str)])
                file_str += "#\n" + sourceInfo.outputSourceInfo() + "#\n"

                file_out.write(file_str)
                del file_str
                del sourceInfo
                del var_data
                del dvs
                df.to_csv(file_out)
                file_out.close()
                file_info.is_empty = False
                print ('{} Finished creating {}'.format(datetime.datetime.now(), filepath))
            else:
                site_files.append(file_info)
                file_out = self.createFile(file_info.file_path)
                if file_out is None:
                    print('Unable to create output file for {}'.format(file_info.site_code))
                    return site_files
                file_info.created = True
                file_out.close()

        except InvalidRequestError as e:
            print ("We had an invalid request: {}".format(e))
        except UnicodeEncodeError as e:
            print('---\nEncoding error while writing to file: \n{}\n{}\n---'.format(type(e), e))
        except Exception as e:
            print('---\nIssue encountered while writing data to file: \n{}\n{}\n---'.format(type(e), e))
        return site_files



def generateQC1Header():
    """
    :return: Returns a string to be inserted as the CSV file's header
    :rtype: str
    """
    file_str = "# ------------------------------------------------------------------------------------------\n"
    file_str += "# WARNING: The data are released on the condition that neither iUTAH nor any of its \n"
    file_str += "# participants may be held liable for any damages resulting from their use. The following \n"
    file_str += "# metadata describe the data in this file:\n"
    file_str += "# ------------------------------------------------------------------------------------------\n"
    file_str += "#\n"
    file_str += "# Quality Control Level Information\n"
    file_str += "# -----------------------------------------------\n"
    file_str += "# These data have passed QA/QC procedures such as sensor calibration and \n"
    file_str += "# visual inspection and removal of obvious errors. These data are approved \n"
    file_str += "# by Technicians as the best available version of the data. See published\n"
    file_str += "# script for correction steps specific to this data series. \n"
    file_str += "#\n"
    return file_str

def generateQualifierHeader(qualifier_list):
    """
    :return: Returns a string to be inserted as the CSV qualifier header portion
    :rtype: str
    """
    sorted_list = sorted(qualifier_list, key=lambda x: x[0])
    file_str = "# Qualifier Information\n"
    file_str += "# ----------------------------------\n"
    file_str += "# Code   Description\n"
    for q_id, code, description in sorted_list:
        file_str += "# " + code.ljust(7) + description + "\n"
    file_str += "#\n"
    return file_str


def getHeaderDisclaimer():
    file_str = "# ------------------------------------------------------------------------------------------\n"
    file_str += "# WARNING: These are raw and unprocessed data that have not undergone quality control.\n"
    file_str += "# They are provisional and subject to revision. The data are released on the condition \n"
    file_str += "# that neither iUTAH nor any of its participants may be held liable for any damages\n"
    file_str += "# resulting from their use. The following metadata describe the data in this file:\n"
    file_str += "# ------------------------------------------------------------------------------------------\n"
    file_str += "#\n"
    return file_str

def generateSiteInformation(site, network):
    """

    :param site: Site for which to generate the header string
    :type site: Site
    :param network: Network for site (e.g. Logan, RedButte, etc)
    :return: Header string
    :rtype: str
    """
    file_str = ""
    file_str += "# Site Information\n"
    file_str += "# ----------------------------------\n"
    file_str += "# Network: " + network + "\n"
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
        return "# " + title + ": " + value + "\n"


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
        formatted = "# " + title + ": " + str(var) + "\n"
        return formatted


class CompactVariableData:
    def __init__(self):
        self.var_dict = {}
        self.method_dict = {}

    def addData(self, var, method):
        self.var_dict[var.code] = (var, method)

    def printToFile(self, vars_to_print):
        if not isinstance(vars_to_print, str) or len(vars_to_print) == 0:
            return ""
        formatted = ""
        formatted += "# Variable and Method Information\n"
        formatted += "# ---------------------------\n"
        for variable_code in vars_to_print:
            if variable_code not in self.var_dict:
                continue
            variable, method = self.var_dict[variable_code]
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
        formatted = title + ": " + str(var) + " | "
        return formatted
