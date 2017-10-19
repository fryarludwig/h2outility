from GAMUTRawData.odmdata import Series
from Utilities.DatasetUtilities import H2OManagedResource
from Common import *

__title__ = 'H2OSeries'


class H2OSeries:
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
        self.QualityControlLevelCode = QualityControlLevelCode if QualityControlLevelCode is not None else -1  #
        # type: float

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
            if isinstance(other, H2OSeries):
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
                print('Type {} cannot be compared to an H2OSeries object.'.format(type(other)))
            return comp_tuple == other_tuple

    def __ne__(self, other):
        return not (self == other)


class OdmSeriesHelper:
    RE_RESOURCE_PARSER = re.compile(r'^(?P<title>.+?)\s+\(ID (?P<id>\w+)\)$', re.I)
    MATCH_ON_ATTRIBUTE = {
        'Site': lambda first_series, second_series: first_series.SiteCode == second_series.SiteCode,
        'Variable': lambda first_series, second_series: first_series.VariableCode == second_series.VariableCode,
        'QC Code': lambda first_series, second_series: first_series.QualityControlLevelCode ==
                                                       second_series.QualityControlLevelCode,
        'Source': lambda first_series, second_series: first_series.SourceID == second_series.SourceID,
        'Method': lambda first_series, second_series: first_series.MethodID == second_series.MethodID
    }
    FORMAT_STRING = 'Series<{}; {}; QC {}; {}; {}>'

    @staticmethod
    def SeriesToString(series):
        format_string = OdmSeriesHelper.FORMAT_STRING
        if isinstance(series, list):
            series_string = 'list['
            for i in range(0, len(series)):
                series_string += OdmSeriesHelper.SeriesToString(series[i])
                if i < len(series) - 1:
                    series_string += ',\n'
            return series_string + ']'
        if isinstance(series, H2OSeries):
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
        if series is None:
            return None
        else:
            return H2OSeries(SeriesID=series.id, SiteID=series.site_id, VariableID=series.variable_id,
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
    def DetermineForcedSeriesChunking(resource):
        """

        :type resource: H2OManagedResource
        :returns list[list[H2OSeries]]
        """
        if APP_SETTINGS.VERBOSE:
            print 'Determining chunking for resource {}'.format(resource)
        if resource.single_file:  # If we should group into the fewest possible files
            chunk_dict = {}
            for series in resource.selected_series.itervalues():
                series_tuple = (series.SiteID, series.SourceID, series.QualityControlLevelID)
                if series_tuple not in chunk_dict.keys():
                    chunk_dict[series_tuple] = []
                chunk_dict[series_tuple].append(series)
            chunk_list = chunk_dict.values()
        else:  # If we should group each into its own file
            chunk_list = [[series] for series in resource.selected_series.itervalues()]
        if APP_SETTINGS.VERBOSE:
            for chunk in chunk_list:
                print 'Chunk: {}'.format(OdmSeriesHelper.SeriesToString(chunk))
        return chunk_list

    @staticmethod
    def createFile(filepath):
        try:
            print 'Creating new file {}'.format(filepath)
            return open(filepath, 'w')
        except Exception as e:
            print('---\nIssue encountered while creating a new file: \n{}\n{}\n---'.format(e, e.message))
            return None
