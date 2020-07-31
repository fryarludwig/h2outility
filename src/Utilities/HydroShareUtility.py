import xml.etree.ElementTree as ElementTree
import os
import re
import sys
import json

import dateutil.parser
from hs_restclient import HydroShareNotFound, HydroShareAuthBasic, HydroShareAuthOAuth2, HydroShare, HydroShareException
from oauthlib.oauth2 import InvalidClientError, InvalidGrantError

from wx.lib.pubsub import pub
from Common import APP_SETTINGS
from Utilities.DatasetUtilities import H2OManagedResource


class HydroShareAccountDetails:
    """
    Used to organize account authentication details
    """

    CLIENT_ID = ''
    CLIENT_SECRET = ''

    def __init__(self, values=None):
        self.name = ""
        self.username = ""
        self.password = ""

        # settings_path = os.environ.get('SETTINGS_PATH')

        # from connection_details import *

        # with open(settings_path, 'rb') as fin:
        #     settings = json.load(fin)
        #
        #     try:
        #         self.CLIENT_ID = settings['client_id']
        #         self.CLIENT_SECRET = settings['client_secret']
        #         self.client_id = self.CLIENT_ID
        #         self.client_secret = self.CLIENT_SECRET
        #     except KeyError:
        #         # This was the way the previous developer was importing settings. I left it here
        #         # but there's no knowing that it will ever work...
        #         self.client_id = values['client_id'] if 'client_id' in values else None
        #         self.client_secret = values['client_secret'] if 'client_secret' in values else None

        self.client_id = self.CLIENT_ID = "8DKopVUmqJ62W1NKN1FbgBkmrrQsHUIIgxEMtHkZ"
        self.client_secret = self.CLIENT_SECRET = "RwcQhj6wgCdsg14TdpIIQ4VM7rhKO3GCoQrJlJaCKz6woh8Y8VkPzl8u54dxgAZLjmK7quUocHmwUYK1yUVnjvJVqDFy0Te9BgEgn5tWEQXeKeeA9KHeC5VBaBSr95ja"

        if values is not None:
            self.name = values.get('name', '')
            self.username = values.get('user', '')
            self.password = values.get('password', '')

    def to_dict(self):
        return dict(username=self.username, password=self.password,
                    client_id=self.client_id, client_secret=self.client_secret)


class HydroShareResource:
    """
    Used to organize HydroShare resource details
    """
    def __init__(self, resource_dict):
        self.id = resource_dict['resource_id'] if 'resource_id' in resource_dict else ""
        self.owner = resource_dict['creator'] if 'creator' in resource_dict else ""
        self.title = resource_dict['resource_title'] if 'resource_title' in resource_dict else ""
        self.abstract = ""
        self.funding_agency = ""
        self.agency_url = ""
        self.award_title = ""
        self.award_number = ""
        self.files = []
        self.subjects = []
        self.period_start = ""
        self.period_end = ""
        self.public = resource_dict['public'] if 'public' in resource_dict else False
        self.shareable = resource_dict.get('shareable', False)

    @property
    def keywords(self):
        return self.subjects

    def get_metadata(self):
        md = self.__metadata__()

        if APP_SETTINGS.H2O_DEBUG:
            print(md)
        return md

    def __str__(self):
        return '{} with {} files'.format(self.title, len(self.files))

    def __repr__(self):
        return getattr(self, 'title', 'N/A')

    def __metadata__(self):

        metadata = {}

        if hasattr(self, 'title'):
            metadata['title'] = getattr(self, 'title', '')

        if hasattr(self, 'abstract'):
            if len(self.abstract):
                metadata['description'] = getattr(self, 'abstract', '')

        if all([getattr(self, 'period_start', None), getattr(self, 'period_end', None)]):
            metadata['coverage'] = [{"type": "period",
                                             "value": {"start": self.period_start, "end": self.period_end}}]

        fundingagency = {}
        fundingagency_attr_map = (
            ('funding_agency', 'agency_name'),
            ('award_title', 'award_title'),
            ('award_number', 'award_number'),
            ('agency_url', 'agency_url')
        )

        for reskey, fakey in fundingagency_attr_map:
            """
            reskey - resource attribute name
            fakey - funding agency attribute name
            """
            value = getattr(self, reskey)

            if isinstance(value, unicode):
                value = str(value)

            if isinstance(value, str):
                value = value.strip()
                if len(value):
                    fundingagency[fakey] = value

        if len(fundingagency.keys()):
            metadata['fundingagency'] = fundingagency

        return metadata


class ResourceTemplate(object):

    KW_DELIMITER = ','

    def __init__(self, *args, **kwargs):

        self.template_name = kwargs.get('name', '')
        self.title = kwargs.get('resource_name', '')
        self.abstract = kwargs.get('abstract', '')
        self.funding_agency = kwargs.get('funding_agency', '')
        self.agency_url = kwargs.get('agency_url', '')
        self.award_title = kwargs.get('award_title', '')
        self.award_number = kwargs.get('award_number', '')
        self.keywords = kwargs.get('keywords', [])

        for arg in args:
            if isinstance(arg, dict):
                for key, value in arg.iteritems():
                    setattr(self, key, value)

        self.keywords = self.__parse_keywords(self.keywords)

    @property
    def subjects(self):
        if isinstance(self.keywords, list):
            return self.keywords
        return self.__parse_keywords(self.keywords)

    def __parse_keywords(self, keywords):
        if not isinstance(keywords, list):

            if isinstance(keywords, set):
                keywords = list(keywords)
            else:
                keywords = str(keywords).split(self.KW_DELIMITER)

        return keywords

    def get_metadata(self):
        return str([{'funding_agencies': {'agency_name': self.funding_agency,
                                          'award_title': self.award_title,
                                          'award_number': self.award_number,
                                          'agency_url': self.agency_url}}]).replace('\'', '"')

    def __str__(self):
        return self.template_name


class HydroShareUtilityException(Exception):
    def __init__(self, *args):
        super(HydroShareUtilityException, self).__init__(*args)


class HydroShareUtility:
    def __init__(self):
        self.client = None  # type: HydroShare
        self.auth = None
        self.user_info = None
        self.re_period = re.compile(r'(?P<tag_start>^start=)(?P<start>[0-9-]{10}T[0-9:]{8}).{2}(?P<tag_end>end=)'
                                    r'(?P<end>[0-9-]{10}T[0-9:]{8}).{2}(?P<tag_scheme>scheme=)(?P<scheme>.+$)', re.I)
        self.xml_ns = {
            'dc': "http://purl.org/dc/elements/1.1/",
            'dcterms': "http://purl.org/dc/terms/",
            'hsterms': "http://hydroshare.org/terms/",
            'rdf': "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            'rdfs1': "http://www.w3.org/2001/01/rdf-schema#"}
        self.time_format = '%Y-%m-%dT%H:%M:%S'
        self.xml_coverage = 'start={start}; end={end}; scheme=W3C-DTF'

    def authenticate(self, username, password, client_id=None, client_secret=None):
        """
        Authenticates access to allow read/write access to privileged resource_cache
        :param username: username for HydroShare.org
        :param password: password associated with username
        :param client_id: Client ID obtained from HydroShare
        :param client_secret: Client Secret provided by HydroShare
        :return: Returns true if authentication was successful, false otherwise
        """

        if not all([username, password]):
            self.auth = None
            return False

        if client_id is not None and client_secret is not None:
            self.auth = HydroShareAuthOAuth2(client_id, client_secret, username=username, password=password)
        else:
            self.auth = HydroShareAuthBasic(username, password)
        try:
            self.client = HydroShare(auth=self.auth)  # , verify=False)
            self.user_info = self.client.getUserInfo()
            return True
        except HydroShareException as e:  # for incorrect username/password combinations
            print('Authentication failed: {}'.format(e))
        except InvalidGrantError as e:  # for failures when attempting to use OAuth2
            print('Credentials could not be validated: {}'.format(e))
        except InvalidClientError as e:
            print('Invalid client ID and/or client secret: {}'.format(e))
        except Exception as e:
            print(e)
        
        self.auth = None

        return False

    def purgeDuplicateGamutFiles(self, resource_id, regex, confirm_delete=False):
        """
        Removes all files that have a duplicate-style naming pattern (e.g. ' (1).csv', '_ASDFGJK9.csv'
        :param resource_id: Resource to inspect for duplicates
        :type resource_id: Resource object received from the HydroShare API client
        :param confirm_delete: If true, requires input that confirm file should be deleted
        :type confirm_delete: bool
        """
        from collections import defaultdict
        re_breakdown = re.compile(regex, re.I)
        resource_files = self.getResourceFileList(resource_id)  # type: [dict]
        duplicates_list = []
        for remote_file in resource_files:
            url = remote_file.get('url', '')
            results = re_breakdown.match(url)  # Check the file URL for expected patterns

            temp_dict = defaultdict(lambda: '')  # type: dict

            if results:
                temp_dict['duplicated'] = results.groupdict()

            if len(temp_dict.get('duplicated', '')):  # A non-duplicated file will match with length 0
                duplicates_list.append(temp_dict)  # Save the file name so we can remove it later

        for file_detail in duplicates_list:
            if not confirm_delete:
                delete_me = True
            else:
                user_answer = raw_input("Delete file {} [Y/n]: ".format(file_detail['name']))
                if user_answer != 'N' and user_answer != 'n':
                    delete_me = True
                else:
                    delete_me = False

            if delete_me:
                self.client.deleteResourceFile(resource_id, file_detail['name'])
                print('Deleting file {}...'.format(file_detail['name']))
            else:
                print('Skipping duplicate file {}...'.format(file_detail['name']))

    def getResourceFileList(self, resource_id):
        """

        :param resource_id: ID of resource for which to retrieve a file list
        :type resource_id: str
        :return: List of files in resource
        :rtype: list of str
        """
        try:
            return list(self.client.getResourceFileList(resource_id))
        except Exception as e:
            print('Error while fetching resource files {}'.format(e))
            return []

    def getAllResources(self):
        filtered_resources = {}
        owner = self.user_info['username']
        if self.auth is None:
            raise HydroShareUtilityException("Cannot query resources without authentication")
        all_resources = self.client.resources(owner=owner)
        for resource in all_resources:
            resource_object = HydroShareResource(resource)
            filtered_resources[resource_object.id] = resource_object
        return filtered_resources

    def getMetadataForResource(self, resource):
        """

        :type resource: HydroShareResource
        """
        metadata = self.client.getScienceMetadata(resource.id)
        resource.title = metadata.get('title', '')
        resource.subjects = [item['value'] for item in metadata.get('subjects', [])]
        resource.abstract = metadata.get('description', '')

        if resource.abstract is None:
            resource.abstract = ''

        if 'funding_agencies' in metadata and len(metadata['funding_agencies']) > 0:
            funding_agency = metadata['funding_agencies'][0]
            resource.agency_url = funding_agency['agency_url'] if 'agency_url' in funding_agency else ''
            resource.funding_agency = funding_agency['agency_name'] if 'agency_name' in funding_agency else ''
            resource.award_number = funding_agency['award_number'] if 'award_number' in funding_agency else ''
            resource.award_title = funding_agency['award_title'] if 'award_title' in funding_agency else ''

    def updateResourceMetadata(self, resource):
        """

        :type resource: HydroShareResource
        """
        return self.client.updateScienceMetadata(resource.id, resource.get_metadata())

    def _request(self, method, url, params=None, data=None, files=None, headers=None, stream=False):
        request = self.client.session.request(method, url, params=params, data=data, files=files, headers=headers,
                                              stream=stream)

        return request

    def requestAccessRules(self, resource):
        """
        Get access rule for a resource.
        """
        url = "{url_base}/resource/{pid}/sysmeta/".format(url_base=self.client.url_base, pid=resource.id)

        r = self._request('GET', url)
        if r.status_code != 200:
            raise Exception("Failed to get system metadata for resource: {}".format(resource.id))

        data = r.json()
        resource.public = data.get('public', False)
        resource.shareable = data.get('shareable', False)

    def makePublic(self, resource, public=True):
        """
        Makes a resource public or private
        :param resource: The resource you want to modify
        :param public: boolean value, True makes the resource public, False makes it private (wowzer)
        :return: None
        """
        hs = HydroShare(auth=self.auth)
        res = hs.resource(resource.id).public(public)

        if res.status_code == 200 or res.status_code == 202:
            resource.public = public

    def updateKeywords(self, resource, keywords=None):
        if keywords is None:
            keywords = resource.keywords

        # remove leading/trailing whitespaces from keywords
        keywords = map(lambda x: x.strip(), keywords)

        url = "{url_base}/resource/{id}/scimeta/elements/".format(url_base=self.client.url_base, id=resource.id)

        subjects = []
        for keyword in keywords:
            subjects.append({"value": keyword})

        r = self.client.session.request('PUT', url, json={"subjects": subjects})

        if r.status_code != 202:
            raise HydroShareException((url, 'PUT', r.status_code, keywords))
        return r.json()

    def getFileListForResource(self, resource):
        resource.files = [os.path.basename(f['url']) for f in self.getResourceFileList(resource.id)]
        return resource.files

    def getFilesByResourceId(self, resource_id):
        return [os.path.basename(f['url']) for f in self.getResourceFileList(resource_id)]

    def filterResourcesByRegex(self, regex_string=None, owner=None, regex_flags=re.IGNORECASE):
        """
        Apply a regex filter to all available resource_cache. Useful for finding GAMUT resource_cache
        :param owner: username of the owner of the resource
        :type owner: string
        :param regex_string: String to be used as the regex filter
        :param regex_flags: Flags to be passed to the regex search
        :return: A list of resource_cache that matched the filter
        """
        filtered_resources = []
        if owner is None:
            owner = self.user_info['username']
        if self.auth is None:
            raise HydroShareUtilityException("Cannot query resources without authentication")
        all_resources = self.client.resources(owner=owner)
        regex_filter = re.compile(regex_string, regex_flags)
        for resource in all_resources:
            if regex_string is not None and regex_filter.search(resource['resource_title']) is None:
                continue
            resource_object = HydroShareResource(resource)
            resource_object.files = [os.path.basename(f['url']) for f in self.getResourceFileList(resource_object.id)]
            filtered_resources.append(resource_object)
        return filtered_resources

    def UploadFiles(self, files, resource):  # type: ([str], HydroShareResource) -> bool
        if self.auth is None:
            raise HydroShareUtilityException("Cannot modify resources without authentication")
        try:
            for csv_file in files:
                try:
                    self.client.deleteResourceFile(resource.id, os.path.basename(csv_file))
                except HydroShareNotFound:
                    pass
                # except Exception as e:
                #     if APP_SETTINGS.H2O_DEBUG and APP_SETTINGS.VERBOSE:
                #         print 'File did not exist in remote: {}, {}'.format(type(e), e)
                if type(csv_file) != str:
                    csv_file = str(csv_file)
                self.client.addResourceFile(resource.id, csv_file)

                msg = "File {} uploaded to remote {}".format(os.path.basename(csv_file), repr(resource))
                print(msg)
                pub.sendMessage('logger', message=msg)

        except HydroShareException as e:
            print("Upload failed - could not complete upload to HydroShare due to exception: {}".format(e))
            return False
        except KeyError as e:
            print('Incorrectly formatted arguments given. Expected key not found: {}'.format(e))
            return False
        return True

    def setResourcesAsPublic(self, resource_ids):
        if self.auth is None:
            raise HydroShareUtilityException("Cannot modify resources without authentication")
        for resource_id in resource_ids:
            try:
                print('Setting resource {} as public'.format(resource_id))
                self.client.setAccessRules(resource_id, public=True)
            except HydroShareException as e:
                print("Access rule edit failed - could not set to public due to exception: {}".format(e))
            except KeyError as e:
                print('Incorrectly formatted arguments given. Expected key not found: {}'.format(e))

    def deleteFilesInResource(self, resource):
        if self.auth is None:
            raise HydroShareUtilityException("Cannot modify resources without authentication")

        try:
            file_list = self.getResourceFileList(resource.id)
            for file_info in file_list:
                msg = 'Deleting resource file: {}'.format(os.path.basename(file_info['url']))
                print(msg)
                pub.sendMessage('logger', message=msg)
                self.client.deleteResourceFile(resource.id, os.path.basename(file_info['url']))
        except Exception as e:
            print('Could not delete files in resource {}\n{}'.format(resource.id, e))

    def getResourceCoveragePeriod(self, resource_id):
        metadata = self.client.getScienceMetadata(resource_id)
        period_start = None
        period_end = None
        try:
            xml_tree = ElementTree.fromstring(metadata)
            description_node = xml_tree.find('rdf:Description', namespaces=self.xml_ns)
            coverage_node = description_node.find('dc:coverage', namespaces=self.xml_ns)
            period_node = coverage_node.find('dcterms:period', namespaces=self.xml_ns)
            value_node = period_node.find('rdf:value', namespaces=self.xml_ns)
            match = self.re_period.match(value_node.text)
            if match is not None:
                period_start = dateutil.parser.parse(match.group('start'))
                period_end = dateutil.parser.parse(match.group('end'))
        except Exception as e:
            print("Unable to find coverage data - encountered exception {}".format(e))
        return period_start, period_end

    def deleteResource(self, resource_id, confirm=True):
        if self.auth is None:
            raise HydroShareUtilityException("Cannot modify resources without authentication")
        try:
            if confirm:
                user_input = raw_input('Are you sure you want to delete the resource {}? (y/N): '.format(resource_id))
                if user_input.lower() != 'y':
                    return
            print('Deleting resource {}'.format(resource_id))
            self.client.deleteResource(resource_id)
        except Exception as e:
            print('Exception encountered while deleting resource {}: {}'.format(resource_id, e))

    def createNewResource(self, resource):  # type: (ResourceTemplate) -> HydroShareResource
        """

        :param resource:
        :type resource: ResourceTemplate
        :return:
        :rtype: str
        """
        if self.auth is None:
            raise HydroShareUtilityException("Cannot create resource without authentication")

        # http://hs-restclient.readthedocs.io/en/latest/
        if resource is not None:

            metadata = []

            fundingagency = {}
            fundingagency_attr_map = (
                ('funding_agency', 'agency_name'),
                ('award_title', 'award_title'),
                ('award_number', 'award_number'),
                ('agency_url', 'agency_url')
            )

            for reskey, fakey in fundingagency_attr_map:
                # reskey - resource attribute name
                # fakey - funding agency attribute name

                value = getattr(resource, reskey).strip()

                if len(value):
                    fundingagency[fakey] = value

            if len(fundingagency.keys()):
                metadata.append({'fundingagency': fundingagency})

            resource_id = self.client.createResource(resource_type='CompositeResource',
                                                     title=resource.title,
                                                     abstract=resource.abstract,
                                                     keywords=resource.keywords,
                                                     metadata=json.dumps(metadata, encoding='ascii'))
            hs_resource = HydroShareResource({'resource_id': resource_id})
            self.getMetadataForResource(hs_resource)
            return hs_resource
        return None


if __name__ == "__main__":

    args = sys.argv

    util = HydroShareAccountDetails(values=args)
