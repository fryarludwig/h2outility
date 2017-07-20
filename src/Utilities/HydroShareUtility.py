import re

import dateutil.parser
import xml.etree.ElementTree as ElementTree

from datetime import datetime
from hs_restclient import *
from GAMUTRawData.CSVDataFileGenerator import GenericResourceDetails
from oauthlib.oauth2 import InvalidGrantError, InvalidClientError

# service_manager = ServiceManager()

class HydroShareAccountDetails:
    def __init__(self, values=None):
        self.name = ""
        self.username = ""
        self.password = ""
        self.client_id = None
        self.client_secret = None

        if values is not None:
            self.name = values['name'] if 'name' in values else ""
            self.username = values['user'] if 'user' in values else ""
            self.password = values['password'] if 'password' in values else ""
            self.client_id = values['client_id'] if 'client_id' in values else None
            self.client_secret = values['client_secret'] if 'client_secret' in values else None

    def to_dict(self):
        return dict(username=self.username, password=self.password,
                    client_id=self.client_id, client_secret=self.client_secret)

class HydroShareResource:
    def __init__(self, resource_dict):
        self.id = resource_dict['resource_id'] if 'resource_id' in resource_dict else ""
        self.owner = resource_dict['creator'] if 'creator' in resource_dict else ""
        self.title = resource_dict['resource_title'] if 'resource_title' in resource_dict else ""
        self.abstract = ""
        self.keywords = []
        self.funding_agency = ""
        self.agency_url = ""
        self.award_title = ""
        self.award_number = ""
        self.files = []
        self.subjects = []
        self.period_start = None
        self.period_end = None
        self.metadata_xml = None
        self.public = resource_dict['public'] if 'public' in resource_dict else False

    def __str__(self):
        return '{} with {} files'.format(self.title, len(self.files))

class ResourceTemplate:
    def __init__(self, values=None):
        self.template_name = ""
        self.name_prefix = ""
        self.abstract = ""
        self.keywords = []
        self.funding_agency = ""
        self.agency_url = ""
        self.award_title = ""
        self.award_number = ""

        if values is not None:
            self.template_name = values['name'] if 'name' in values else ""
            self.name_prefix = values['resource_name'] if 'resource_name' in values else ""
            self.abstract = values['abstract'] if 'abstract' in values else ""
            self.funding_agency = values['funding_agency'] if 'funding_agency' in values else ""
            self.agency_url = values['agency_url'] if 'agency_url' in values else ""
            self.award_title = values['award_title'] if 'award_title' in values else ""
            self.award_number = values['award_number'] if 'award_number' in values else ""

    def __str__(self):
        return self.template_name


class HydroShareUtilityException(Exception):
    def __init__(self, args):
        super(HydroShareUtilityException, self).__init__(args)


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
        self.resource_cache = {}  # type: dict of HSResource

    def authenticate(self, username, password, client_id=None, client_secret=None):
        """
        Authenticates access to allow read/write access to privileged resource_cache
        :param username: username for HydroShare.org
        :param password: password associated with username
        :param client_id: Client ID obtained from HydroShare
        :param client_secret: Client Secret provided by HydroShare
        :return: Returns true if authentication was successful, false otherwise
        """
        if client_id is not None and client_secret is not None:
            self.auth = HydroShareAuthOAuth2(client_id, client_secret, username=username, password=password)
        else:
            self.auth = HydroShareAuthBasic(username, password)
        try:
            self.client = HydroShare(auth=self.auth)#, verify=False)
            self.user_info = self.client.getUserInfo()
            return True
        except HydroShareException as e:  # for incorrect username/password combinations
            print('Authentication failed: {}'.format(e))
        except InvalidGrantError as e:  # for failures when attempting to use OAuth2
            print('Credentials could not be validated: {}'.format(e))
        except InvalidClientError as e:
            print('Invalid client ID and/or client secret: {}'.format(e))
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
        re_breakdown = re.compile(regex, re.I)
        resource_files = self.getResourceFileList(resource_id)
        duplicates_list = []
        for remote_file in resource_files:
            results = re_breakdown.match(remote_file['url'])  # Check the file URL for expected patterns
            temp_dict = {'duplicated': ''} if results is None else results.groupdict()  # type: dict
            if len(temp_dict['duplicated']) > 0:               # A non-duplicated file will match with length 0
                duplicates_list.append(temp_dict)              # Save the file name so we can remove it later

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
                # self.resource_cache[resource_id].files.remove(file_detail['name'])
                print('Deleting file {}...'.format(file_detail['name']))
            else:
                print('Skipping duplicate file {}...'.format(file_detail['name']))

    def pairSitesToResources(self, site_list, resource_list):
        """
        Given a list of files and optional global filter, find a resource that matches the site code of each file
        :param site_list: List of sites to be matched with corresponding HydroShare resource_cache
        :type site_list: list of str
        :param resource_list: Resources to attempt to match to the file list
        :type resource_list: List of HydroShareResource
        :return: Returns matched and unmatched files dictionary lists in form [{'resource': resource, 'file', file_dict,
                 'overwrite_remote': True/False }, { ... } ]
        """
        matched_sites = []
        unmatched_sites = []
        for site in site_list:
            found_match = False
            for resource in resource_list:
                # resource_title = self.resource_cache[resource_id].name
                if not re.search(site, resource.title, re.IGNORECASE):
                    continue
                matched_sites.append({'resource_id': resource.title, 'site_code': site})
                found_match = True
                break
            if not found_match:
                unmatched_sites.append(site)
        return matched_sites, unmatched_sites

    def pairFilesToResources(self, file_list, resource_list):
        """
        Given a list of files and optional global filter, find a resource that matches the site code of each file
        :param file_list: List of files to be matched with corresponding HydroShare resource_cache
        :type file_list: List of dictionary objects, formatted as {'path': path, 'name': name, 'site': site}
        :param resource_list: Resources to attempt to match to the file list
        :type resource_list: List of HydroShareResource
        :return: Returns matched and unmatched files dictionary lists in form [{'resource': resource, 'file', file_dict,
                 'overwrite_remote': True/False }, { ... } ]
        """
        matched_files = []
        unmatched_files = []
        for local_file in file_list:
            found_match = False
            for resource in resource_list:
                # resource_title = self.resource_cache[resource_id].name
                if not re.search(local_file['site'], resource.title, re.IGNORECASE):
                    continue
                resource_files = self.getResourceFileList(resource.id)
                file_list = []
                for resource_file in resource_files:
                    file_url = resource_file['url']
                    file_list.append(file_url)
                print file_list
                duplicates = len([remote_file for remote_file in file_list if local_file['name'] in remote_file])
                matched_files.append({'resource_id': resource.id, 'file': local_file, 'overwrite_remote': duplicates})
                found_match = True
                break
            if not found_match:
                unmatched_files.append(local_file)
        return matched_files, unmatched_files

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
            print 'Error while fetching resource files {}'.format(e)
            return []

    def getAllResources(self):
        filtered_resources = []
        owner = self.user_info['username']
        if self.auth is None:
            raise HydroShareUtilityException("Cannot query resources without authentication")
        all_resources = self.client.resources(owner=owner)
        for resource in all_resources:
            resource_object = HydroShareResource(resource)
            filtered_resources.append(resource_object)
        return filtered_resources

    def getMetadataForResource(self, resource):
        """

        :type resource: HydroShareResource
        """
        metadata = self.client.getScienceMetadata(resource.id)
        resource.subjects = [item['value'] for item in metadata['subjects']]
        resource.abstract = metadata['description']
        resource.agency_url = metadata['funding_agencies'][0]['agency_url']
        resource.funding_agency = metadata['funding_agencies'][0]['agency_name']
        resource.award_number = metadata['funding_agencies'][0]['award_number']
        resource.award_title = metadata['funding_agencies'][0]['award_title']

    def updateResourceMetadata(self, resource):
        """

        :type resource: HydroShareResource
        """
        return self.client.updateScienceMetadata(resource.id, resource.to_json())

    def getFileListForResource(self, resource):
        resource.files = [os.path.basename(f['url']) for f in self.getResourceFileList(resource.id)]
        return resource.files

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

    def upload(self, files_list, resource_id, retry_on_failure=False):
        """
        Connect as user and upload the files to HydroShare
        :param paired_file_list: List of dictionaries in format [ {"name": "file_name", "path": "file_path" }, {...} ]
        :param retry_on_failure: If an exception occurs in this function, this function will call itself once more
        :return: An error string on multiple failures, and nothing on success
        """
        if self.auth is None:
            raise HydroShareUtilityException("Cannot modify resources without authentication")
        try:
            for local_file in files_list:

                matching_files = [f for f in self.getResourceFileList(resource_id) if local_file.file_name in f]
                action = 'update' if len(matching_files) > 0 else 'create'
                if action == 'update':
                    self.client.deleteResourceFile(resource_id, local_file.file_name)
                self.client.addResourceFile(resource_id, str(local_file.file_path)) #, resource_filename=local_file.file_name)
                # if self.updateResourcePeriodMetadata(resource_id, local_file.coverage_start, local_file.coverage_end):
                #     print "Resource metadata for temporal coverage was updated"
                #     exit()
                # else:
                #     print "Unable to update resource metadata"
                print("{} {}d in remote {}".format(local_file.file_name, action, self.resource_cache[resource_id].name))
        except HydroShareException as e:
            if retry_on_failure:
                print('Upload encountered an error - attempting again. Error encountered: \n{}'.format(e.message))
                return self.upload(files_list, resource_id, retry_on_failure=False)
            else:
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
                print 'Setting resource {} as public'.format(self.resource_cache[resource_id].name)
                self.client.setAccessRules(resource_id, public=True)
            except HydroShareException as e:
                print "Access rule edit failed - could not set to public due to exception: {}".format(e)
            except KeyError as e:
                print 'Incorrectly formatted arguments given. Expected key not found: {}'.format(e)

    def removeResourceFiles(self, files_list, resource_id, quiet_fail=True):
        if self.auth is None:
            raise HydroShareUtilityException("Cannot modify resources without authentication")
        try:
            for local_file in files_list:
                if local_file.file_name in self.resource_cache[resource_id].files:
                    print "I'm gonna delete the file: {}".format(local_file.file_name)
                    self.client.deleteResourceFile(resource_id, local_file.file_name)
                elif quiet_fail:
                    print "Cannot delete file that does not exist on HydroShare: {}".format(local_file.file_name)
                else:
                    resource_name = self.resource_cache[resource_id].name
                    raise HydroShareUtilityException("Unable to delete {} on {}".format(local_file, resource_name))
        except HydroShareException as e:
            return ["Removal failed - could not complete upload to HydroShare due to exception: {}".format(e)]
        except KeyError as e:
            return ['Incorrectly formatted arguments given. Expected key not found: {}'.format(e)]
        return []

    def updateResourcePeriodMetadata(self, resource_id, period_start, period_end):
        resource_start, resource_end = self.getResourceCoveragePeriod(resource_id)
        if resource_start is None or resource_end is None:
            return False
        if period_start is None or period_end is None:
            return False

        resource_updated = False
        if period_start < resource_start:
            self.resource_cache[resource_id].period_start = period_start
            resource_updated = True

        if period_end > resource_end:
            self.resource_cache[resource_id].period_end = period_end
            resource_updated = True

        if resource_updated and self.resource_cache[resource_id].metadata_xml is not None:
            try:
                print 'Now doing the updateResourcePeriodMetadata function stuff'
                start = self.resource_cache[resource_id].period_start  # type: datetime
                end = self.resource_cache[resource_id].period_end  # type: datetime

                xml_tree = self.resource_cache[resource_id].metadata_xml
                node_names = ['rdf:Description', 'dc:coverage', 'dcterms:period', 'rdf:value', 'period']
                next_node = xml_tree
                for level in node_names:
                    if next_node is None:
                        return False
                    elif level == 'period':
                        next_node.text = self.xml_coverage.format(start=start.strftime(self.time_format),
                                                                  end=end.strftime(self.time_format))
                        print next_node.text
                    else:
                        next_node = next_node.find(level, namespaces=self.xml_ns)

                self.resource_cache[resource_id].metadata_xml = xml_tree
                xml_string = ElementTree.tostring(self.resource_cache[resource_id].metadata_xml)
                xml_file_name = 'resourcemetadata.xml'
                file_out = open(xml_file_name, 'wb')
                file_out.write(xml_string)
                file_out.close()

                # print coverage_str
                self.client.updateScienceMetadata(resource_id, xml_file_name)
                # os.remove(xml_file_name)
            except Exception as e:
                print e
            return True
        else:
            return False

    def getResourceCoveragePeriod(self, resource_id, refresh_cache=False):
        if resource_id not in self.resource_cache:
            self.resource_cache[resource_id] = HydroShareResource({'resource_id': resource_id})
            self.resource_cache[resource_id].id = resource_id
        if self.resource_cache[resource_id].metadata_xml is None or refresh_cache:
            metadata = self.client.getScienceMetadata(resource_id)
            self.resource_cache[resource_id].metadata_xml = ElementTree.fromstring(metadata)
            self.resource_cache[resource_id].period_start = None
            self.resource_cache[resource_id].period_end = None
            print("Updating the metadata cache for resource {}".format(self.resource_cache[resource_id].name))
        if self.resource_cache[resource_id].period_start is None or self.resource_cache[resource_id].period_end is None:
            try:
                xml_tree = self.resource_cache[resource_id].metadata_xml
                description_node = xml_tree.find('rdf:Description', namespaces=self.xml_ns)
                coverage_node = description_node.find('dc:coverage', namespaces=self.xml_ns)
                period_node = coverage_node.find('dcterms:period', namespaces=self.xml_ns)
                value_node = period_node.find('rdf:value', namespaces=self.xml_ns)
                match = self.re_period.match(value_node.text)
                if match is not None:
                    self.resource_cache[resource_id].period_start = dateutil.parser.parse(match.group('start'))
                    self.resource_cache[resource_id].period_end = dateutil.parser.parse(match.group('end'))
            except Exception as e:
                print("Unable to find coverage data - encountered exception {}".format(e.message))
        return self.resource_cache[resource_id].period_start, self.resource_cache[resource_id].period_end

    def deleteResource(self, resource_id, confirm=True):
        if self.auth is None:
            raise HydroShareUtilityException("Cannot modify resources without authentication")
        try:
            resource_name = self.resource_cache[resource_id] if resource_id in self.resource_cache else resource_id
            if confirm:
                user_input = raw_input('Are you sure you want to delete the resource {}? (y/N): '.format(resource_name))
                if user_input.lower() != 'y':
                    return
            print 'Deleting resource {}'.format(resource_name)
            if resource_id in self.resource_cache:
                self.resource_cache.pop(resource_id)
            self.client.deleteResource(resource_id)
        except Exception as e:
            print 'Exception encountered while deleting resource {}: {}'.format(resource_id, e)


    def createNewResource(self, resource):
        """

        :param resource:
        :type resource: GenericResourceDetails
        :return:
        :rtype: str
        """
        if self.auth is None:
            raise HydroShareUtilityException("Cannot modify resources without authentication")

        # http://hs-restclient.readthedocs.io/en/latest/
        if resource is not None:
            # print 'Creating resource {}'.format(resource.resource_name)
            # print 'Metadata: {}'.format(resource.getMetadata())
            # print 'Formatted: \n{}'.format(resource.getMetadata().replace(r'}, {', '},\n{'))
            resource_id = self.client.createResource(resource_type='GenericResource', title=resource.resource_name,
                                                     abstract=resource.abstract, keywords=resource.keywords,
                                                     metadata=resource.getMetadata())
            new_resource = HydroShareResource({})
            new_resource.id = resource_id
            new_resource.name = resource.resource_name
            self.resource_cache[resource_id] = new_resource
            return resource_id
