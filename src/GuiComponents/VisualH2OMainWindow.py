from functools import partial

import datetime
import threading
from Queue import Queue
import copy

import wx
import wx.dataview
import wx.grid

from wx.lib.pubsub import pub
# from pubsub import pub

from hs_restclient import HydroShareException

from Utilities.HydroShareUtility import HydroShareAccountDetails, HydroShareUtility, ResourceTemplate, \
    HydroShareResource
from Common import *
from Utilities.DatasetUtilities import OdmDatasetConnection, H2OManagedResource
from Utilities.H2OServices import H2OService
from Utilities.H2OSeries import H2OSeries, OdmSeriesHelper
from GAMUTRawData.odmservices import ServiceManager
from EditConnectionsDialog import DatabaseConnectionDialog
from EditAccountsDialog import HydroShareAccountDialog
from WxUtilities import WxHelper, Orientation, PADDING, ALIGN
from ResourceTemplatesDialog import HydroShareResourceTemplateDialog
from InputValidator import *
from GuiComponents.UIController import UIController
from sqlalchemy.exc import DBAPIError

service_manager = ServiceManager()


class CHOICE_DEFAULTS:
    NEW_TEMPLATE_CHOICE = 'Create a new resource template'
    SELECT_TEMPLATE_CHOICE = 'Select a resource template'
    RESOURCE_STR = '{:<130} (ID {})'
    CREATE_NEW_RESOURCE = 'Create a new resource'
    MANAGED_RESOURCES = '      -- {} resource{} managed by H2O --'
    UNMANAGED_RESOURCES = '      -- {} resource{} not managed by H2O --'
    CONNECT_TO_HYDROSHARE = 'Please connect to a HydroShare account'


HS_RES_STR = lambda resource: CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, resource.id)
H2O_RES_STR = lambda resource: CHOICE_DEFAULTS.RESOURCE_STR.format(resource.resource.title, resource.id)


# noinspection PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,
# PyUnusedLocal
# noinspection PyUnusedLocal,PyUnusedLocal,PyUnusedLocal
class VisualH2OWindow(wx.Frame):

    selected_account_name = None



    def __init__(self, parent, id, title):
        ###########################################
        # Declare/populate variables, wx objects  #
        ###########################################

        super(VisualH2OWindow, self).__init__(parent, id, title)

        APP_SETTINGS.GUI_MODE = True

        self.MONOSPACE = wx.Font(9, 75, 90, 90, False, "Inconsolata")

        self.ORIGINAL_SIZE = (940, 860)
        self._setup_internal_subscriptions()
        h2o_subs = self._setup_h2o_subscriptions()
        self.H2OService = H2OService(subscriptions=h2o_subs)

        self.odm_series_dict = {}  # type: dict[str, Series]
        self.h2o_series_dict = {}  # type: dict[str, H2OSeries]

        self._resources = None  # type: dict[str, HydroShareResource]
        self.clean_resource = None

        # Widgets
        self.status_gauge = None  # type: wx.Gauge
        self.database_connection_choice = None  # type: wx.Choice
        self.hydroshare_account_choice = None  # type: wx.Choice
        self.mapping_grid = None  # type: WxHelper.SeriesGrid

        # just technicalities, honestly
        super(wx.Frame, self).__init__(parent, id, title, style=wx.DEFAULT_FRAME_STYLE, size=self.ORIGINAL_SIZE)
        self.panel = wx.Panel(self, wx.ID_ANY)
        self.Centre()
        main_sizer = self._build_main_window()
        self.panel.SetSizerAndFit(main_sizer)
        self.Fit()

        self.Show(True)

        self.H2OService.LoadData()
        self.update_choice_controls()

        self.resourceUIController.Disable()
        self.odmSeriesUIController.Disable()

    def _setup_internal_subscriptions(self):
        subscriptions = [
            (self.on_delete_resource_template_clicked, 'hs_resource_remove'),
            (self.on_save_resource_template, 'hs_resource_save'),
            (self.on_create_resource, 'hs_resource_create'),
            (self.on_save_hydroshare_auth, 'hs_auth_save'),
            (self.on_test_hydroshare_auth, 'hs_auth_test'),
            (self.on_remove_hydroshare_auth, 'hs_auth_remove'),
            (self.on_save_database_auth, 'db_auth_save'),
            (self.on_test_database_auth, 'db_auth_test'),
            (self.on_remove_database_auth, 'db_auth_remove'),
            (self.__onConnectHydroshare, 'hydroshare.connect'),
            (self.__onChangeResource, 'resource.change'),
            (self.__onGetResourceComplete, 'onCompleteGetResource'),
            (self.__onChangeODMDBConnection, 'odmdb.change_connection'),
            (self.__onSetVisibility, 'resource.set_visibility'),
            (self.__onUpdateResource, 'resource.update')
        ]
        for observer, signal in subscriptions:
            pub.subscribe(observer, signal)

    def _setup_h2o_subscriptions(self):
        subscriptions = [
            (self.on_log_print, 'logger'),
            (self.on_operations_stopped, 'Operations_Stopped'),
            (self.on_datasets_generated, 'Datasets_Completed'),
            (self.on_file_generation_failed, 'File_Failed'),
            (self.update_status_gauge_datasets, 'Dataset_Started'),
            (self.update_status_gauge_datasets, 'Dataset_Generated'),
            (self.update_status_gauge_uploads, 'Files_Uploaded'),
            (self.update_status_gauge_uploads, 'Uploads_Completed')
        ]

        for sub_tuple in subscriptions:
            pub.subscribe(sub_tuple[0], sub_tuple[1])
        return [sub_tuple[1] for sub_tuple in subscriptions]

    def on_log_print(self, message=""):
        if message is None or len(message) < 4 or message.isspace():
            return
        self.log_message_listbox.Append('{}: {}'.format(datetime.datetime.now().strftime('%H-%M-%S'), message))
        selections = self.log_message_listbox.GetSelections()
        for selection in selections:
            self.log_message_listbox.Deselect(selection)
        self.log_message_listbox.Select(len(self.log_message_listbox.Items) - 1)
        self.log_message_listbox.Deselect(len(self.log_message_listbox.Items) - 1)

    def update_choice_controls(self, progress=None):
        WxHelper.UpdateChoiceControl(self.database_connection_choice, self._get_database_choices())
        WxHelper.UpdateChoiceControl(self.hydroshare_account_choice, self._get_hydroshare_choices())

    def on_delete_resource_template_clicked(self, result=None):
        if result is None:
            return
        self.H2OService.ResourceTemplates.pop(result['selector'], None)
        self.update_choice_controls()
        self.H2OService.SaveData()

    def on_save_resource_template(self, result=None):
        if result is None:
            return
        template = ResourceTemplate(result)
        self.H2OService.ResourceTemplates.pop(result['selector'], None)
        self.H2OService.ResourceTemplates[template.template_name] = template
        self.update_choice_controls()
        self.H2OService.SaveData()

    def on_create_resource(self, result=None):

        wait = wx.BusyCursor()

        if result is None:
            return

        template = ResourceTemplate(result)
        resource = None
        if template is not None:
            try:
                resource = self.H2OService.CreateResourceFromTemplate(template)
            except HydroShareException as e:

                msg = 'Request could not complete.'
                if e.status_code == 400:
                    msg += "\n\nError 400: Bad request. Please check for errors in the Create Resource form."
                else:
                    msg += "\n\n{}".format(e.message)

                wx.MessageBox(msg, caption='Error', parent=self.panel)

            if resource:

                self._resources[resource.id] = resource
                self.hs_resource_choice.Append(CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, resource.id))
                self.hs_resource_choice.SetStringSelection(CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, resource.id))
                self.populate_resource_fields(resource)

                self.resourceUIController.EnableControls()

        else:
            self.populate_resource_fields(None)

    def on_remove_database_auth(self, result=None):
        if result is None:
            return
        self.H2OService.DatabaseConnections.pop(result['selector'], None)
        self.update_choice_controls()
        self.H2OService.SaveData()

    def on_save_database_auth(self, result=None):
        if result is None:
            return
        connection = OdmDatasetConnection(result)
        self.H2OService.DatabaseConnections.pop(result['selector'], None)
        self.H2OService.DatabaseConnections[connection.name] = connection
        self.update_choice_controls()
        self.H2OService.SaveData()

    def on_test_database_auth(self, result=None):

        wait = wx.BusyCursor()

        if result is None:
            wx.CallAfter(pub.sendMessage, 'db_auth_test_reply', reply='An error occurred, please try again later')
            return

        db_details = OdmDatasetConnection(result)
        if db_details.VerifyConnection():

            wx.CallAfter(pub.sendMessage, 'db_auth_test_reply', reply='Successfully authenticated!')

        else:
            wx.CallAfter(pub.sendMessage, 'db_auth_test_reply', reply='Authentication details were not accepted')

    def on_remove_hydroshare_auth(self, result=None):
        if result is None:
            return
        self.H2OService.HydroShareConnections.pop(result['selector'], None)
        self.update_choice_controls()
        self.H2OService.SaveData()

    def on_save_hydroshare_auth(self, result=None):
        if result is None:
            return
        account = HydroShareAccountDetails(result)
        self.H2OService.HydroShareConnections.pop(result['selector'], None)
        self.H2OService.HydroShareConnections[account.username] = account
        self.update_choice_controls()
        self.H2OService.SaveData()

    def on_test_hydroshare_auth(self, result=None):
        if result is None:
            pub.sendMessage('hs_auth_test_reply', reply='An error occurred, please try again later')
            return

        account = HydroShareAccountDetails(result)
        hydroshare = HydroShareUtility()
        if hydroshare.authenticate(**account.to_dict()):
            pub.sendMessage('hs_auth_test_reply', reply='Successfully authenticated!')
        else:
            pub.sendMessage('hs_auth_test_reply', reply='Authentication details were not accepted')
        print(result)

    def _get_selected_resource(self):  # type: (None) -> HydroShareResource
        resource = None
        re_match = OdmSeriesHelper.RE_RESOURCE_PARSER.match(self.hs_resource_choice.GetStringSelection())
        if re_match is not None and 'title' in re_match.groupdict() and 'id' in re_match.groupdict():
            resource_id = re_match.groupdict()['id']
            if resource_id in self.H2OService.ManagedResources:
                resource = self.H2OService.ManagedResources[resource_id].resource
            else:
                resource = self._resources[resource_id] if resource_id in self._resources else None
        return resource

    def _get_dataset_choices(self):
        if len(self.H2OService.ManagedResources) > 0:
            return ['Create a new dataset'] + list(self.H2OService.ManagedResources.keys())
        else:
            return ['Create a new dataset']

    def _get_hydroshare_choices(self):
        if len(self.H2OService.HydroShareConnections) > 0:
            return ['Select an account'] + [account.username for _, account in self.H2OService.HydroShareConnections.iteritems()]
        else:
            return ['No saved accounts']

    def _get_destination_resource_choices(self):
        if self._resources is None:
            choices = [CHOICE_DEFAULTS.CONNECT_TO_HYDROSHARE]
        else:
            managed_resources = [CHOICE_DEFAULTS.RESOURCE_STR.format(resource.resource.title, hs_id) for hs_id, resource
                                 in self.H2OService.ManagedResources.iteritems() if resource.resource is not None]
            unmanaged_resources = [CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, hs_id) for hs_id, resource in
                                   self._resources.iteritems() if hs_id not in self.H2OService.ManagedResources]
            managed_resources.sort(reverse=self.invert_resource_choices_checkbox.IsChecked())
            unmanaged_resources.sort(reverse=self.invert_resource_choices_checkbox.IsChecked())

            choices = [CHOICE_DEFAULTS.CREATE_NEW_RESOURCE,
                       CHOICE_DEFAULTS.MANAGED_RESOURCES.format(len(managed_resources),
                                                                's' if len(managed_resources) > 1 else '')]

            choices += managed_resources
            choices += [CHOICE_DEFAULTS.UNMANAGED_RESOURCES.format(len(unmanaged_resources),
                                                                   's' if len(unmanaged_resources) > 1 else '')]
            choices += unmanaged_resources
        return choices

    def _set_selected_resource_by_id(self, id):
        selection = ''
        if id in self.H2OService.ManagedResources:
            selection = self.H2OService.ManagedResources[id].resource.title
        elif id in self._resources:
            selection = self._resources[id].title
        if len(selection) > 0:
            self.hs_resource_choice.SetStringSelection(selection)

    def _get_database_choices(self):
        if len(self.H2OService.DatabaseConnections) > 0:
            return ['Select a connection'] + [connection for connection in
                                              self.H2OService.DatabaseConnections.keys()]
        else:
            return ['No saved connections']

    def on_edit_database(self, event):
        result = DatabaseConnectionDialog(self, self.H2OService.DatabaseConnections,
                                          self.database_connection_choice.GetCurrentSelection()).ShowModal()

    def on_edit_hydroshare(self, event):
        result = HydroShareAccountDialog(self, self.H2OService.HydroShareConnections,
                                         self.hydroshare_account_choice.GetCurrentSelection()).ShowModal()

    def set_odm_connection(self, connection):
        self.available_series_grid.Clear()
        self.selected_series_grid.Clear()

        if connection is None:
            self.h2o_series_dict.clear()
            self.odm_series_dict.clear()
            self.odmSeriesUIController.DisableGrids()
            return

        wait = wx.BusyCursor()
        busy = wx.BusyInfo("Loading ODM series from database {}".format(connection.name), parent=self.panel)

        if connection.VerifyConnection():
            self.h2o_series_dict.clear()
            self.odm_series_dict.clear()
            service_manager._current_connection = connection.ToDict()
            series_service = service_manager.get_series_service()

            try:
                series_list = series_service.get_all_series()
            except DBAPIError as e:
                self.on_log_print('Failed to connect to database: {}'.format(e))
                return

            for series in series_list:
                self.h2o_series_dict[series.id] = OdmSeriesHelper.CreateH2OSeriesFromOdmSeries(series)
                self.odm_series_dict[series.id] = series
            self.reset_series_in_grid()

            # Re-enable controls for ODM series UI elements
            self.odmSeriesUIController.Enable()

            # Set the current managed resource's connection to the newly selected database connection
            resource = self._get_selected_resource()
            mnged_resource = self.H2OService.ManagedResources.get(resource.id, None)

            current_db = self.database_connection_choice.GetStringSelection()

            if mnged_resource and mnged_resource.odm_db_name != current_db:
                mnged_resource.odm_db_name = current_db

            self.H2OService.SaveData()

        else:
            self.odmSeriesUIController.DisableGrids()
            self.on_log_print('Unable to authenticate using connection {}'.format(connection.name))

    def __onChangeODMDBConnection(self, data, extra=None, extra1=None):
        self.set_odm_connection(self.H2OService.DatabaseConnections[data])
        self.reset_series_in_grid()

    def _on_select_odm_database(self, event):
        wait = wx.BusyCursor()

        if event.GetSelection() > 0:
            wx.CallAfter(pub.sendMessage, 'odmdb.change_connection',
                         data=self.database_connection_choice.GetStringSelection())
        else:
            print "No selection made"
            self.set_odm_connection(None)
            self.reset_series_in_grid()

    def set_hydroshare_connection(self, account_name):
        wait = wx.BusyCursor()

        busy = wx.BusyInfo("Loading HydroShare account information for {}".format(account_name), parent=self.panel)

        if self.H2OService.ConnectToHydroShareAccount(account_name):
            self._resources = self.H2OService.FetchResources()
        else:
            self._resources = None

        del busy

    def __onConnectHydroshare(self, data, extra1=None, extra2=None):

        # enables UI elements for resource management
        self.resourceUIController.EnableDropdown()

        self.on_log_print('Connecting to HydroShare')
        self.set_hydroshare_connection(data)

        self._update_target_choices()

    def on_select_hydroshare_account(self, event):
        self._resources = None
        self.hydroshare_account_choice.Close()

        if event.GetSelection() != 0:
            account_name = self.hydroshare_account_choice.GetStringSelection()

            if self.selected_account_name != account_name:
                self.selected_account_name = account_name

                wx.CallAfter(pub.sendMessage, 'hydroshare.connect', data=account_name)

    def reset_series_in_grid(self, event=None):
        if self.odm_series_dict is None or len(self.odm_series_dict) == 0:
            self.remove_selected_button.Disable()
            self.add_to_selected_button.Disable()
            return

        self.available_series_grid.Clear()
        self.selected_series_grid.Clear()
        self.available_series_grid.InsertSeriesList(self.odm_series_dict.values())
        self.remove_selected_button.Enable()
        self.add_to_selected_button.Enable()

    def _move_to_selected_series(self, event):
        series_list = [self.odm_series_dict[series_id] for series_id in self.available_series_grid.GetSelectedSeries()]
        self.selected_series_grid.InsertSeriesList(series_list, do_sort=True)
        self.available_series_grid.RemoveSelectedRows()

        mngres = self.get_managed_resource()
        mngres.selected_series = self.get_selected_series()

    def _move_from_selected_series(self, event):
        series_list = [self.odm_series_dict[series_id] for series_id in self.selected_series_grid.GetSelectedSeries()]
        self.available_series_grid.InsertSeriesList(series_list, do_sort=True)
        self.selected_series_grid.RemoveSelectedRows()

        mngres = self.get_managed_resource()
        mngres.selected_series = self.get_selected_series()

    def _get_current_series_ids_from_resource(self, resource):
        if isinstance(resource, H2OManagedResource):
            return [series.SeriesID for series in self.h2o_series_dict.itervalues() if series in
                    resource.selected_series.itervalues()]
        else:
            print('Resource is unmanaged - there are no associated series')
            return []

    def _update_target_choices(self, event=None):
        WxHelper.UpdateChoiceControl(self.hs_resource_choice, self._get_destination_resource_choices())
        if event is not None:
            event.Skip()

    def _remove_from_managed_clicked(self, event):
        resource = self._get_selected_resource()  # type: HydroShareResource

        if resource is None:
            self.on_log_print('Resource not selected.')
            return

        self.H2OService.ManagedResources.pop(resource.id, None)
        self.H2OService.SaveData()
        self._update_target_choices()
        self.hs_resource_choice.SetSelection(event.GetSelection())

    def _delete_files_clicked(self, event):
        resource = self._get_selected_resource()  # type: HydroShareResource
        if resource is None:
            self.on_log_print('Invalid resource selected, cannot delete resource files')
            return

        message = 'Do you want to delete all files currently in HydroShare resource "{}"? This action cannot ' \
                  'be undone.'.format(resource.title)

        confim_dialog = WxHelper.ModalConfirm(self, message, 'Delete HydroShare resource files')

        if confim_dialog.ShowModal() == wx.ID_YES:

            busy_cursor = wx.BusyCursor()
            busy_info = wx.BusyInfo("Deleting resource files...", parent=self.panel)

            self.H2OService.ActiveHydroshare.deleteFilesInResource(resource)

            managed_resource = self.H2OService.ManagedResources.get(resource.id, None)  # type: H2OManagedResource
            if managed_resource:
                managed_resource.associated_files = []
                self.H2OService.SaveData()

        else:
            print 'File delete canceled'

    def _save_managed_clicked(self, event):

        resource = self._get_selected_resource()
        mngd_res = self.H2OService.ManagedResources.get(resource.id, None)

        if not self._verify_dataset_selections():
            if len(self.selected_series_grid.GetSeries()):
                self.on_log_print('"Selected Series" box is empty. Please add one or more series to "Selected Series" to apply changes.')
            else:
                self.on_log_print(
                    'You must add series to "Selected Series" box and click "{}" before you can apply changes.'.format(
                        self.run_script_button.GetLabel()))
            return

        wait = wx.BusyCursor()

        series = {}
        for series_id in self.selected_series_grid.GetSeries():
            if series_id in self.h2o_series_dict:
                series[series_id] = self.h2o_series_dict[series_id]

        # resource = self._get_selected_resource()  # type: HydroShareResource
        if resource is None:
            self.on_log_print('Resource not selected, cannot save changes.')
            return

        # resource.title = self.resource_title_input.Value
        resource.abstract = self.resource_abstract_input.Value
        resource.agency_url = self.resource_agency_website_input.GetLabel()
        resource.award_number = self.resource_award_number_input.GetLabel()
        resource.award_title = self.resource_award_title_input.GetLabel()
        resource.funding_agency = self.resource_funding_agency_input.GetLabel()

        self.save_resource_to_managed_resources(resource, series=series)

    def save_resource_to_managed_resources(self, resource, series=None):  # type: (HydroShareResource, any) -> None
        if series is None:
            series = dict()

        selected_db = self.database_connection_choice.GetStringSelection()
        if selected_db.lower() in ['no saved connections', 'select a connection']:
            selected_db = None

        if resource.id in self.H2OService.ManagedResources:
            managed = self.H2OService.ManagedResources[resource.id]
            managed.selected_series = series
            managed.single_file = not self.chunk_by_series_checkbox.IsChecked()
            managed.chunk_years = self.chunk_by_year_checkbox.IsChecked()
            managed.resource_id = resource.id

            if selected_db is not None:
                managed.odm_db_name = selected_db

        else:
            managed = H2OManagedResource(resource=resource,
                                         odm_series=series,
                                         resource_id=resource.id,
                                         hs_account_name=self.hydroshare_account_choice.GetStringSelection(),
                                         odm_db_name=selected_db,
                                         single_file=not self.chunk_by_series_checkbox.IsChecked(),
                                         chunk_years=self.chunk_by_year_checkbox.Value,
                                         associated_files=[])

            self.H2OService.ManagedResources[resource.id] = managed

        self.H2OService.SaveData()

        self.hs_resource_choice.SetStringSelection(HS_RES_STR(resource))

        wx.CallAfter(self._update_target_choices)

    def _verify_dataset_selections(self):
        if len(self.selected_series_grid.GetSeries()) == 0:
            self.on_log_print('Invalid options - please select the ODM series you would like to add to the dataset')
        elif self.hydroshare_account_choice.GetSelection() == 0:
            self.on_log_print('Invalid options - please select a HydroShare account to use')
        else:
            return True
        return False

    def on_edit_resource_templates_clicked(self, event, create_resource=False):
        return HydroShareResourceTemplateDialog(self, self.H2OService.ResourceTemplates,
                                                create_selected=create_resource).ShowModal()

    def on_run_script_clicked(self, event):
        """
        Starts the file upload process given the user has selected series to be uploaded for the
        currently selected resource
        """
        self._save_managed_clicked(event)

        data = self.H2OService.LoadData()
        managed_resources = data.get('managed_resources', {})
        mngd_resource = managed_resources.get(self._get_selected_resource().id, None)

        if mngd_resource and len(mngd_resource.selected_series):
            # Only run if the managed resource has selected series
            self.on_log_print('Running script')
            self.run_script_button.Enable(enable=False)
            self.stop_script_button.Enable(enable=True)

            try:
                self.H2OService.StartOperations()
            except Exception as e:
                self.on_log_print(e.message)

    def on_stop_script_clicked(self, event):
        self.on_log_print('Stopping the script... this may take a while.')
        self.status_gauge.Pulse()
        self.H2OService.StopActions()

    def __onChangeResource(self, data, extra1=None, extra2=None):
        self._change_resource(data)

    def _on_select_resource(self, event):
        wx.CallAfter(pub.sendMessage, 'resource.change', data=event)

    def __onGetResourceComplete(self, resource, extra1=None, extra2=None):
        """
        Just in case anyone else comes looking here... 'resource' might be a HydroShareResource or an
        H2OManagedResource. Your guess is as good as mine.
        """
        managed_resource = None
        if isinstance(resource, H2OManagedResource):
            managed_resource = resource
            resource = resource.resource

        # 'self.clean_resource' is used to keep track the state of the 'resource'
        self.clean_resource = copy.copy(resource)

        self.odmSeriesUIController.EnableDropdown()
        self.odmSeriesUIController.EnableButtons()

        self.populate_resource_fields(resource)
        self.reset_series_in_grid()
        self.save_resource_to_managed_resources(resource)

        self.on_log_print('Fetching information for resource {}'.format(resource.title))

        if managed_resource is not None:

            if managed_resource.odm_db_name in self.H2OService.DatabaseConnections:
                self.database_connection_choice.SetStringSelection(managed_resource.odm_db_name)
                self.set_odm_connection(self.H2OService.DatabaseConnections[managed_resource.odm_db_name])
            else:

                if managed_resource.odm_db_name is not None:
                    self.on_log_print('Error loading ODM series: Unknown connection "{}"'.format(managed_resource.odm_db_name))

                return

            self.reset_series_in_grid()
            matches = self._get_current_series_ids_from_resource(managed_resource)
            for series in self.odm_series_dict.itervalues():
                if series.id in matches:
                    self.selected_series_grid.AppendSeries(series)
                else:
                    self.available_series_grid.AppendSeries(series)

            self.chunk_by_series_checkbox.SetValue(wx.CHK_CHECKED if not managed_resource.single_file else wx.CHK_UNCHECKED)
            self.chunk_by_year_checkbox.Value = managed_resource.chunk_years

    def _change_resource(self, event):

        if self.hs_resource_choice.GetStringSelection() == CHOICE_DEFAULTS.CREATE_NEW_RESOURCE:

            # disable buttons for resource management
            # self.resourceUIController.DisableControls()
            self.resourceUIController.DisableButtons()
            self.odmSeriesUIController.Disable()

            result = self.on_edit_resource_templates_clicked(None, create_resource=True)

            if result == 0:
                self.populate_resource_fields(None)
                self.reset_series_in_grid()

        else:

            # Enable buttons for resource management
            self.resourceUIController.EnableControls()

            resource = self._get_selected_resource()  # type: H2OManagedResource | HydroShareResource

            if resource is None:
                print('No resource was selected')
                self.reset_series_in_grid()
                return
            elif isinstance(resource, HydroShareResource):
                if resource.id in self.H2OService.ManagedResources:
                    resource = self.H2OService.ManagedResources[resource.id]

            def __get_resource(q, managed_resource):
                resource_ = managed_resource.resource if hasattr(managed_resource, 'resource') else managed_resource
                self.H2OService.ActiveHydroshare.getMetadataForResource(resource_)

                self.H2OService.ActiveHydroshare.requestAccessRules(resource_)

                wx.CallAfter(pub.sendMessage, 'onCompleteGetResource', resource=managed_resource)

            thread = threading.Thread(target=__get_resource, args=(Queue(), resource))
            thread.setDaemon(True)
            thread.start()

    def populate_resource_fields(self, resource):  # type: (HydroShareResource) -> None
        if resource is None:
            for label in self.resourceUIController.inputs:
                label.SetLabel('')
        else:
            self.resource_abstract_input.Value = resource.abstract
            self.resource_agency_website_input.SetLabel(' {}'.format(resource.agency_url))
            self.resource_award_number_input.SetLabel(' {}'.format(resource.award_number))
            self.resource_award_title_input.SetLabel(' {}'.format(resource.award_title))
            self.resource_funding_agency_input.SetLabel(' {}'.format(resource.funding_agency))
            self.is_public_checkbox.SetValue(resource.public)
            self.is_private_checkbox.SetValue(not resource.public)
            self.keywords_input.SetValue(', '.join(resource.subjects))

    def _sort_resource_choices(self, event):
        WxHelper.UpdateChoiceControl(self.hs_resource_choice, self._get_destination_resource_choices())

    def on_operations_stopped(self, message):
        self.run_script_button.Enable(enable=True)
        self.stop_script_button.Enable(enable=False)
        self.on_log_print(message)
        self.status_gauge.SetValue(0)

    def on_datasets_generated(self, completed, total):
        self.on_log_print('Generation stopped, created files for {}/{} HydroShare resources'.format(completed, total))
        self.status_gauge.SetValue(0)

    def on_file_generation_failed(self, filename, message):
        print 'Files failed to generate'
        self.on_log_print('File "{}" could not be created: {}'.format(filename, message))

    def update_status_gauge_datasets(self, resource="None", completed=None, started=None):
        message = ' file generation for resource "{}"'.format(resource)
        state = 'None'
        if completed is not None:
            state = 'Finished'
            self.status_gauge.SetValue(completed)
        elif started is not None:
            state = 'Starting'
            self.status_gauge.SetValue(started)
        else:
            message = 'File generation completed'
            self.status_gauge.SetValue(0)
        self.on_log_print(state + message)

    def update_status_gauge_uploads(self, resource="None", completed=None, started=None):
        message = ' file uploads to resource "{}"'.format(resource)
        state = 'None'
        if completed is not None:
            state = 'Finished'
            self.status_gauge.SetValue(completed)
        elif started is not None:
            state = 'Starting'
            self.status_gauge.SetValue(started)
        else:
            message = 'File uploads to HydroShare completed'
            self.status_gauge.SetValue(0)
        self.on_log_print(state + message)

    def create_gui_label(self, label, font=None, style=7):
        if font is None:
            font = self.MONOSPACE
        return WxHelper.GetLabel(self.panel, label, font, style=style)

    def _build_main_window(self):
        ######################################
        #   Setup sizers and panels          #
        ######################################
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        hs_account_sizer = WxHelper.GetGridBagSizer()
        selection_label_sizer = WxHelper.GetGridBagSizer()
        resource_sizer = WxHelper.GetGridBagSizer()
        odm_series_sizer = WxHelper.GetGridBagSizer()
        action_status_sizer = WxHelper.GetGridBagSizer()

        ######################################
        #   Build connection details sizer   #
        ######################################
        edit_hydroshare_button = WxHelper.GetButton(self, self.panel, u'Edit...', on_click=self.on_edit_hydroshare)
        self.hydroshare_account_choice = WxHelper.GetChoice(self,
                                                            self.panel,
                                                            self._get_hydroshare_choices(),
                                                            on_change=self.on_select_hydroshare_account,
                                                            size_x=310,
                                                            size_y=23,
                                                            font=self.MONOSPACE)

        hs_account_sizer.Add(self.create_gui_label(u'Select a HydroShare account to continue'), pos=(0, 0), span=(1, 4),
                             flag=ALIGN.LEFT)
        hs_account_sizer.Add(self.hydroshare_account_choice, pos=(1, 0), span=(1, 4), flag=ALIGN.LEFT)
        hs_account_sizer.Add(edit_hydroshare_button, pos=(1, 4), span=(1, 1), flag=ALIGN.LEFT)

        """
        Sizer for HydroShare resource metadata
        """
        self._layout_resource_panel(sizer=resource_sizer)

        """
        ODM Series selection sizer
        """
        # Buttons (and bitmaps) to add or remove series from the active dataset
        left_arrow = WxHelper.GetBitmap(APP_SETTINGS.PROJECT_DIR + '/GuiComponents/previous_icon.png', 20, 20)
        right_arrow = WxHelper.GetBitmap(APP_SETTINGS.PROJECT_DIR + '/GuiComponents/next_icon.png', 20, 20)

        self.add_to_selected_button = wx.BitmapButton(self.panel, wx.ID_ANY, right_arrow, wx.DefaultPosition,
                                                      wx.DefaultSize)
        self.Bind(wx.EVT_BUTTON, self._move_to_selected_series, self.add_to_selected_button)

        self.remove_selected_button = wx.BitmapButton(self.panel, wx.ID_ANY, left_arrow, wx.DefaultPosition,
                                                      wx.DefaultSize)
        self.Bind(wx.EVT_BUTTON, self._move_from_selected_series, self.remove_selected_button)

        self.remove_selected_button.Disable()
        self.add_to_selected_button.Disable()

        # Database connection items
        row = 0
        span = 3
        edit_database_button = WxHelper.GetButton(self, self.panel, u'Edit...', on_click=self.on_edit_database)
        self.database_connection_choice = WxHelper.GetChoice(self, self.panel, self._get_database_choices(),
                                                             on_change=self._on_select_odm_database, font=self.MONOSPACE)
        odm_series_sizer.Add(self.create_gui_label(u'Select a database connection'), pos=(row, 0), span=(1, span),
                             flag=wx.ALIGN_LEFT)
        odm_series_sizer.Add(self.database_connection_choice, pos=(row + 1, 0), span=(1, span), flag=ALIGN.LEFT)
        odm_series_sizer.Add(edit_database_button, pos=(row + 1, span), span=(1, 1))

        # File chunking options
        text_flags = wx.ALIGN_CENTER | wx.ALIGN_CENTER_VERTICAL
        self.chunk_by_year_checkbox = WxHelper.GetCheckBox(self, self.panel, u'Group series by year')
        self.chunk_by_year_checkbox.SetToolTip(wx.ToolTip(tip="Select to split series into multiple \n"
                                                              "files, separated by year."))

        self.chunk_by_series_checkbox = WxHelper.GetCheckBox(self, self.panel, u'One series per file')
        self.chunk_by_series_checkbox.SetToolTip(wx.ToolTip("Select to split series into individual\n"
                                                            "files (by default, series are uploaded\n"
                                                            "as a single file)."))

        odm_series_sizer.Add(self.create_gui_label(u'File options:    '), pos=(row + 1, 6), span=(1, 1), flag=text_flags)
        odm_series_sizer.Add(self.chunk_by_series_checkbox, pos=(row + 1, 7), span=(1, 1), flag=text_flags)
        odm_series_sizer.Add(self.chunk_by_year_checkbox, pos=(row + 1, 8), span=(1, 1), flag=text_flags)

        # Series selection controls
        odm_series_sizer.Add(self.create_gui_label(u'Available Series', font=self.MONOSPACE.Bold()), pos=(row + 2, 0),
                             span=(1, 4),
                             flag=wx.ALIGN_CENTER)
        odm_series_sizer.Add(self.create_gui_label(u'Selected Series', font=self.MONOSPACE.Bold()), pos=(row + 2, 5),
                             span=(1, 4),
                             flag=wx.ALIGN_CENTER)

        grid_x_size = 500
        grid_y_size = 175
        self.selected_series_grid = WxHelper.SeriesGrid(self, self.panel, size=wx.Size(grid_x_size, grid_y_size))
        self.available_series_grid = WxHelper.SeriesGrid(self, self.panel, size=wx.Size(grid_x_size, grid_y_size))

        odm_series_sizer.Add(self.available_series_grid, pos=(row + 3, 0), span=(6, 4), flag=PADDING.ALL)
        odm_series_sizer.Add(self.selected_series_grid, pos=(row + 3, 5), span=(6, 4), flag=PADDING.ALL)

        odm_series_sizer.Add(self.add_to_selected_button, pos=(row + 4, 4), span=(1, 1), flag=wx.ALIGN_CENTER)
        odm_series_sizer.Add(self.remove_selected_button, pos=(row + 6, 4), span=(1, 1), flag=wx.ALIGN_CENTER)

        """
        Build action sizer and logging box
        """
        self.run_script_button = WxHelper.GetButton(self, self.panel, "Upload Series", self.on_run_script_clicked)
        self.stop_script_button = WxHelper.GetButton(self, self.panel, "Cancel", self.on_stop_script_clicked)
        self.stop_script_button.Enable(enable=False)

        self.status_gauge = wx.Gauge(self.panel, wx.ID_ANY, 100, wx.DefaultPosition, wx.DefaultSize, wx.GA_HORIZONTAL)
        self.status_gauge.SetValue(0)

        self.log_message_listbox = WxHelper.GetListBox(self, self.panel, [], size_x=920, size_y=100,
                                                       font=self.MONOSPACE,
                                                       on_right_click=self.on_right_click_log_output,
                                                       style=wx.HSCROLL | wx.TE_RICH)


        self.clear_console_button = WxHelper.GetButton(self, self.panel, "Clear Console",
                                                       lambda ev: self.log_message_listbox.Clear())

        action_status_sizer.Add(self.status_gauge, pos=(0, 0), span=(1, 8), flag=ALIGN.CENTER)
        action_status_sizer.Add(self.stop_script_button, pos=(0, 9), span=(1, 1), flag=ALIGN.CENTER)
        action_status_sizer.Add(self.run_script_button, pos=(0, 8), span=(1, 1), flag=ALIGN.CENTER)
        action_status_sizer.Add(self.log_message_listbox, pos=(1, 0), span=(2, 10), flag=ALIGN.CENTER)
        action_status_sizer.Add(self.clear_console_button, pos=(3, 0), span=(1, 1), flag=ALIGN.CENTER)

        self.odmSeriesUIController = UIController(buttons=[
            edit_database_button,
            self.add_to_selected_button,
            self.remove_selected_button,
            self.run_script_button,
            self.stop_script_button
        ],
            dropdowns=[self.database_connection_choice],
            checkboxes=[self.chunk_by_series_checkbox, self.chunk_by_year_checkbox],
            grids=[self.selected_series_grid, self.available_series_grid]
        )

        """
        Build menu bar and setup callbacks
        """
        self.add_grid_bag_to_main_sizer(main_sizer, hs_account_sizer, expand=False, flags=PADDING.HORIZONTAL)
        self.add_line_to_main_sizer(main_sizer, flags=PADDING.ALL)
        self.add_grid_bag_to_main_sizer(main_sizer, selection_label_sizer, flags=PADDING.HORIZONTAL)
        self.add_grid_bag_to_main_sizer(main_sizer, resource_sizer, flags=PADDING.HORIZONTAL)
        self.add_grid_bag_to_main_sizer(main_sizer, odm_series_sizer, flags=ALIGN.CENTER | PADDING.ALL | wx.EXPAND)
        self.add_line_to_main_sizer(main_sizer, flags=PADDING.ALL)
        self.add_grid_bag_to_main_sizer(main_sizer, action_status_sizer, flags=WxHelper.GetFlags(top=False))

        """
        Build menu bar and setup callbacks
        """
        file_menu = wx.Menu()

        WxHelper.AddNewMenuItem(self, file_menu, 'ODM Connections...', self.on_edit_database)
        WxHelper.AddNewMenuItem(self, file_menu, 'HydroShare Accounts...', self.on_edit_hydroshare)
        WxHelper.AddNewMenuItem(self, file_menu, 'Resource Templates...', self.on_edit_resource_templates_clicked)

        file_menu.AppendSeparator()
        WxHelper.AddNewMenuItem(self, file_menu, 'Quit', self.on_quit_clicked)
        # file_menu.Append(wx.ID_EXIT, 'Quit', 'Quit application')

        menuBar = wx.MenuBar()
        menuBar.Append(file_menu, "&File")  # Adding the "filemenu" to the MenuBar
        self.SetMenuBar(menuBar)  # Adding the MenuBar to the Frame content.
        return main_sizer

    def _layout_resource_panel(self, sizer):
        """
        Lays out the UI elements dealing with resource management
        """
        input_font = self.MONOSPACE
        label_font = self.MONOSPACE
        flags = wx.GROW
        text_flags = wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL
        border_style = wx.BORDER_THEME

        gap = (16, 16)
        leftSizer = wx.GridBagSizer(*gap)
        rightSizer = wx.GridBagSizer(*gap)

        self.hs_resource_choice = WxHelper.GetChoice(self.panel, self.panel, self._get_destination_resource_choices(),
                                                     on_change=self._on_select_resource,
                                                     font=self.MONOSPACE)

        self.invert_resource_choices_checkbox = WxHelper.GetCheckBox(self, self.panel, 'Sort A-Z', checked=True,
                                                                     on_change=self._sort_resource_choices)

        sizer.Add(self.create_gui_label('Select a resource'), pos=(0, 0), span=(1, 1), flag=ALIGN.CENTER)
        sizer.Add(self.hs_resource_choice, pos=(1, 0), span=(1, 9), flag=wx.GROW)
        sizer.Add(self.invert_resource_choices_checkbox, pos=(1, 9), span=(1, 1),
                  flag=wx.ALIGN_LEFT|wx.ALIGN_CENTER_VERTICAL)

        resource_row = 2
        sizer.Add(leftSizer, pos=(resource_row, 0), span=(1, 3), flag=wx.EXPAND)
        sizer.Add(rightSizer, pos=(resource_row, 3), span=(1, 6), flag=wx.EXPAND)

        """
        Elements on the RIGHT hand side (and below the resource choice list)
        """
        self.resource_abstract_label = self.create_gui_label(u'Abstract', font=label_font)
        self.resource_abstract_input = WxHelper.GetTextInput(self.panel, '', wrap_text=True,
                                                             style=wx.BORDER_STATIC | wx.TE_READONLY)
        # added a background color to help make the abstract appear uneditable
        self.resource_abstract_input.SetBackgroundColour(wx.Colour(245, 245, 245))

        # These two lines of code are to make the abstract appear non-editable (because it isn't).
        # Binding to wx.EVT_SET_FOCUS makes it so the cursor doesn't appear when clicking inside
        # the textctrl window. Calling SetCursor(...) makes the cursor appear as the default arrow
        # instead of the cursor you'd normally see when hovering over an editable window.
        self.resource_abstract_input.Bind(wx.EVT_SET_FOCUS,
                                          lambda ev: self.resource_abstract_input.ShowNativeCaret(False))
        self.resource_abstract_input.SetCursor(wx.Cursor(wx.CURSOR_ARROW))

        rightSizer.Add(self.resource_abstract_label, pos=(0, 0), span=(1, 1))
        rightSizer.Add(self.resource_abstract_input, pos=(0, 1), span=(1, 4), flag=wx.GROW | wx.LEFT)
        rightSizer.AddGrowableCol(1)
        rightSizer.AddGrowableRow(0)

        """
        Elements on the LEFT hand side (also below the resource choice list)
        """
        # These all have the word 'input' in them, but they're actually labels.
        # "Why?", do you ask? Because they used to be inputs (for who knows what
        # reason), but  guess what? Now they're labels!
        input_style = wx.TE_READONLY | wx.BORDER_STATIC
        self.resource_funding_agency_input = WxHelper.GetLabel(self.panel, '', style=input_style)
        self.resource_agency_website_input = WxHelper.GetLabel(self.panel, '', style=input_style)
        self.resource_award_title_input = WxHelper.GetLabel(self.panel, '', style=input_style)
        self.resource_award_number_input = WxHelper.GetLabel(self.panel, '', style=input_style)

        # checkboxes to toggle between 'private' and 'public'
        self.is_public_checkbox = wx.CheckBox(self.panel, wx.ID_ANY, 'Public', wx.Point(-1, -1), wx.DefaultSize, 0)
        self.is_private_checkbox = wx.CheckBox(self.panel, wx.ID_ANY, 'Private', wx.Point(-1, -1), wx.DefaultSize, 0)

        # add event binding to the checkboxes
        self.Bind(wx.EVT_CHECKBOX, self.on_check_is_public, self.is_public_checkbox)
        self.Bind(wx.EVT_CHECKBOX, self.on_check_is_private, self.is_private_checkbox)

        # create input for keywords
        self.keywords_input = WxHelper.GetTextInput(self.panel, '')
        self.Bind(wx.EVT_TEXT, self.on_change_keywords_input, self.keywords_input)

        # Labels
        fundingAgencyLabel = self.create_gui_label('Funding Agency')
        agencyWebsiteLabel = self.create_gui_label('Agency Website')
        awardTitleLabel = self.create_gui_label('Award Title')
        awardNumberLabel = self.create_gui_label('Award Number')
        visibilityLabel = self.create_gui_label('Sharing Status')
        keywordsLabel = self.create_gui_label('Keywords')

        # Keywords information icon with help text
        # bmp = WxHelper.GetBitmap(APP_SETTINGS.PROJECT_DIR + '/GuiComponents/info-512.png', 20, 20)
        # keywordIcon = wx.StaticBitmap(self.panel, wx.ID_ANY, bmp)

        keywordIconToolTip = wx.ToolTip('Enter keywords as a comma seperated\n'
                                        'list (i.e. "Keyword 1, Keyword 2", etc.)')
        keywordIconToolTip.SetDelay(0)
        # keywordIcon.SetToolTip(keywordIconToolTip)
        self.keywords_input.SetToolTip(keywordIconToolTip)

        resourceManagementLabel = self.create_gui_label('Resource Management:')

        leftSizer.Add(fundingAgencyLabel, pos=(0, 0), span=(1, 1), flag=text_flags)
        leftSizer.Add(self.resource_funding_agency_input, pos=(0, 1), span=(1, 2), flag=wx.GROW)

        leftSizer.Add(agencyWebsiteLabel, pos=(1, 0), span=(1, 1), flag=text_flags)
        leftSizer.Add(self.resource_agency_website_input, pos=(1, 1), span=(1, 2), flag=wx.GROW)

        leftSizer.Add(awardTitleLabel, pos=(2, 0), span=(1, 1), flag=text_flags)
        leftSizer.Add(self.resource_award_title_input, pos=(2, 1), span=(1, 2), flag=wx.GROW)

        leftSizer.Add(awardNumberLabel, pos=(3, 0), span=(1, 1), flag=text_flags)
        leftSizer.Add(self.resource_award_number_input, pos=(3, 1), span=(1, 2), flag=wx.GROW)

        leftSizer.Add(visibilityLabel, pos=(4, 0), span=(1, 1), flag=text_flags)
        leftSizer.Add(self.is_public_checkbox, pos=(4, 1), span=(1, 1), flag=wx.ALIGN_LEFT)
        leftSizer.Add(self.is_private_checkbox, pos=(4, 2), span=(1, 1), flag=wx.ALIGN_LEFT)

        leftSizer.Add(keywordsLabel, pos=(5, 0), span=(1, 1), flag=text_flags)
        leftSizer.Add(self.keywords_input, pos=(5, 1), span=(1, 2), flag=wx.GROW)
        # leftSizer.Add(keywordIcon, pos=(5, 3), span=(1, 1))

        leftSizer.AddGrowableCol(2)

        """
        Action Buttons
        """
        self.remove_files_button = WxHelper.GetButton(self, self.panel, "Delete Resource Files",
                                                      self._delete_files_clicked)
        self.update_resource = WxHelper.GetButton(self, self.panel, "Update Resource", self.on_click_update)

        sizer.Add(resourceManagementLabel, pos=(3, 0), span=(1, 1))
        sizer.Add(self.remove_files_button, pos=(4, 0), span=(1, 1), flag=wx.ALIGN_CENTER)
        # sizer.Add(self.set_resource_visibility_button, pos=(4, 1), span=(1, 1), flag=wx.ALIGN_CENTER)
        sizer.Add(self.update_resource, pos=(4, 1), span=(1, 1), flag=wx.ALIGN_CENTER)

        self.resourceUIController = UIController(inputs=[self.resource_abstract_input, self.keywords_input],
                                                 buttons=[self.remove_files_button,
                                                          self.update_resource],
                                                 dropdowns=[self.hs_resource_choice],
                                                 checkboxes=[self.is_public_checkbox, self.is_private_checkbox])

    def on_click_set_visibility(self, event):
        wait = wx.BusyCursor()

        resource = self._get_selected_resource()  # type: HydroShareResource
        if resource is not None:
            self.H2OService.ActiveHydroshare.makePublic(resource, public=not resource.public)
            wx.CallAfter(pub.sendMessage, 'resource.set_visibility')

    def __onSetVisibility(self):
        self.populate_resource_fields(self._get_selected_resource())

    def on_check_is_public(self, event):
        checked = self.is_public_checkbox
        self.is_private_checkbox.SetValue(not checked)
        self.check_if_dirty()

    def on_check_is_private(self, event):
        checked = self.is_private_checkbox
        self.is_public_checkbox.SetValue(not checked)
        self.check_if_dirty()

    def on_change_keywords_input(self, event):
        self.check_if_dirty()

    def check_if_dirty(self):
        is_dirty = False

        # First check if the sharing status has changed
        is_dirty = self.is_dirty_sharing_status()

        # Second check if the text in 'keywords_input' has changed. If is_dirty is True,
        # there is no need to check.
        if not is_dirty:
            is_dirty = self.is_dirty_keywords()

        # Enable/disable the Update button
        self.update_resource.Enable(is_dirty)

    def is_dirty_sharing_status(self):

        if self.clean_resource is None:
            # The resource has not been created, so carry on!
            return True

        pub_to_priv = self.clean_resource.public and self.is_public_checkbox.GetValue()
        priv_to_pub = not self.clean_resource.public and self.is_private_checkbox.GetValue()
        return not (pub_to_priv or priv_to_pub)

    def is_dirty_keywords(self):
        """
        :return: True if keywords are dirty
        """
        curr_keywords = self.keywords_input.GetValue()

        if not len(curr_keywords):
            curr_keywords = []
        else:
            curr_keywords = curr_keywords.split(',')

        dirty_keywords = sorted(map(lambda x: x.strip(), curr_keywords))
        clean_keywords = sorted(map(lambda x: x.strip(), self.clean_resource.subjects))

        return dirty_keywords != clean_keywords

    def on_click_update(self, event):
        """
        Handler for updating metadata elements for the currently selected resource.

        !!NOTE: This only updates the resource's Sharing Status and Keywords/Subjects.
        """

        # Resources can only be made public if they have Subjects/Keywords. If this is not the
        # case, warn the user before updating.
        if self.is_public_checkbox.GetValue() and self.keywords_input.GetValue() == '':

            message = 'Keywords are required to make the resource public.\n\n'\
                      + 'Please add keywords or make the resource private.'
            self.Warn(message)

        else:

            wait = wx.BusyCursor()

            resource = self._get_selected_resource()
            if resource is not None:

                # Keywords must be updated first, since the sharing status may have also changed
                if self.is_dirty_keywords():
                    res = self.H2OService.ActiveHydroshare.updateKeywords(resource, self.keywords_input.GetValue().split(','))

                    resource.subjects = []

                    for value in res.get('subjects', []):
                        if isinstance(value, dict):
                            resource.subjects.append(value.get('value'))
                        elif isinstance(value, str):
                            resource.subjects.append(value)

                # After the keywords have updated, update the sharing status if needed
                if self.is_dirty_sharing_status():
                    self.H2OService.ActiveHydroshare.makePublic(resource, public=not resource.public)

                self.clean_resource = copy.copy(resource)

                wx.CallAfter(pub.sendMessage, 'resource.update')

    def __onUpdateResource(self):
        resource = self._get_selected_resource()
        self.populate_resource_fields(resource)
        r_url = 'https://www.hydroshare.org/resource/{}/'.format(resource.id)
        self.on_log_print('Resource successfully updated at {url}'.format(url=r_url))

    def Warn(self, message, caption="Warning"):
        dialog = wx.MessageDialog(self.panel, message, caption, wx.OK | wx.ICON_WARNING)
        dialog.ShowModal()
        dialog.Destroy()

    def on_right_click_log_output(self, event):
        message = 'Print only important log messages' if APP_SETTINGS.VERBOSE else 'Print all log messages'
        menu = wx.Menu()
        WxHelper.AddNewMenuItem(self, menu, message, on_click=self.toggle_verbose_log)
        self.PopupMenu(menu)

    def on_quit_clicked(self, event):
        exit(0)

    def toggle_verbose_log(self, event):
        APP_SETTINGS.VERBOSE = not APP_SETTINGS.VERBOSE

    def add_line_to_main_sizer(self, parent, border=10, flags=wx.ALL | wx.EXPAND):
        parent.Add(wx.StaticLine(self.panel), 0, flag=flags, border=border)

    def add_grid_bag_to_main_sizer(self, parent, child, expand=True, border=12, flags=None):
        """
        :type parent: wx.BoxSizer
        :type child: wx.GridBagSizer
        :type expand: bool
        :type border: int
        """
        if expand:
            for x in range(0, child.GetCols()):
                child.AddGrowableCol(x)
            for y in range(0, child.GetRows()):
                child.AddGrowableRow(y)
        if flags is None:
            flags = WxHelper.GetFlags()
        parent.Add(child, flag=flags, border=border)

    def get_managed_resource(self):  # type: (None) -> H2OManagedResource
        """
        :return: The currently selected managed resource
        """
        res = self._get_selected_resource()
        return self.H2OService.ManagedResources.get(res.id, None)  # type: H2OManagedResource

    def get_selected_series(self):  # type: (None) -> dict(H2OSeries)
        series = {}
        for series_id in self.selected_series_grid.GetSeries():
            if series_id in self.h2o_series_dict:
                series[series_id] = self.h2o_series_dict[series_id]
        return series