from functools import partial

import datetime

import wx
import wx.dataview
import wx.grid

from wx.lib.pubsub import pub
# from pubsub import pub
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

service_manager = ServiceManager()


class CHOICE_DEFAULTS:
    NEW_TEMPLATE_CHOICE = 'Create a new resource template'
    SELECT_TEMPLATE_CHOICE = 'Select a resource template'
    RESOURCE_STR = '{:<130} (ID {})'
    CREATE_NEW_RESOURCE = 'Create a new resource'
    MANAGED_RESOURCES = '      -- {} resources managed by H2O --'
    UNMANAGED_RESOURCES = '      -- {} resources not managed by H2O --'
    CONNECT_TO_HYDROSHARE = 'Please connect to a HydroShare account'

HS_RES_STR = lambda resource: CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, resource.id)
H2O_RES_STR = lambda resource: CHOICE_DEFAULTS.RESOURCE_STR.format(resource.resource.title, resource.id)


# noinspection PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,
# PyUnusedLocal
# noinspection PyUnusedLocal,PyUnusedLocal,PyUnusedLocal
class VisualH2OWindow(wx.Frame):
    def __init__(self, parent, id, title):
        ###########################################
        # Declare/populate variables, wx objects  #
        ###########################################
        APP_SETTINGS.GUI_MODE = True
        self.ORIGINAL_SIZE = (940, 860)
        self.MONOSPACE = wx.Font(9, 75, 90, 90, False, "Inconsolata")
        self._setup_internal_subscriptions()
        h2o_subs = self._setup_h2o_subscriptions()
        self.H2OService = H2OService(subscriptions=h2o_subs)

        self.odm_series_dict = {}  # type: dict[str, Series]
        self.h2o_series_dict = {}  # type: dict[str, H2OSeries]

        self._resources = None  # type: dict[str, HydroShareResource]

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

    def _setup_internal_subscriptions(self):
        SUBSCRIPTIONS = [
            (self.on_delete_resource_template_clicked, 'hs_resource_remove'),
            (self.on_save_resource_template, 'hs_resource_save'),
            (self.on_create_resource, 'hs_resource_create'),
            (self.on_save_hydroshare_auth, 'hs_auth_save'),
            (self.on_test_hydroshare_auth, 'hs_auth_test'),
            (self.on_remove_hydroshare_auth, 'hs_auth_remove'),
            (self.on_save_database_auth, 'db_auth_save'),
            (self.on_test_database_auth, 'db_auth_test'),
            (self.on_remove_database_auth, 'db_auth_remove')
        ]
        for sub_tuple in SUBSCRIPTIONS:
            pub.subscribe(sub_tuple[0], sub_tuple[1])

    def _setup_h2o_subscriptions(self):
        H2O_SUBSCRIPTIONS = [
            (self.on_log_print, 'logger'),
            (self.on_operations_stopped, 'Operations_Stopped'),
            (self.on_datasets_generated, 'Datasets_Completed'),
            (self.on_file_generation_failed, 'File_Failed'),
            (self.update_status_gauge_datasets, 'Dataset_Started'),
            (self.update_status_gauge_datasets, 'Dataset_Generated'),
            (self.update_status_gauge_uploads, 'Files_Uploaded'),
            (self.update_status_gauge_uploads, 'Uploads_Completed')
        ]

        for sub_tuple in H2O_SUBSCRIPTIONS:
            pub.subscribe(sub_tuple[0], sub_tuple[1])
        return [sub_tuple[1] for sub_tuple in H2O_SUBSCRIPTIONS]

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
        # if progress is not None:
        #     self.status_gauge = progress if 100 >= progress >= 0 else progress % 100
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
        if result is None:
            return
        template = ResourceTemplate(result)
        if template is not None:
            resource = self.H2OService.CreateResourceFromTemplate(template)
            self._resources[resource.id] = resource
            self.hs_resource_choice.Append(CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, resource.id))
            self.hs_resource_choice.SetStringSelection(CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, resource.id))
            self.populate_resource_fields(resource)
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
        if result is None:
            pub.sendMessage('db_auth_test_reply', reply='An error occurred, please try again later')
            return
        db_details = OdmDatasetConnection(result)
        if db_details.VerifyConnection():
            pub.sendMessage('db_auth_test_reply', reply='Successfully authenticated!')
        else:
            pub.sendMessage('db_auth_test_reply', reply='Authentication details were not accepted')

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
        self.H2OService.HydroShareConnections[account.name] = account
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
        print result

    def _get_selected_resource(self):
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
            return ['Select an account'] + [account for account in self.H2OService.HydroShareConnections.keys()]
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
                       CHOICE_DEFAULTS.MANAGED_RESOURCES.format(len(managed_resources))]
            choices += managed_resources + [CHOICE_DEFAULTS.UNMANAGED_RESOURCES.format(len(unmanaged_resources))]
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

        busy = wx.BusyInfo("Loading ODM series from database {}".format(connection.name))

        if connection.VerifyConnection():
            self.h2o_series_dict.clear()
            self.odm_series_dict.clear()
            service_manager._current_connection = connection.ToDict()
            series_service = service_manager.get_series_service()
            series_list = series_service.get_all_series()
            for series in series_list:
                self.h2o_series_dict[series.id] = OdmSeriesHelper.CreateH2OSeriesFromOdmSeries(series)
                self.odm_series_dict[series.id] = series
            self.reset_series_in_grid()
        else:
            self.on_log_print('Unable to authenticate using connection {}'.format(connection.name))

    def set_hydroshare_connection(self, account_name):
        busy = wx.BusyInfo("Loading HydroShare account information for {}".format(account_name))
        if self.H2OService.ConnectToHydroShareAccount(account_name):
            self._resources = self.H2OService.FetchResources()
        else:
            self._resources = None

    def on_database_chosen(self, event):
        if event.GetSelection() > 0:
            selection_string = self.database_connection_choice.GetStringSelection()
            self.set_odm_connection(self.H2OService.DatabaseConnections[selection_string])
        else:
            print "No selection made"
            self.set_odm_connection(None)
        self.reset_series_in_grid()

    def on_hydroshare_account_chosen(self, event):
        self._resources = None
        if event.GetSelection() != 0:
            self.on_log_print('Connecting to HydroShare')
            self.set_hydroshare_connection(self.hydroshare_account_choice.GetStringSelection())
        self._update_target_choices()

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

    def _move_from_selected_series(self, event):
        series_list = [self.odm_series_dict[series_id] for series_id in self.selected_series_grid.GetSelectedSeries()]
        self.available_series_grid.InsertSeriesList(series_list, do_sort=True)
        self.selected_series_grid.RemoveSelectedRows()

    def _get_current_series_ids_from_resource(self, resource):
        if isinstance(resource, H2OManagedResource):
            return [series.SeriesID for series in self.h2o_series_dict.itervalues() if series in
                    resource.selected_series.itervalues()]
        else:
            print 'Resource is unmanaged - there are no associated series'
            return []

    def _update_target_choices(self, event=None):
        WxHelper.UpdateChoiceControl(self.hs_resource_choice, self._get_destination_resource_choices())
        if event is not None:
            event.Skip()

    def _remove_from_managed_clicked(self, event):
        resource = self._get_selected_resource()  # type: HydroShareResource
        if resource is None:
            self.on_log_print('Invalid resource selected, cannot remove from managed resources')
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
            print 'Deleting files!'
            self.H2OService.ActiveHydroshare.deleteFilesInResource(resource.id)
        else:
            print 'File delete canceled'

    def _save_managed_clicked(self, event):
        if not self._verify_dataset_selections():
            return

        series = {}
        for series_id in self.selected_series_grid.GetSeries():
            if series_id in self.h2o_series_dict:
                series[series_id] = self.h2o_series_dict[series_id]

        resource = self._get_selected_resource()  # type: HydroShareResource
        if resource is None:
            self.on_log_print('Invalid resource selected, cannot save changes')
            return

        resource.title = self.resource_title_input.Value
        resource.abstract = self.resource_abstract_input.Value
        resource.agency_url = self.resource_agency_website_input.Value
        resource.award_number = self.resource_award_number_input.Value
        resource.award_title = self.resource_award_title_input.Value
        resource.funding_agency = self.resource_funding_agency_input.Value

        managed = H2OManagedResource(resource=resource,
                                     odm_series=series,
                                     resource_id=resource.id,
                                     hs_account_name=self.hydroshare_account_choice.GetStringSelection(),
                                     odm_db_name=self.database_connection_choice.GetStringSelection(),
                                     single_file=not self.chunk_by_series_checkbox.IsChecked(),
                                     chunk_years=self.chunk_by_year_checkbox.Value,
                                     associated_files=[])

        # if we aren't making a new dataset, let's remove the old one from the dictionary
        self.H2OService.ManagedResources[resource.id] = managed
        self.H2OService.SaveData()
        self._update_target_choices()
        self.hs_resource_choice.SetStringSelection(HS_RES_STR(resource))

    def _verify_dataset_selections(self):
        if len(self.selected_series_grid.GetSeries()) == 0:
            self.on_log_print('Invalid options - please select the ODM series you would like to add to the dataset')
        elif self.hydroshare_account_choice.GetSelection() == 0:
            self.on_log_print('Invalid options - please select a HydroShare account to use')
        elif len(self.resource_title_input.Value) == 0:
            self.on_log_print('Invalid options - please enter a resource name')
        else:
            return True
        return False

    def on_edit_resource_templates_clicked(self, event, create_resource=False):
        return HydroShareResourceTemplateDialog(self, self.H2OService.ResourceTemplates,
                                                create_selected=create_resource).ShowModal()

    def on_run_script_clicked(self, event):
        self.on_log_print('Running script')
        self.run_script_button.Enable(enable=False)
        self.stop_script_button.Enable(enable=True)
        self.H2OService.SaveData()
        self.H2OService.LoadData()
        self.H2OService.StartOperations()

    def on_stop_script_clicked(self, event):
        self.on_log_print('Stopping the script... This may take up to a minute')
        self.status_gauge.Pulse()
        self.H2OService.StopActions()

    def _destination_resource_changed(self, event):
        if self.hs_resource_choice.GetStringSelection() == CHOICE_DEFAULTS.CREATE_NEW_RESOURCE:
            result = self.on_edit_resource_templates_clicked(None, create_resource=True)
            if result == 0:
                self.populate_resource_fields(None)
                self.reset_series_in_grid()
        elif len(self._resources) > 2:
            resource = self._get_selected_resource()        # type: H2OManagedResource

            if resource is None:
                print 'No resource was selected'
                self.reset_series_in_grid()
                return
            elif isinstance(resource, HydroShareResource):
                if resource.id in self.H2OService.ManagedResources:
                    resource = self.H2OService.ManagedResources[resource.id]
                else:
                    temp = resource
                    self.H2OService.ActiveHydroshare.getMetadataForResource(temp)
                    self.populate_resource_fields(temp)
                    self.reset_series_in_grid()
                    return

            self.populate_resource_fields(resource.resource)
            self.on_log_print('Fetching information for resource {}'.format(resource.resource.title))

            if resource.odm_db_name != self.database_connection_choice.GetStringSelection():
                if resource.odm_db_name in self.H2OService.DatabaseConnections:
                    self.database_connection_choice.SetStringSelection(resource.odm_db_name)
                    self.set_odm_connection(self.H2OService.DatabaseConnections[resource.odm_db_name])
                else:
                    self.on_log_print('Error loading ODM series: Unknown connection {}'.format(resource.odm_db_name))
                    return

            self.reset_series_in_grid()
            matches = self._get_current_series_ids_from_resource(resource)
            for series in self.odm_series_dict.itervalues():
                if series.id in matches:
                    self.selected_series_grid.AppendSeries(series)
                else:
                    self.available_series_grid.AppendSeries(series)

            self.chunk_by_series_checkbox.SetValue(wx.CHK_CHECKED if not resource.single_file else wx.CHK_UNCHECKED)
            self.chunk_by_year_checkbox.Value = resource.chunk_years

    def populate_resource_fields(self, resource):
        if resource is None:
            self.resource_title_input.Value = ''
            self.resource_abstract_input.Value = ''
            self.resource_agency_website_input.Value = ''
            self.resource_award_number_input.Value = ''
            self.resource_award_title_input.Value = ''
            self.resource_funding_agency_input.Value = ''
        else:
            self.resource_title_input.Value = resource.title
            self.resource_abstract_input.Value = resource.abstract
            self.resource_agency_website_input.Value = resource.agency_url
            self.resource_award_number_input.Value = resource.award_number
            self.resource_award_title_input.Value = resource.award_title
            self.resource_funding_agency_input.Value = resource.funding_agency

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

    def create_gui_label(self, label, font=None):
        return WxHelper.GetLabel(self.panel, label, font)

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
        self.hydroshare_account_choice = WxHelper.GetChoice(self, self.panel, self._get_hydroshare_choices(),
                                                            on_change=self.on_hydroshare_account_chosen, size_x=310, size_y=23,
                                                            font=self.MONOSPACE)

        hs_account_sizer.Add(self.create_gui_label(u'Select a HydroShare account to continue'), pos=(0, 0), span=(1, 4),
                             flag=ALIGN.LEFT)
        hs_account_sizer.Add(self.hydroshare_account_choice, pos=(1, 0), span=(1, 4), flag=ALIGN.LEFT)
        hs_account_sizer.Add(edit_hydroshare_button, pos=(1, 4), span=(1, 1), flag=ALIGN.LEFT)

        ###################################################
        #   Sizer for HydroShare resource metadata        #
        ###################################################

        self.hs_resource_choice = WxHelper.GetChoice(self.panel, self.panel, self._get_destination_resource_choices(),
                                                     on_change=self._destination_resource_changed,
                                                     font=self.MONOSPACE)

        self.invert_resource_choices_checkbox = WxHelper.GetCheckBox(self, self.panel, u'Invert Resource Sorting', on_change=self._sort_resource_choices)
        self.resource_title_input = WxHelper.GetTextInput(self.panel)
        self.resource_abstract_input = WxHelper.GetTextInput(self.panel, wrap_text=True)
        self.resource_funding_agency_input = WxHelper.GetTextInput(self.panel)
        self.resource_agency_website_input = WxHelper.GetTextInput(self.panel)
        self.resource_award_title_input = WxHelper.GetTextInput(self.panel)
        self.resource_award_number_input = WxHelper.GetTextInput(self.panel)

        # Dataset action buttons
        self.save_dataset_button = WxHelper.GetButton(self, self.panel, u" Apply Changes ", self._save_managed_clicked,
                                                      size_x=100, size_y=30)
        self.clear_dataset_button = WxHelper.GetButton(self, self.panel, u" Clear Changes ",
                                                       self._remove_from_managed_clicked, size_x=100, size_y=30)
        self.remove_files_button = WxHelper.GetButton(self, self.panel, u"Delete Resource Files",
                                                      self._delete_files_clicked, size_x=150, size_y=30)

        col_base = 4
        row_base = 5
        flags = ALIGN.CENTER
        text_flags = wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL

        resource_sizer.Add(self.create_gui_label(u'Select a resource'), pos=(0, 0), span=(1, 1), flag=ALIGN.CENTER)
        resource_sizer.Add(self.invert_resource_choices_checkbox, pos=(0, 6), span=(1, 2), flag=text_flags)
        resource_sizer.Add(self.hs_resource_choice, pos=(1, 0), span=(1, 8), flag=ALIGN.CENTER)

        resource_sizer.Add(self.create_gui_label(u'Resource Title'), pos=(2, 0), span=(1, 1))
        resource_sizer.Add(self.create_gui_label(u'Resource Abstract'), pos=(4, 0), span=(1, 1))
        resource_sizer.Add(self.resource_title_input, pos=(3, 0), span=(1, 8), flag=ALIGN.LEFT)
        resource_sizer.Add(self.resource_abstract_input, pos=(row_base, 0), span=(4, 4), flag=ALIGN.CENTER | PADDING.ALL)

        resource_sizer.Add(self.create_gui_label(u'Funding Agency'), pos=(row_base, col_base), span=(1, 1), flag=text_flags)
        resource_sizer.Add(self.create_gui_label(u'Agency Website'), pos=(row_base + 1, col_base), span=(1, 1), flag=text_flags)
        resource_sizer.Add(self.create_gui_label(u'Award Title'), pos=(row_base + 2, col_base), span=(1, 1), flag=text_flags)
        resource_sizer.Add(self.create_gui_label(u'Award Number'), pos=(row_base + 3, col_base), span=(1, 1), flag=text_flags)
        resource_sizer.Add(self.resource_funding_agency_input, pos=(row_base, col_base + 1), span=(1, 3), flag=flags)
        resource_sizer.Add(self.resource_agency_website_input, pos=(row_base + 1, col_base + 1), span=(1, 3),
                           flag=flags)
        resource_sizer.Add(self.resource_award_title_input, pos=(row_base + 2, col_base + 1), span=(1, 3), flag=flags)
        resource_sizer.Add(self.resource_award_number_input, pos=(row_base + 3, col_base + 1), span=(1, 3), flag=flags)

        resource_sizer.Add(self.create_gui_label(u'Resource Management:'), pos=(row_base + 4, 4), span=(1, 1), flag=text_flags)
        resource_sizer.Add(self.save_dataset_button, pos=(row_base + 4, 7), span=(1, 1), flag=wx.ALIGN_CENTER)
        resource_sizer.Add(self.clear_dataset_button, pos=(row_base + 4, 6), span=(1, 1), flag=wx.ALIGN_CENTER)
        resource_sizer.Add(self.remove_files_button, pos=(row_base + 4, 5), span=(1, 1), flag=wx.ALIGN_CENTER)

        ###################################################
        #         ODM Series selection sizer              #
        ###################################################

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
                                                             on_change=self.on_database_chosen, font=self.MONOSPACE)
        odm_series_sizer.Add(self.create_gui_label(u'Select a database connection'), pos=(row, 0), span=(1, span),
                             flag=wx.ALIGN_LEFT)
        odm_series_sizer.Add(self.database_connection_choice, pos=(row + 1, 0), span=(1, span), flag=ALIGN.LEFT)
        odm_series_sizer.Add(edit_database_button, pos=(row + 1, span), span=(1, 1))

        # File chunking options
        text_flags = wx.ALIGN_CENTER | wx.ALIGN_CENTER_VERTICAL
        self.chunk_by_year_checkbox = WxHelper.GetCheckBox(self, self.panel, u'Chunk files by year')
        self.chunk_by_series_checkbox = WxHelper.GetCheckBox(self, self.panel, u'One series per file')
        odm_series_sizer.Add(self.create_gui_label(u'File options:    '), pos=(row + 1, 6), span=(1, 1), flag=text_flags)
        odm_series_sizer.Add(self.chunk_by_series_checkbox, pos=(row + 1, 7), span=(1, 1), flag=text_flags)
        odm_series_sizer.Add(self.chunk_by_year_checkbox, pos=(row + 1, 8), span=(1, 1), flag=text_flags)

        # Series selection controls
        bold_font = wx.Font(11, wx.DEFAULT, wx.NORMAL, wx.NORMAL)
        odm_series_sizer.Add(self.create_gui_label(u'Available Series', font=bold_font), pos=(row + 2, 0), span=(1, 4),
                             flag=wx.ALIGN_CENTER)
        odm_series_sizer.Add(self.create_gui_label(u'Selected Series', font=bold_font), pos=(row + 2, 5), span=(1, 4),
                             flag=wx.ALIGN_CENTER)

        grid_x_size = 500
        grid_y_size = 175
        self.selected_series_grid = WxHelper.SeriesGrid(self, self.panel, size=wx.Size(grid_x_size, grid_y_size))
        self.available_series_grid = WxHelper.SeriesGrid(self, self.panel, size=wx.Size(grid_x_size, grid_y_size))

        odm_series_sizer.Add(self.available_series_grid, pos=(row + 3, 0), span=(6, 4), flag=PADDING.ALL)
        odm_series_sizer.Add(self.selected_series_grid, pos=(row + 3, 5), span=(6, 4), flag=PADDING.ALL)

        odm_series_sizer.Add(self.add_to_selected_button, pos=(row + 4, 4), span=(1, 1), flag=wx.ALIGN_CENTER)
        odm_series_sizer.Add(self.remove_selected_button, pos=(row + 6, 4), span=(1, 1), flag=wx.ALIGN_CENTER)

        ######################################
        # Build action sizer and logging box #
        ######################################

        self.run_script_button = WxHelper.GetButton(self, self.panel, u"Run Script", self.on_run_script_clicked)
        self.stop_script_button = WxHelper.GetButton(self, self.panel, u"Stop Script", self.on_stop_script_clicked)
        self.stop_script_button.Enable(enable=False)

        self.status_gauge = wx.Gauge(self.panel, wx.ID_ANY, 100, wx.DefaultPosition, wx.DefaultSize, wx.GA_HORIZONTAL)
        self.status_gauge.SetValue(0)

        self.log_message_listbox = WxHelper.GetListBox(self, self.panel, [], size_x=920, size_y=100,
                                                       font=self.MONOSPACE, on_right_click=self.on_right_click_log_output)

        action_status_sizer.Add(self.status_gauge, pos=(0, 0), span=(1, 8), flag=ALIGN.CENTER)
        action_status_sizer.Add(self.run_script_button, pos=(0, 9), span=(1, 1), flag=ALIGN.CENTER)
        action_status_sizer.Add(self.stop_script_button, pos=(0, 8), span=(1, 1), flag=ALIGN.CENTER)
        action_status_sizer.Add(self.log_message_listbox, pos=(1, 0), span=(2, 10), flag=ALIGN.CENTER)

        ######################################
        # Build menu bar and setup callbacks #
        ######################################

        self.add_grid_bag_to_main_sizer(main_sizer, hs_account_sizer, expand=False, flags=PADDING.HORIZONTAL)
        self.add_line_to_main_sizer(main_sizer, flags=PADDING.ALL)
        self.add_grid_bag_to_main_sizer(main_sizer, selection_label_sizer, flags=PADDING.HORIZONTAL)
        self.add_grid_bag_to_main_sizer(main_sizer, resource_sizer, flags=PADDING.HORIZONTAL)
        self.add_grid_bag_to_main_sizer(main_sizer, odm_series_sizer, flags=ALIGN.CENTER | PADDING.ALL | wx.EXPAND)
        self.add_line_to_main_sizer(main_sizer, flags=PADDING.ALL)
        self.add_grid_bag_to_main_sizer(main_sizer, action_status_sizer, flags=WxHelper.GetFlags(top=False))

        ######################################
        # Build menu bar and setup callbacks #
        ######################################

        file_menu = wx.Menu()

        WxHelper.AddNewMenuItem(self, file_menu, u'ODM Connections...', self.on_edit_database)
        WxHelper.AddNewMenuItem(self, file_menu, u'HydroShare Accounts...', self.on_edit_hydroshare)
        WxHelper.AddNewMenuItem(self, file_menu, u'Resource Templates...', self.on_edit_resource_templates_clicked)

        file_menu.AppendSeparator()
        WxHelper.AddNewMenuItem(self, file_menu, u'Quit', self.on_quit_clicked)
        # file_menu.Append(wx.ID_EXIT, 'Quit', 'Quit application')

        menuBar = wx.MenuBar()
        menuBar.Append(file_menu, "&File")  # Adding the "filemenu" to the MenuBar
        self.SetMenuBar(menuBar)  # Adding the MenuBar to the Frame content.
        return main_sizer

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
