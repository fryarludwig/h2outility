from functools import partial

import datetime

import wx
import wx.dataview
import wx.grid

from pubsub import pub
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
        self.MAIN_WINDOW_SIZE = (940, 860)
        self.MONOSPACE = wx.Font(9, 75, 90, 90, False, "Inconsolata")
        self._setup_subscriptions()
        supported_notifications = ['logger', 'Dataset_Started', 'Dataset_Generated']
        self.H2OService = H2OService(subscriptions=supported_notifications)

        self.odm_series_dict = {}  # type: dict[str, Series]
        self.h2o_series_dict = {}  # type: dict[str, H2OSeries]

        self._resources = None  # type: dict[str, HydroShareResource]

        # Widgets
        self.status_gauge = None  # type: WxHelper.SimpleGauge
        self.database_connection_choice = None  # type: wx.Choice
        self.hydroshare_account_choice = None  # type: wx.Choice
        self.mapping_grid = None  # type: WxHelper.SeriesGrid

        # just technicalities, honestly
        wx.Frame.__init__(self, parent, id, title, style=wx.MAXIMIZE_BOX | wx.RESIZE_BORDER | wx.CAPTION | wx.CLOSE_BOX,
                          size=self.MAIN_WINDOW_SIZE)
        self.parent = parent
        self.Centre()
        self._build_main_window()

        self.panel.Fit()
        self.Fit()
        self.Show(True)

        self.H2OService.LoadData()
        self.UpdateControls()

    def _setup_subscriptions(self):
        SUBSCRIPTIONS = [
            (self.OnDeleteResourceTemplate, 'hs_resource_remove'),
            (self.OnSaveResourceTemplate, 'hs_resource_save'),
            (self.OnCreateResource, 'hs_resource_create'),
            (self.OnSaveHydroShareAuth, 'hs_auth_save'),
            (self.OnTestHydroShareAuth, 'hs_auth_test'),
            (self.OnRemoveHydroShareAuth, 'hs_auth_remove'),
            (self.OnSaveDatabaseAuth, 'db_auth_save'),
            (self.OnTestDatabaseAuth, 'db_auth_test'),
            (self.OnRemoveDatabaseAuth, 'db_auth_remove'),
            (self.OnPrintLog, 'logger'),
            (self.OnDatasetsGenerated, 'Datasets_Completed'),
            (self.update_status_gauge, 'Dataset_Started'),
            (self.update_status_gauge, 'Dataset_Generated')
        ]

        for sub_tuple in SUBSCRIPTIONS:
            pub.subscribe(sub_tuple[0], sub_tuple[1])

    def OnPrintLog(self, message=""):
        if message is None or len(message) < 4 or message.isspace():
            return
        self.log_message_listbox.Insert('{}: {}'.format(datetime.datetime.now().strftime('%H-%M-%S'), message), 0)

    def UpdateControls(self, progress=None):
        if progress is not None:
            self.status_gauge = progress if 100 >= progress >= 0 else progress % 100
        WxHelper.UpdateChoiceControl(self.database_connection_choice, self._get_database_choices())
        WxHelper.UpdateChoiceControl(self.hydroshare_account_choice, self._get_hydroshare_choices())

    def OnDeleteResourceTemplate(self, result=None):
        if result is None:
            return
        self.H2OService.ResourceTemplates.pop(result['selector'], None)
        self.UpdateControls()
        self.H2OService.SaveData()

    def OnSaveResourceTemplate(self, result=None):
        if result is None:
            return
        template = ResourceTemplate(result)
        self.H2OService.ResourceTemplates.pop(result['selector'], None)
        self.H2OService.ResourceTemplates[template.template_name] = template
        self.UpdateControls()
        self.H2OService.SaveData()

    def OnCreateResource(self, result=None):
        if result is None:
            return
        template = ResourceTemplate(result)
        if template is not None:
            resource = self.H2OService.CreateResourceFromTemplate(template)
            self._resources[resource.id] = resource
            self.hs_resource_choice.Append(CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, resource.id))
            self.hs_resource_choice.SetStringSelection(CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, resource.id))
            self.FillResourceFields(resource)

    def OnRemoveDatabaseAuth(self, result=None):
        if result is None:
            return
        self.H2OService.DatabaseConnections.pop(result['selector'], None)
        self.UpdateControls()
        self.H2OService.SaveData()

    def OnSaveDatabaseAuth(self, result=None):
        if result is None:
            return
        connection = OdmDatasetConnection(result)
        self.H2OService.DatabaseConnections.pop(result['selector'], None)
        self.H2OService.DatabaseConnections[connection.name] = connection
        self.UpdateControls()
        self.H2OService.SaveData()

    def OnTestDatabaseAuth(self, result=None):
        if result is None:
            pub.sendMessage('db_auth_test_reply', reply='An error occurred, please try again later')
            return
        db_details = OdmDatasetConnection(result)
        if db_details.VerifyConnection():
            pub.sendMessage('db_auth_test_reply', reply='Successfully authenticated!')
        else:
            pub.sendMessage('db_auth_test_reply', reply='Authentication details were not accepted')

    def OnRemoveHydroShareAuth(self, result=None):
        if result is None:
            return
        self.H2OService.HydroShareConnections.pop(result['selector'], None)
        self.UpdateControls()
        self.H2OService.SaveData()

    def OnSaveHydroShareAuth(self, result=None):
        if result is None:
            return
        account = HydroShareAccountDetails(result)
        self.H2OService.HydroShareConnections.pop(result['selector'], None)
        self.H2OService.HydroShareConnections[account.name] = account
        self.UpdateControls()
        self.H2OService.SaveData()

    def OnTestHydroShareAuth(self, result=None):
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
            id = re_match.groupdict()['id']
            if id in self.H2OService.ManagedResources:
                resource = self.H2OService.ManagedResources[id].resource
            else:
                resource = self._resources[id] if id in self._resources else None
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
            managed_resources = [CHOICE_DEFAULTS.RESOURCE_STR.format(resource.resource.title, id) for id, resource in
                        self.H2OService.ManagedResources.iteritems() if resource.resource is not None]
            unmanaged_resources = [CHOICE_DEFAULTS.RESOURCE_STR.format(resource.title, id) for id, resource in
                        self._resources.iteritems() if id not in self.H2OService.ManagedResources]
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

    def SetOdmConnection(self, connection):
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
            self.ResetSeriesInGrid()
        else:
            self.OnPrintLog('Unable to authenticate using connection {}'.format(connection.name))

    def SetHydroShareConnection(self, account_name):
        busy = wx.BusyInfo("Loading HydroShare account information for {}".format(account_name))
        if self.H2OService.ConnectToHydroShareAccount(account_name):
            self._resources = self.H2OService.FetchResources()
        else:
            self._resources = None

    def on_database_chosen(self, event):
        if event.GetSelection() > 0:
            selection_string = self.database_connection_choice.GetStringSelection()
            self.SetOdmConnection(self.H2OService.DatabaseConnections[selection_string])
        else:
            print "No selection made"
            self.SetOdmConnection(None)
        self.ResetSeriesInGrid()

    def on_hydroshare_account_chosen(self, event):
        self._resources = None
        if event.GetSelection() != 0:
            self.OnPrintLog('Connecting to HydroShare')
            self.SetHydroShareConnection(self.hydroshare_account_choice.GetStringSelection())
        self._update_target_choices()

    def ResetSeriesInGrid(self, event=None):
        if self.odm_series_dict is None or len(self.odm_series_dict) == 0:
            self.remove_selected_button.Disable()
            self.add_to_selected_button.Disable()
            return

        self.available_series_grid.Clear()
        self.selected_series_grid.Clear()

        for series in self.odm_series_dict.values():
            self.available_series_grid.AppendSeries(series)

        self.remove_selected_button.Enable()
        self.add_to_selected_button.Enable()

    def _move_to_selected_series(self, event):
        for id in self.available_series_grid.GetSelectedSeries():
            self.selected_series_grid.AppendSeries(self.odm_series_dict[id])
        self.available_series_grid.RemoveSelectedRows()

    def _move_from_selected_series(self, event):
        for id in self.selected_series_grid.GetSelectedSeries():
            self.available_series_grid.AppendSeries(self.odm_series_dict[id])
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
            self.OnPrintLog('Invalid resource selected, cannot remove from managed resources')
            return
        self.H2OService.ManagedResources.pop(resource.id, None)
        self.H2OService.SaveData()
        self._update_target_choices()
        self.hs_resource_choice.SetSelection(event.GetSelection())

    def _save_managed_clicked(self, event):
        if not self._verify_dataset_selections():
            return

        series = {}
        for series_id in self.selected_series_grid.GetSeries():
            if series_id in self.h2o_series_dict:
                series[series_id] = self.h2o_series_dict[series_id]

        resource = self._get_selected_resource()  # type: HydroShareResource
        if resource is None:
            self.OnPrintLog('Invalid resource selected, cannot save changes')
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
            self.OnPrintLog('Invalid options - please select the ODM series you would like to add to the dataset')
        elif self.hydroshare_account_choice.GetSelection() == 0:
            self.OnPrintLog('Invalid options - please select a HydroShare account to use')
        elif len(self.resource_title_input.Value) == 0:
            self.OnPrintLog('Invalid options - please enter a resource name')
        else:
            return True
        return False

    def OnEditResourceTemplates(self, event, create_resource=False):
        result = HydroShareResourceTemplateDialog(self, self.H2OService.ResourceTemplates,
                                                  create_selected=create_resource).ShowModal()

    def OnRunScriptClicked(self, event):
        self.OnPrintLog('Running script')
        self.H2OService.SaveData()
        self.H2OService.LoadData()
        self.H2OService.GenerateDatasetFiles()
        self.H2OService.UploadGeneratedFiles()

    def OnStopScriptClicked(self, event):
        self.OnPrintLog('Stopping the script... This may take few moments')
        self.H2OService.StopActions()
        self.status_gauge.SetValue(0)

    def _destination_resource_changed(self, event):
        if self.hs_resource_choice.GetStringSelection() == CHOICE_DEFAULTS.CREATE_NEW_RESOURCE:
            self.OnEditResourceTemplates(None, create_resource=True)
        elif len(self._resources) > 2:
            resource = self._get_selected_resource()        # type: H2OManagedResource

            if resource is None:
                print 'No resource was selected'
                return
            elif isinstance(resource, HydroShareResource):
                if resource.id in self.H2OService.ManagedResources:
                    resource = self.H2OService.ManagedResources[resource.id]
                else:
                    temp = resource
                    self.H2OService.ActiveHydroshare.getMetadataForResource(temp)
                    self.FillResourceFields(temp)
                    return

            self.FillResourceFields(resource.resource)
            self.OnPrintLog('Fetching information for resource {}'.format(resource.resource.title))

            if resource.odm_db_name != self.database_connection_choice.GetStringSelection():
                if resource.odm_db_name in self.H2OService.DatabaseConnections:
                    self.database_connection_choice.SetStringSelection(resource.odm_db_name)
                    self.SetOdmConnection(self.H2OService.DatabaseConnections[resource.odm_db_name])
                else:
                    self.OnPrintLog('Error loading ODM series: Unknown connection {}'.format(resource.odm_db_name))
                    return

            self.ResetSeriesInGrid()
            matches = self._get_current_series_ids_from_resource(resource)
            for series in self.odm_series_dict.itervalues():
                if series.id in matches:
                    self.selected_series_grid.AppendSeries(series)
                else:
                    self.available_series_grid.AppendSeries(series)

            self.chunk_by_series_checkbox.SetValue(wx.CHK_CHECKED if not resource.single_file else wx.CHK_UNCHECKED)
            self.chunk_by_year_checkbox.Value = resource.chunk_years

    def FillResourceFields(self, resource):
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

    def OnDatasetsGenerated(self, completed):
        self.OnPrintLog('Finished generating {} files for upload to HydroShare'.format(completed))
        self.status_gauge.SetValue(0)


    def update_status_gauge(self, resource="None", completed=None, started=None):
        message = ' generating files for resource {}'.format(resource)
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
        self.OnPrintLog(state + message)

    def GetLabel(self, label, font=None):
        return WxHelper.GetLabel(self.panel, label, font)

    def _build_main_window(self):
        ######################################
        #   Setup sizers and panels          #
        ######################################
        self.panel = wx.Panel(self, wx.ID_ANY)
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

        hs_account_sizer.Add(self.GetLabel(u'Select a HydroShare account to continue'), pos=(0, 0), span=(1, 4),
                             flag=ALIGN.LEFT)
        hs_account_sizer.Add(self.hydroshare_account_choice, pos=(1, 0), span=(1, 4), flag=ALIGN.LEFT)
        hs_account_sizer.Add(edit_hydroshare_button, pos=(1, 4), span=(1, 1), flag=ALIGN.LEFT)

        ###################################################
        #   Sizer for HydroShare resource metadata        #
        ###################################################

        self.hs_resource_choice = WxHelper.GetChoice(self.panel, self.panel, self._get_destination_resource_choices(),
                                                     on_change=self._destination_resource_changed,
                                                     font=self.MONOSPACE)

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
                                                       self._remove_from_managed_clicked,
                                                       size_x=100, size_y=30)

        col_base = 4
        row_base = 5
        flags = ALIGN.CENTER
        text_flags = wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL

        resource_sizer.Add(self.GetLabel(u'Select a resource'), pos=(0, 0), span=(1, 1), flag=ALIGN.CENTER)
        resource_sizer.Add(self.hs_resource_choice, pos=(1, 0), span=(1, 8), flag=ALIGN.CENTER)

        resource_sizer.Add(self.GetLabel(u'Resource Title'), pos=(2, 0), span=(1, 1))
        resource_sizer.Add(self.GetLabel(u'Resource Abstract'), pos=(4, 0), span=(1, 1))
        resource_sizer.Add(self.resource_title_input, pos=(3, 0), span=(1, 8), flag=ALIGN.LEFT)
        resource_sizer.Add(self.resource_abstract_input, pos=(row_base, 0), span=(4, 4), flag=ALIGN.CENTER)

        resource_sizer.Add(self.GetLabel(u'Funding Agency'), pos=(row_base, col_base), span=(1, 1), flag=text_flags)
        resource_sizer.Add(self.GetLabel(u'Agency Website'), pos=(row_base + 1, col_base), span=(1, 1), flag=text_flags)
        resource_sizer.Add(self.GetLabel(u'Award Title'), pos=(row_base + 2, col_base), span=(1, 1), flag=text_flags)
        resource_sizer.Add(self.GetLabel(u'Award Number'), pos=(row_base + 3, col_base), span=(1, 1), flag=text_flags)
        resource_sizer.Add(self.resource_funding_agency_input, pos=(row_base, col_base + 1), span=(1, 3), flag=flags)
        resource_sizer.Add(self.resource_agency_website_input, pos=(row_base + 1, col_base + 1), span=(1, 3),
                           flag=flags)
        resource_sizer.Add(self.resource_award_title_input, pos=(row_base + 2, col_base + 1), span=(1, 3), flag=flags)
        resource_sizer.Add(self.resource_award_number_input, pos=(row_base + 3, col_base + 1), span=(1, 3), flag=flags)

        resource_sizer.Add(self.GetLabel(u'Resource Management:'), pos=(row_base + 4, 4), span=(1, 2), flag=text_flags)
        resource_sizer.Add(self.save_dataset_button, pos=(row_base + 4, 7), span=(1, 1), flag=wx.ALIGN_CENTER)
        resource_sizer.Add(self.clear_dataset_button, pos=(row_base + 4, 6), span=(1, 1), flag=wx.ALIGN_CENTER)

        ###################################################
        #         ODM Series selection sizer              #
        ###################################################

        # Buttons (and bitmaps) to add or remove series from the active dataset
        left_arrow = WxHelper.GetBitmap('./GuiComponents/previous_icon.png', 20, 20)
        right_arrow = WxHelper.GetBitmap('./GuiComponents/next_icon.png', 20, 20)

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
        odm_series_sizer.Add(self.GetLabel(u'Select a database connection'), pos=(row, 0), span=(1, span),
                             flag=wx.ALIGN_LEFT)
        odm_series_sizer.Add(self.database_connection_choice, pos=(row + 1, 0), span=(1, span), flag=ALIGN.LEFT)
        odm_series_sizer.Add(edit_database_button, pos=(row + 1, span), span=(1, 1))

        # File chunking options
        text_flags = wx.ALIGN_CENTER | wx.ALIGN_CENTER_VERTICAL
        self.chunk_by_year_checkbox = WxHelper.GetCheckBox(self, self.panel, u'Chunk files by year')
        self.chunk_by_series_checkbox = WxHelper.GetCheckBox(self, self.panel, u'One series per file')
        odm_series_sizer.Add(self.GetLabel(u'File options:    '), pos=(row + 1, 6), span=(1, 1), flag=text_flags)
        odm_series_sizer.Add(self.chunk_by_series_checkbox, pos=(row + 1, 7), span=(1, 1), flag=text_flags)
        odm_series_sizer.Add(self.chunk_by_year_checkbox, pos=(row + 1, 8), span=(1, 1), flag=text_flags)

        # Series selection controls
        bold_font = wx.Font(11, wx.DEFAULT, wx.NORMAL, wx.NORMAL)
        odm_series_sizer.Add(self.GetLabel(u'Available Series', font=bold_font), pos=(row + 2, 0), span=(1, 4),
                             flag=wx.ALIGN_CENTER)
        odm_series_sizer.Add(self.GetLabel(u'Selected Series', font=bold_font), pos=(row + 2, 5), span=(1, 4),
                             flag=wx.ALIGN_CENTER)

        x = 500
        y = 450
        self.selected_series_grid = WxHelper.SeriesGrid(self, self.panel, max_size=wx.Size(x, y))
        self.available_series_grid = WxHelper.SeriesGrid(self, self.panel, max_size=wx.Size(x, y))

        odm_series_sizer.Add(self.available_series_grid, pos=(row + 3, 0), span=(6, 4), flag=ALIGN.CENTER)
        odm_series_sizer.Add(self.selected_series_grid, pos=(row + 3, 5), span=(6, 4), flag=ALIGN.CENTER)

        odm_series_sizer.Add(self.add_to_selected_button, pos=(row + 4, 4), span=(1, 1), flag=wx.ALIGN_CENTER)
        odm_series_sizer.Add(self.remove_selected_button, pos=(row + 6, 4), span=(1, 1), flag=wx.ALIGN_CENTER)

        ######################################
        # Build action sizer and logging box #
        ######################################

        toggle_execute_button = WxHelper.GetButton(self, self.panel, u"Run Script", self.OnRunScriptClicked)
        save_config_button = WxHelper.GetButton(self, self.panel, u"Stop Script", self.OnStopScriptClicked)

        self.status_gauge = wx.Gauge(self.panel, wx.ID_ANY, 100, wx.DefaultPosition, wx.DefaultSize, wx.GA_HORIZONTAL)
        self.status_gauge.SetValue(0)

        self.log_message_listbox = WxHelper.GetListBox(self, self.panel, [], size_x=920, size_y=100,
                                                       font=self.MONOSPACE)

        action_status_sizer.Add(self.status_gauge, pos=(0, 0), span=(1, 8), flag=ALIGN.CENTER)
        action_status_sizer.Add(toggle_execute_button, pos=(0, 9), span=(1, 1), flag=ALIGN.CENTER)
        action_status_sizer.Add(save_config_button, pos=(0, 8), span=(1, 1), flag=ALIGN.CENTER)
        action_status_sizer.Add(self.log_message_listbox, pos=(1, 0), span=(2, 10), flag=ALIGN.CENTER)

        ######################################
        # Build menu bar and setup callbacks #
        ######################################

        self.AddGridBagToMainSizer(main_sizer, hs_account_sizer, expand=False, flags=PADDING.HORIZONTAL)
        self.AddLineToMainSizer(main_sizer, flags=PADDING.ALL)
        self.AddGridBagToMainSizer(main_sizer, selection_label_sizer, flags=PADDING.HORIZONTAL)
        self.AddGridBagToMainSizer(main_sizer, resource_sizer, flags=PADDING.HORIZONTAL)
        self.AddGridBagToMainSizer(main_sizer, odm_series_sizer, flags=PADDING.HORIZONTAL)
        self.AddLineToMainSizer(main_sizer, flags=PADDING.ALL)
        self.AddGridBagToMainSizer(main_sizer, action_status_sizer, flags=WxHelper.GetFlags(top=False))

        ######################################
        # Build menu bar and setup callbacks #
        ######################################

        file_menu = wx.Menu()

        WxHelper.AddNewMenuItem(self, file_menu, u'ODM Connections...', self.on_edit_database)
        WxHelper.AddNewMenuItem(self, file_menu, u'HydroShare Accounts...', self.on_edit_hydroshare)
        WxHelper.AddNewMenuItem(self, file_menu, u'Resource Templates...', self.OnEditResourceTemplates)

        file_menu.AppendSeparator()
        file_menu.Append(wx.ID_EXIT, 'Quit', 'Quit application')

        menuBar = wx.MenuBar()
        menuBar.Append(file_menu, "&File")  # Adding the "filemenu" to the MenuBar
        self.SetMenuBar(menuBar)  # Adding the MenuBar to the Frame content.

        self.panel.SetSizerAndFit(main_sizer)
        self.SetAutoLayout(True)
        main_sizer.Fit(self.panel)

    def AddLineToMainSizer(self, parent, border=10, flags=wx.ALL | wx.EXPAND):
        parent.Add(wx.StaticLine(self.panel), 0, flag=flags, border=border)

    def AddGridBagToMainSizer(self, parent, child, expand=True, border=12, flags=None):
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
