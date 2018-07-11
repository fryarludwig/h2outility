###########################################################################
## Class hydroshare_resource_editor
###########################################################################

import wx
import wx.xrc
from Utilities.HydroShareUtility import HydroShareAccountDetails, HydroShareUtility
from WxUtilities import *
from wx.lib.pubsub import pub
# from pubsub import pub
from InputValidator import *

from urlparse import urlparse
import re


# noinspection PyPropertyAccess,PyPropertyAccess,PyPropertyAccess,PyPropertyAccess,PyPropertyAccess,PyPropertyAccess,
# PyPropertyAccess
# noinspection PyPropertyAccess,PyPropertyAccess,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal
class HydroShareResourceTemplateDialog(wx.Dialog):
    def __init__(self, parent, templates, selected=0, create_selected=False):

        title = u'Create a new HydroShare Resource' if create_selected else u"Manage HydroShare Resource Templates"

        self.dialog = wx.Dialog.__init__(self, parent, id=wx.ID_ANY, title=title, pos=wx.DefaultPosition,
                           size=wx.DefaultSize, style=wx.DEFAULT_DIALOG_STYLE)

        self.urlregex = re.compile(
            r'^(?:http|ftp)s?://'
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        self.templates = templates
        self.create_new = create_selected
        self.SetSizeHints(wx.DefaultSize, wx.DefaultSize)
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        
        label_size = wx.Size(125, -1)
        input_size = wx.Size(300, -1)

        ###########################################################################
        #  Template Selection
        ###########################################################################
        template_text = u'Modify Template' if not create_selected else u'Load saved template'
        self.label1 = wx.StaticText(self, wx.ID_ANY, template_text, wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_CENTRE)
        self.label1.Wrap(-1)
        self.label1.SetMinSize(label_size)

        default_item = u'Populate fields from template...' if create_selected else u'Create new template...'
        template_choices = [default_item] + templates.keys()
        self.template_selector = wx.Choice(self, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, template_choices, 0)

        self.template_selector.SetMinSize(input_size)

        template_selector_sizer = wx.BoxSizer(wx.HORIZONTAL)
        template_selector_sizer.Add(self.label1, 0, flag=wx.ALL | wx.EXPAND, border=5)
        template_selector_sizer.Add(self.template_selector, 0, wx.ALL | wx.EXPAND, border=5)
        main_sizer.Add(template_selector_sizer, flag=wx.ALL | wx.EXPAND, border=5)

        ###########################################################################
        #  Template name input (optional)
        ###########################################################################
        if not create_selected:
            template_name_sizer = wx.BoxSizer(wx.HORIZONTAL)
            self.m_staticText1 = wx.StaticText(self, wx.ID_ANY, u"Template Name", wx.DefaultPosition, wx.DefaultSize,
                                               wx.ALIGN_CENTRE)
            self.m_staticText1.Wrap(-1)
            self.m_staticText1.SetMinSize(label_size)
            template_name_sizer.Add(self.m_staticText1, 0, flag=wx.ALL | wx.EXPAND, border=5)
            self.template_name_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize,
                                                   0)
            self.template_name_input.SetMinSize(input_size)
            template_name_sizer.Add(self.template_name_input, 0, wx.ALL | wx.EXPAND, border=5)
            main_sizer.Add(template_name_sizer, flag=wx.ALL | wx.EXPAND, border=5)

        ###########################################################################
        #  Resource name input
        ###########################################################################
        self.m_staticText11 = wx.StaticText(self, wx.ID_ANY, u"Resource Name", wx.DefaultPosition, wx.DefaultSize,
                                            wx.ALIGN_CENTRE)
        self.m_staticText11.Wrap(-1)
        self.m_staticText11.SetMinSize(label_size)

        self.resource_name_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.resource_name_input.SetMinSize(input_size)

        name_sizer = wx.BoxSizer(wx.HORIZONTAL)
        name_sizer.Add(self.m_staticText11,  0, flag=wx.ALL | wx.EXPAND, border=5)
        name_sizer.Add(self.resource_name_input,  0, flag=wx.ALL | wx.EXPAND, border=5)
        main_sizer.Add(name_sizer, flag=wx.ALL | wx.EXPAND, border=5)

        ###########################################################################
        #  Resource Abstract input 
        ###########################################################################
        self.m_staticText12 = wx.StaticText(self, wx.ID_ANY, u"Resource Abstract", wx.DefaultPosition, wx.DefaultSize,
                                            wx.ALIGN_CENTRE)
        self.m_staticText12.Wrap(-1)
        self.m_staticText12.SetMinSize(label_size)

        self.resource_abstract_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize,
                                                   wx.TE_MULTILINE)
        self.resource_abstract_input.SetMinSize(wx.Size(300, 75))
        abstract_sizer = wx.BoxSizer(wx.HORIZONTAL)
        abstract_sizer.Add(self.m_staticText12, 0, flag=wx.ALL | wx.EXPAND, border=5)
        abstract_sizer.Add(self.resource_abstract_input, 0, flag=wx.ALL | wx.EXPAND, border=5)
        main_sizer.Add(abstract_sizer, flag=wx.ALL | wx.EXPAND, border=5)

        ###########################################################################
        #  Funding agency input 
        ###########################################################################
        self.m_staticText13 = wx.StaticText(self, wx.ID_ANY, u"Funding Agency", wx.DefaultPosition, wx.DefaultSize,
                                            wx.ALIGN_CENTRE)
        self.m_staticText13.Wrap(-1)
        self.m_staticText13.SetMinSize(label_size)
        self.funding_agency_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.funding_agency_input.SetMinSize(input_size)
        funding_agency_sizer = wx.BoxSizer(wx.HORIZONTAL)
        funding_agency_sizer.Add(self.m_staticText13, 0, flag=wx.ALL | wx.EXPAND, border=5)
        funding_agency_sizer.Add(self.funding_agency_input, 0, flag=wx.ALL | wx.EXPAND, border=5)
        main_sizer.Add(funding_agency_sizer, flag=wx.ALL | wx.EXPAND, border=5)

        ###########################################################################
        #  Agency URL input 
        ###########################################################################
        self.m_staticText14 = wx.StaticText(self, wx.ID_ANY, u"Agency Website", wx.DefaultPosition, wx.DefaultSize,
                                            wx.ALIGN_CENTRE)
        self.m_staticText14.Wrap(-1)
        self.m_staticText14.SetMinSize(label_size)

        self.agency_url_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.agency_url_input.SetMinSize(input_size)

        agency_url_sizer = wx.BoxSizer(wx.HORIZONTAL)
        agency_url_sizer.Add(self.m_staticText14, 0, flag=wx.ALL | wx.EXPAND, border=5)
        agency_url_sizer.Add(self.agency_url_input, 0, flag=wx.ALL | wx.EXPAND, border=5)
        main_sizer.Add(agency_url_sizer, flag=wx.ALL | wx.EXPAND, border=5)

        ###########################################################################
        #  Award title input 
        ###########################################################################
        self.m_staticText15 = wx.StaticText(self, wx.ID_ANY, u"Award Title", wx.DefaultPosition, wx.DefaultSize,
                                            wx.ALIGN_CENTRE)
        self.m_staticText15.Wrap(-1)
        self.m_staticText15.SetMinSize(label_size)

        self.award_title_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.award_title_input.SetMinSize(input_size)

        award_title_sizer = wx.BoxSizer(wx.HORIZONTAL)
        award_title_sizer.Add(self.m_staticText15, 0, flag=wx.ALL | wx.EXPAND, border=5)
        award_title_sizer.Add(self.award_title_input, 0, flag=wx.ALL | wx.EXPAND, border=5)
        main_sizer.Add(award_title_sizer, flag=wx.ALL | wx.EXPAND, border=5)

        ###########################################################################
        #  Award number input 
        ###########################################################################
        self.m_staticText16 = wx.StaticText(self, wx.ID_ANY, u"Award Number", wx.DefaultPosition, wx.DefaultSize,
                                            wx.ALIGN_CENTRE)
        self.m_staticText16.Wrap(-1)
        self.m_staticText16.SetMinSize(label_size)

        self.award_number_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.award_number_input.SetMinSize(input_size)

        award_number_sizer = wx.BoxSizer(wx.HORIZONTAL)
        award_number_sizer.Add(self.m_staticText16, flag=wx.ALL | wx.EXPAND, border=5)
        award_number_sizer.Add(self.award_number_input, flag=wx.ALL | wx.EXPAND, border=5)
        main_sizer.Add(award_number_sizer, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer211 = wx.BoxSizer(wx.VERTICAL)
        bSizer20 = wx.BoxSizer(wx.HORIZONTAL)
        bSizer211.Add(bSizer20, 1, wx.EXPAND, border=5)
        bSizer201 = wx.BoxSizer(wx.HORIZONTAL)
        bSizer211.Add(bSizer201, 1, wx.EXPAND, border=5)
        main_sizer.Add(bSizer211, flag=wx.ALL | wx.EXPAND, border=5)

        ###########################################################################
        #  Action buttons
        ###########################################################################
        buttons_sizer = wx.BoxSizer(wx.HORIZONTAL)

        if create_selected:
            self.cancel_button = wx.Button(self, wx.ID_ANY, u"Cancel", wx.DefaultPosition, wx.DefaultSize, 0)
            self.save_button = wx.Button(self, wx.ID_ANY, u"Create Resource", wx.DefaultPosition, wx.DefaultSize, 0)
            buttons_sizer.Add(self.cancel_button, 0, flag=wx.ALL | wx.EXPAND, border=5)
            buttons_sizer.Add(self.save_button, 0, flag=wx.ALL | wx.EXPAND, border=5)

            # Connect Events
            self.cancel_button.Bind(wx.EVT_BUTTON, self.on_cancel_clicked)
            self.save_button.Bind(wx.EVT_BUTTON, self.on_create_clicked)
        else:
            self.cancel_button = wx.Button(self, wx.ID_ANY, u"Cancel", wx.DefaultPosition, wx.DefaultSize, 0)
            self.delete_button = wx.Button(self, wx.ID_ANY, u"Delete Template", wx.DefaultPosition, wx.DefaultSize, 0)
            self.copy_button = wx.Button(self, wx.ID_ANY, u"Copy Template", wx.DefaultPosition, wx.DefaultSize, 0)
            self.save_button = wx.Button(self, wx.ID_ANY, u"Save Template", wx.DefaultPosition, wx.DefaultSize, 0)

            buttons_sizer.Add(self.cancel_button, 0, flag=wx.ALL | wx.EXPAND, border=5)
            buttons_sizer.Add(self.delete_button, 0, flag=wx.ALL | wx.EXPAND, border=5)
            buttons_sizer.Add(self.copy_button, 0, flag=wx.ALL | wx.EXPAND, border=5)
            buttons_sizer.Add(self.save_button, 0, flag=wx.ALL | wx.EXPAND, border=5)

            # Connect Events
            self.cancel_button.Bind(wx.EVT_BUTTON, self.on_cancel_clicked)
            self.save_button.Bind(wx.EVT_BUTTON, self.on_save_clicked)
            self.delete_button.Bind(wx.EVT_BUTTON, self.on_delete_clicked)
            self.copy_button.Bind(wx.EVT_BUTTON, self.on_copy_clicked)

        self.template_selector.Bind(wx.EVT_CHOICE, self.on_selection_changed)
        main_sizer.Add(buttons_sizer, flag=wx.ALL | wx.ALIGN_RIGHT, border=5)

        ###########################################################################
        #  Finish off the rest
        ###########################################################################
        self.SetSizerAndFit(main_sizer)
        self.Layout()

        self.Centre(wx.BOTH)
        self.template_selector.SetSelection(selected)
        self.on_selection_changed()

    def __del__(self):
        pass

    def on_cancel_clicked(self, event):
        self.EndModal(False)

    def on_delete_clicked(self, event):
        print "remove account clicked!"
        pub.sendMessage("hs_resource_remove", result=self._get_input_as_dict())
        selected = self.template_selector.GetCurrentSelection()
        if selected > 0:
            pub.sendMessage("hs_auth_remove", result=self._get_input_as_dict())
            self.template_selector.SetSelection(selected - 1)
            self.template_selector.Delete(selected)
            self.on_selection_changed()

    def on_copy_clicked(self, event):
        self.template_selector.SetSelection(0)
        counter = 1
        new_name = "{}_({})".format(self.template_name_input.Value, counter)
        while new_name in self.templates and counter < 10:
            new_name = "{}_({})".format(self.template_name_input.Value, counter)
            counter += 1
        self.template_name_input.SetValue(new_name)

    def on_save_clicked(self, event):
        pub.sendMessage("hs_resource_save", result=self._get_input_as_dict())
        self.EndModal(True)
        event.Skip()

    def on_create_clicked(self, event):

        result = self._get_input_as_dict()
        agwebsite_initial = agwebsite = result.get('agency_url', '')

        error_list = []

        # Make sure the resource has a name
        if not len(result.get('resource_name', '')):
            error_list.append("The 'Resource Name' field is required.")

        # If the value for `agency_url` is not empty, validate the URL, otherwise, continue on
        if len(agwebsite_initial):

            # If the user did not include a scheme for the agency website, use 'http://' as the default
            if not re.match(r'https?://', agwebsite):
                result['agency_url'] = 'http://' + result.get('agency_url', '')

            # If `agwebsite` passes the url pattern check, continue on, otherwise
            # show some sort of validation error
            if not self.urlregex.match(result.get('agency_url')):
                error_list.append(
                    "Agency Website '{}' is an invalid URL.\n\nEnter a valid URL to continue.".format(agwebsite_initial)
                )


        if not len(error_list):

            pub.sendMessage("hs_resource_create", result=result)

            self.EndModal(True)

        else:
            if len(error_list) > 1:
                msg = "Please fix the following errors"

                for err in error_list:
                    msg += "\n\n - {}".format(err)


            else:
                msg = "Error: {}".format(error_list[0])

            wx.MessageBox(msg, parent=self.dialog, caption='Error', style=wx.OK)


        event.Skip()

    def on_selection_changed(self, event=None):
        value = self.template_selector.GetStringSelection()
        if value in self.templates:
            template = self.templates[value]
            if not self.create_new:
                self.template_name_input.SetValue(template.template_name)
            self.resource_name_input.SetValue(template.title)
            self.resource_abstract_input.SetValue(template.abstract)
            self.award_number_input.SetValue(template.award_number)
            self.award_title_input.SetValue(template.award_title)
            self.funding_agency_input.SetValue(template.funding_agency)
            self.agency_url_input.SetValue(template.agency_url)
        else:
            if not self.create_new:
                self.template_name_input.SetValue("")
            self.resource_name_input.SetValue("")
            self.resource_abstract_input.SetValue("")
            self.award_number_input.SetValue("")
            self.award_title_input.SetValue("")
            self.funding_agency_input.SetValue("")
            self.agency_url_input.SetValue("")

    def _get_input_as_dict(self):
        return dict(selector=self.template_selector.GetStringSelection(),
                    name=self.template_name_input.Value if not self.create_new else '',
                    resource_name=self.resource_name_input.Value, abstract=self.resource_abstract_input.Value,
                    funding_agency=self.funding_agency_input.Value, agency_url=self.agency_url_input.Value,
                    award_title=self.award_title_input.Value, award_number=self.award_number_input.Value)
