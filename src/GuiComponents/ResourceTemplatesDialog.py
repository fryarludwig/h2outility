###########################################################################
## Class hydroshare_resource_editor
###########################################################################

import wx
import wx.xrc
from Utilities.HydroShareUtility import HydroShareAccountDetails, HydroShareUtility
from GAMUTRawData.CSVDataFileGenerator import OdmDatabaseDetails
from pubsub import pub
from InputValidator import *


class HydroShareResourceTemplateDialog(wx.Dialog):
    def __init__( self, parent, templates, selected=0, create_selected=False):
        wx.Dialog.__init__(self, parent, id=wx.ID_ANY, title=u"HydroShare Resource Templates", pos=wx.DefaultPosition, size=wx.DefaultSize, style=wx.DEFAULT_DIALOG_STYLE)
        self.templates = templates

        self.SetSizeHints(wx.DefaultSize, wx.DefaultSize)
        bSizer1 = wx.BoxSizer(wx.VERTICAL)

        template_selector_sizer = wx.GridBagSizer(7, 7)
        template_selector_sizer.SetFlexibleDirection(wx.BOTH)
        template_selector_sizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_SPECIFIED)

        template_text = u'Modify Template' if not create_selected else u'Load saved template'
        self.label1 = wx.StaticText(self, wx.ID_ANY, template_text, wx.DefaultPosition, wx.Size(65, -1), 0)
        self.label1.Wrap(-1)
        template_selector_sizer.Add(self.label1, wx.GBPosition(0, 0), wx.GBSpan(1, 1), wx.ALL, 7)

        template_selector_comboChoices = ['Add new...'] + templates.keys()
        self.template_selector_combo = wx.Choice(self, wx.ID_ANY, wx.DefaultPosition, wx.Size(250, -1), template_selector_comboChoices, 0)
        template_selector_sizer.Add(self.template_selector_combo, wx.GBPosition(0, 1), wx.GBSpan(1, 1), wx.ALL | wx.EXPAND, 5)

        template_selector_sizer.AddGrowableCol(1)
        bSizer1.Add(template_selector_sizer)

        bSizer2 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_staticText1 = wx.StaticText(self, wx.ID_ANY, u"Template Name", wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_CENTRE)
        self.m_staticText1.Wrap(-1)
        self.m_staticText1.SetMinSize(wx.Size(100, -1))

        bSizer2.Add(self.m_staticText1, 0, wx.ALL, 7)

        self.template_name_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.template_name_input.SetMinSize(wx.Size(250, -1))

        bSizer2.Add(self.template_name_input, 0, wx.ALL, 5)

        bSizer1.Add(bSizer2, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer21 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_staticText11 = wx.StaticText(self, wx.ID_ANY, u"Resource Name", wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_CENTRE)
        self.m_staticText11.Wrap(-1)
        self.m_staticText11.SetMinSize(wx.Size(100, -1))

        bSizer21.Add(self.m_staticText11, 0, wx.ALL, 7)

        self.resource_name_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.resource_name_input.SetMinSize(wx.Size(250, -1))

        bSizer21.Add(self.resource_name_input, 0, wx.ALL, 5)

        bSizer1.Add(bSizer21, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer22 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_staticText12 = wx.StaticText(self, wx.ID_ANY, u"Resource Abstract", wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_CENTRE)
        self.m_staticText12.Wrap(-1)
        self.m_staticText12.SetMinSize(wx.Size(100, -1))

        bSizer22.Add(self.m_staticText12, 0, wx.ALL, 7)

        self.resource_abstract_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, wx.TE_MULTILINE)
        self.resource_abstract_input.SetMinSize(wx.Size(250, 75))

        bSizer22.Add(self.resource_abstract_input, 0, wx.ALL, 5)

        bSizer1.Add(bSizer22, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer23 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_staticText13 = wx.StaticText(self, wx.ID_ANY, u"Funding Agency", wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_CENTRE)
        self.m_staticText13.Wrap(-1)
        self.m_staticText13.SetMinSize(wx.Size(100, -1))

        bSizer23.Add(self.m_staticText13, 0, wx.ALL, 7)

        self.funding_agency_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.funding_agency_input.SetMinSize(wx.Size(250, -1))

        bSizer23.Add(self.funding_agency_input, 0, wx.ALL, 5)

        bSizer1.Add(bSizer23, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer24 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_staticText14 = wx.StaticText(self, wx.ID_ANY, u"Agency Website", wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_CENTRE)
        self.m_staticText14.Wrap(-1)
        self.m_staticText14.SetMinSize(wx.Size(100, -1))

        bSizer24.Add(self.m_staticText14, 0, wx.ALL, 7)

        self.agency_url_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.agency_url_input.SetMinSize(wx.Size(250, -1))

        bSizer24.Add(self.agency_url_input, 0, wx.ALL, 5)

        bSizer1.Add(bSizer24, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer25 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_staticText15 = wx.StaticText(self, wx.ID_ANY, u"Award Title", wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_CENTRE)
        self.m_staticText15.Wrap(-1)
        self.m_staticText15.SetMinSize(wx.Size(100, -1))

        bSizer25.Add(self.m_staticText15, 0, wx.ALL, 7)

        self.award_title_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.award_title_input.SetMinSize(wx.Size(250, -1))

        bSizer25.Add(self.award_title_input, 0, wx.ALL, 5)

        bSizer1.Add(bSizer25, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer26 = wx.BoxSizer(wx.HORIZONTAL)

        self.m_staticText16 = wx.StaticText(self, wx.ID_ANY, u"Award Number", wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_CENTRE)
        self.m_staticText16.Wrap(-1)
        self.m_staticText16.SetMinSize(wx.Size(100, -1))

        bSizer26.Add(self.m_staticText16, flag=wx.ALL | wx.EXPAND, border=5)

        self.award_number_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0)
        self.award_number_input.SetMinSize(wx.Size(250, -1))

        bSizer26.Add(self.award_number_input, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer1.Add(bSizer26, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer211 = wx.BoxSizer(wx.VERTICAL)

        bSizer20 = wx.BoxSizer(wx.HORIZONTAL)

        bSizer211.Add(bSizer20, 1, wx.EXPAND, 5)

        bSizer201 = wx.BoxSizer(wx.HORIZONTAL)

        bSizer211.Add(bSizer201, 1, wx.EXPAND, 5)

        bSizer1.Add(bSizer211, flag=wx.ALL | wx.EXPAND, border=5)

        bSizer2111 = wx.BoxSizer(wx.HORIZONTAL)

        if create_selected:
            self.cancel_button = wx.Button(self, wx.ID_ANY, u"Cancel", wx.DefaultPosition, wx.DefaultSize, 0)
            bSizer2111.Add(self.cancel_button, 0, wx.ALL, 5)

            self.save_button = wx.Button(self, wx.ID_ANY, u"Create Resource", wx.DefaultPosition, wx.DefaultSize, 0)
            bSizer2111.Add(self.save_button, 0, wx.ALL, 5)

            # Connect Events
            self.cancel_button.Bind(wx.EVT_BUTTON, self.on_cancel_clicked)
            self.save_button.Bind(wx.EVT_BUTTON, self.on_create_clicked)
        else:
            self.cancel_button = wx.Button(self, wx.ID_ANY, u"Cancel", wx.DefaultPosition, wx.DefaultSize, 0)
            bSizer2111.Add(self.cancel_button, 0, wx.ALL, 5)

            self.delete_button = wx.Button(self, wx.ID_ANY, u"Delete Template", wx.DefaultPosition, wx.DefaultSize, 0)
            bSizer2111.Add(self.delete_button, 0, wx.ALL, 5)

            self.copy_button = wx.Button(self, wx.ID_ANY, u"Copy Template", wx.DefaultPosition, wx.DefaultSize, 0)
            bSizer2111.Add(self.copy_button, 0, wx.ALL, 5)

            self.save_button = wx.Button(self, wx.ID_ANY, u"Save Template", wx.DefaultPosition, wx.DefaultSize, 0)
            bSizer2111.Add(self.save_button, 0, wx.ALL, 5)
            # Connect Events
            self.cancel_button.Bind(wx.EVT_BUTTON, self.on_cancel_clicked)
            self.save_button.Bind(wx.EVT_BUTTON, self.on_save_clicked)
            self.template_selector_combo.Bind(wx.EVT_CHOICE, self.on_selection_changed)
            self.delete_button.Bind(wx.EVT_BUTTON, self.on_delete_clicked)
            self.copy_button.Bind(wx.EVT_BUTTON, self.on_copy_clicked)

        bSizer1.Add(bSizer2111, flag=wx.ALL | wx.EXPAND, border=5)

        self.SetSizerAndFit(bSizer1)
        self.Layout()

        self.Centre(wx.BOTH)


        self.template_selector_combo.SetSelection(selected)
        self.on_selection_changed()

    def __del__(self):
        pass

    def on_cancel_clicked(self, event):
        self.EndModal(False)

    def on_delete_clicked(self, event):
        print "remove account clicked!"
        pub.sendMessage("hs_resource_remove", result=self._get_input_as_dict())
        selected = self.template_selector_combo.GetCurrentSelection()
        if selected > 0:
            pub.sendMessage("hs_auth_remove", result=self._get_input_as_dict())
            self.template_selector_combo.SetSelection(selected - 1)
            self.template_selector_combo.Delete(selected)
            self.on_selection_changed()

    def on_copy_clicked(self, event):
        self.template_selector_combo.SetSelection(0)
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
        pub.sendMessage("hs_resource_create", result=self._get_input_as_dict())
        self.EndModal(True)
        event.Skip()

    def on_selection_changed(self, event=None):
        value = self.template_selector_combo.GetStringSelection()
        if value in self.templates:
            template = self.templates[value]
            self.template_name_input.SetValue(template.template_name)
            self.resource_name_input.SetValue(template.name_prefix)
            self.resource_abstract_input.SetValue(template.abstract)
            self.award_number_input.SetValue(template.award_number)
            self.award_title_input.SetValue(template.award_title)
            self.funding_agency_input.SetValue(template.funding_agency)
            self.agency_url_input.SetValue(template.agency_url)
        else:
            self.template_name_input.SetValue("")
            self.resource_name_input.SetValue("")
            self.resource_abstract_input.SetValue("")
            self.award_number_input.SetValue("")
            self.award_title_input.SetValue("")
            self.funding_agency_input.SetValue("")
            self.agency_url_input.SetValue("")

    def _get_input_as_dict(self):
        return dict(selector=self.template_selector_combo.GetStringSelection(),
                    name=self.template_name_input.Value,
                    resource_name=self.resource_name_input.Value, abstract=self.resource_abstract_input.Value,
                    funding_agency=self.funding_agency_input.Value, agency_url=self.agency_url_input.Value,
                    award_title=self.award_title_input.Value, award_number=self.award_number_input.Value)
