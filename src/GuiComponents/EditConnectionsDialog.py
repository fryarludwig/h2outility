import wx
import wx.xrc
from Utilities.HydroShareUtility import HydroShareAccountDetails, HydroShareUtility
from InputValidator import *
from pubsub import pub


# noinspection PyPropertyAccess,PyPropertyAccess,PyPropertyAccess,PyPropertyAccess,PyPropertyAccess,PyPropertyAccess,
# PyPropertyAccess
# noinspection PyPropertyAccess,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal
class DatabaseConnectionDialog(wx.Dialog):
    def __init__(self, parent, connections, selected=0):
        wx.Dialog.__init__(self, parent, id=wx.ID_ANY, title=u'Modify Database Connections', pos=wx.DefaultPosition,
                           size=wx.DefaultSize, style=wx.DEFAULT_DIALOG_STYLE)

        self.connections = connections

        pub.subscribe(self.handle_test_response, 'db_auth_test_reply')
        self.SetSizeHintsSz(wx.DefaultSize, wx.DefaultSize)

        dialog_sizer = wx.BoxSizer(wx.VERTICAL)

        account_selector_sizer = wx.GridBagSizer(7, 7)
        account_selector_sizer.SetFlexibleDirection(wx.BOTH)
        account_selector_sizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_SPECIFIED)

        self.label1 = wx.StaticText(self, wx.ID_ANY, u"Modify connection", wx.DefaultPosition, wx.Size(65, -1), 0)
        self.label1.Wrap(-1)
        account_selector_sizer.Add(self.label1, wx.GBPosition(0, 0), wx.GBSpan(1, 1), wx.ALL, 7)

        connection_selector_comboChoices = ['Add new...'] + self.connections.keys()
        self.connection_selector_combo = wx.Choice(self, wx.ID_ANY,  wx.DefaultPosition, wx.DefaultSize, connection_selector_comboChoices, 0)
        account_selector_sizer.Add(self.connection_selector_combo, wx.GBPosition(0, 1), wx.GBSpan(1, 2), wx.ALL | wx.EXPAND, 5)

        account_selector_sizer.AddGrowableCol(1)

        dialog_sizer.Add(account_selector_sizer, 1, wx.BOTTOM | wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 5)

        account_name_sizer1 = wx.GridBagSizer(7, 7)
        account_name_sizer1.SetFlexibleDirection(wx.BOTH)
        account_name_sizer1.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_SPECIFIED)

        self.label2 = wx.StaticText(self, wx.ID_ANY, u"Name", wx.DefaultPosition, wx.Size(65, -1), 0)
        self.label2.Wrap(-1)
        account_name_sizer1.Add(self.label2, wx.GBPosition(0, 0), wx.GBSpan(1, 1), wx.ALL, 7)

        self.connection_name_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.Size(250, -1), 0, validator=CharValidator(PATTERNS.WORD))
        self.connection_name_input.SetMaxLength(32)
        account_name_sizer1.Add(self.connection_name_input, wx.GBPosition(0, 1), wx.GBSpan(1, 1), wx.ALIGN_CENTER | wx.ALL | wx.EXPAND, 5)

        account_name_sizer1.AddGrowableCol(1)

        dialog_sizer.Add(account_name_sizer1, 1, wx.BOTTOM | wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 5)

        hs_username_sizer = wx.GridBagSizer(7, 7)
        hs_username_sizer.SetFlexibleDirection(wx.BOTH)
        hs_username_sizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_SPECIFIED)

        self.label3 = wx.StaticText(self, wx.ID_ANY, u"Username", wx.DefaultPosition, wx.Size(65, -1), 0)
        self.label3.Wrap(-1)
        hs_username_sizer.Add(self.label3, wx.GBPosition(0, 0), wx.GBSpan(1, 1), wx.ALL, 7)

        self.db_username_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.Size(250, -1), 0, validator=CharValidator(PATTERNS.USERNAME))
        self.db_username_input.SetMaxLength(32)
        hs_username_sizer.Add(self.db_username_input, wx.GBPosition(0, 1), wx.GBSpan(1, 1),
                              wx.ALIGN_CENTER | wx.ALL | wx.EXPAND, 5)

        hs_username_sizer.AddGrowableCol(1)

        dialog_sizer.Add(hs_username_sizer, 1, wx.EXPAND, 5)

        hs_password_sizer = wx.GridBagSizer(7, 7)
        hs_password_sizer.SetFlexibleDirection(wx.BOTH)
        hs_password_sizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_SPECIFIED)

        self.label4 = wx.StaticText(self, wx.ID_ANY, u"Password", wx.DefaultPosition, wx.Size(65, -1), 0)
        self.label4.Wrap(-1)
        hs_password_sizer.Add(self.label4, wx.GBPosition(0, 0), wx.GBSpan(1, 1), wx.ALL, 7)

        self.db_password_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.Size(250, -1), wx.TE_PASSWORD)
        self.db_password_input.SetMaxLength(32)
        hs_password_sizer.Add(self.db_password_input, wx.GBPosition(0, 1), wx.GBSpan(1, 1),
                              wx.ALIGN_CENTER | wx.ALL | wx.EXPAND, 5)

        hs_password_sizer.AddGrowableCol(1)

        dialog_sizer.Add(hs_password_sizer, 1, wx.EXPAND, 5)

        client_id_sizer = wx.GridBagSizer(7, 7)
        client_id_sizer.SetFlexibleDirection(wx.BOTH)
        client_id_sizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_SPECIFIED)

        self.label5 = wx.StaticText(self, wx.ID_ANY, u"Hostname", wx.DefaultPosition, wx.Size(65, -1), 0)
        self.label5.Wrap(-1)
        client_id_sizer.Add(self.label5, wx.GBPosition(0, 0), wx.GBSpan(1, 1), wx.ALL, 7)

        self.hostname_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.Size(250, -1), 0, validator=CharValidator(PATTERNS.HOSTNAME))
        self.hostname_input.SetMaxLength(32)
        client_id_sizer.Add(self.hostname_input, wx.GBPosition(0, 1), wx.GBSpan(1, 1),
                            wx.ALIGN_CENTER | wx.ALL | wx.EXPAND, 5)

        client_id_sizer.AddGrowableCol(1)

        dialog_sizer.Add(client_id_sizer, 1, wx.EXPAND, 5)

        port_engine_sizer = wx.GridBagSizer(7, 7)
        port_engine_sizer.SetFlexibleDirection(wx.BOTH)
        port_engine_sizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_SPECIFIED)

        self.label6 = wx.StaticText(self, wx.ID_ANY, u"Port", wx.DefaultPosition, wx.Size(65, -1), 0)
        self.label6.Wrap(-1)
        port_engine_sizer.Add(self.label6, wx.GBPosition(0, 0), wx.GBSpan(1, 1), wx.ALL, 7)

        self.port_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.Size(100, -1), 0, validator=CharValidator(PATTERNS.DIGIT_ONLY))
        self.port_input.SetMaxLength(32)
        port_engine_sizer.Add(self.port_input, wx.GBPosition(0, 1), wx.GBSpan(1, 1), wx.ALIGN_LEFT | wx.ALL, 5)

        self.m_staticText28 = wx.StaticText(self, wx.ID_ANY, u"Engine", wx.DefaultPosition, wx.Size(65, -1), 0)
        self.m_staticText28.Wrap(-1)
        port_engine_sizer.Add(self.m_staticText28, wx.GBPosition(0, 2), wx.GBSpan(1, 1), wx.ALL | wx.ALIGN_RIGHT, 5)

        engine_selector_comboChoices = [u"mssql", u"psql"]
        self.engine_selector_combo = wx.ComboBox(self, wx.ID_ANY, u"mssql", wx.DefaultPosition, wx.Size(100, -1), engine_selector_comboChoices, 0)
        port_engine_sizer.Add(self.engine_selector_combo, wx.GBPosition(0, 3), wx.GBSpan(1, 1), wx.ALIGN_LEFT | wx.ALL, 5)

        port_engine_sizer.AddGrowableCol(1)

        dialog_sizer.Add(port_engine_sizer, 1, wx.EXPAND, 5)

        databse_name_sizer = wx.GridBagSizer(7, 7)
        databse_name_sizer.SetFlexibleDirection(wx.BOTH)
        databse_name_sizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_SPECIFIED)

        self.label51 = wx.StaticText(self, wx.ID_ANY, u"Database name", wx.DefaultPosition, wx.Size(65, -1), 0)
        self.label51.Wrap(-1)
        databse_name_sizer.Add(self.label51, wx.GBPosition(0, 0), wx.GBSpan(1, 1), wx.ALL, 7)

        self.database_name_input = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.Size(250, -1), 0, validator=CharValidator(PATTERNS.WORD))
        self.database_name_input.SetMaxLength(32)
        databse_name_sizer.Add(self.database_name_input, wx.GBPosition(0, 1), wx.GBSpan(1, 1),
                               wx.ALIGN_CENTER | wx.ALL | wx.EXPAND, 5)

        databse_name_sizer.AddGrowableCol(1)
        dialog_sizer.Add(databse_name_sizer, 1, wx.EXPAND, 5)

        action_button_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.remove_connection_button = wx.Button(self, wx.ID_ANY, u"Delete", wx.DefaultPosition, wx.Size(65, -1), 0)
        action_button_sizer.Add(self.remove_connection_button, 0, wx.ALIGN_RIGHT | wx.ALL, 5)

        self.copy_connection_button = wx.Button(self, wx.ID_ANY, u"Copy", wx.DefaultPosition, wx.Size(65, -1), 0)
        action_button_sizer.Add(self.copy_connection_button, 0, wx.ALIGN_RIGHT | wx.ALL, 5)

        self.test_connection_button = wx.Button(self, wx.ID_ANY, u"Test", wx.DefaultPosition, wx.Size(65, -1), 0)
        action_button_sizer.Add(self.test_connection_button, 0, wx.ALIGN_RIGHT | wx.ALL, 5)

        self.save_connection_button = wx.Button(self, wx.ID_ANY, u"Save", wx.DefaultPosition, wx.Size(65, -1), 0)
        action_button_sizer.Add(self.save_connection_button, 0, wx.ALIGN_RIGHT | wx.ALL, 5)

        self.cancel_edit_button = wx.Button(self, wx.ID_CANCEL, u"Close", wx.DefaultPosition, wx.Size(65, -1), 0)
        self.cancel_edit_button.SetDefault()
        action_button_sizer.Add(self.cancel_edit_button, 0, wx.ALIGN_RIGHT | wx.ALL, 5)


        dialog_sizer.Add(action_button_sizer, 1, wx.LEFT | wx.RIGHT | wx.SHAPED | wx.TOP, 5)

        self.SetSizer(dialog_sizer)
        self.Layout()
        dialog_sizer.Fit(self)

        self.Centre(wx.BOTH)

        # Connect Events
        self.test_connection_button.Bind(wx.EVT_BUTTON, self.on_test_connection_clicked)
        self.copy_connection_button.Bind(wx.EVT_BUTTON, self.on_copy_connection_clicked)
        self.cancel_edit_button.Bind(wx.EVT_BUTTON, self.on_cancel_clicked)
        self.remove_connection_button.Bind(wx.EVT_BUTTON, self.on_remove_connection_clicked)
        self.save_connection_button.Bind(wx.EVT_BUTTON, self.on_save_connection_clicked)
        self.connection_selector_combo.Bind( wx.EVT_CHOICE, self.on_selection_changed)

        self.connection_selector_combo.SetSelection(selected)
        self.on_selection_changed()

    def __del__(self):
        pass

    def _get_input_as_dict(self):
        return dict(selector=self.connection_selector_combo.GetStringSelection(),
                engine=self.engine_selector_combo.GetStringSelection(), user=self.db_username_input.Value,
                    password=self.db_password_input.Value, address=self.hostname_input.Value,
                    db=self.database_name_input.Value, port=self.port_input.Value,
                    name=self.connection_name_input.Value)

    def handle_test_response(self, reply=None):
        if reply is None:
            return
        box = wx.MessageDialog(None, reply, 'Authentication Results', wx.ICON_INFORMATION)
        box.ShowModal()

    def on_copy_connection_clicked(self, event):
        self.connection_selector_combo.SetSelection(0)
        counter = 1
        new_name = "{}_({})".format(self.connection_name_input.Value, counter)
        while new_name in self.connections and counter < 10:
            new_name = "{}_({})".format(self.connection_name_input.Value, counter)
            counter += 1
        self.connection_name_input.SetValue(new_name)

    def on_test_connection_clicked(self, event):
        print "Test connection clicked!"
        pub.sendMessage("db_auth_test", result=self._get_input_as_dict())

    def on_remove_connection_clicked(self, event):
        selected = self.connection_selector_combo.GetCurrentSelection()
        if selected > 0:
            pub.sendMessage("db_auth_remove", result=self._get_input_as_dict())
            self.connection_selector_combo.SetSelection(selected - 1)
            self.connection_selector_combo.Delete(selected)
            self.on_selection_changed()

    def on_cancel_clicked(self, event):
        print "Cancel connection clicked!"
        self.EndModal(True)

    def on_save_connection_clicked(self, event):
        print "Save connection clicked!"
        details = self._get_input_as_dict()
        pub.sendMessage("db_auth_save", result=details)

        if self.connection_selector_combo.GetCurrentSelection() == 0:    # if new connection
            connection_name = details['name']
            counter = 1
            while connection_name in self.connections and counter < 10:
                connection_name = "{}_({})".format(details['name'], counter)
                # details['name'] = "{}_({})".format(counter)
                counter += 1

            self.connections[connection_name] = details
            self.connection_selector_combo.Append(connection_name)
            self.connection_selector_combo.SetStringSelection(connection_name)
            details['selector'] = connection_name

        pub.sendMessage("db_auth_save", result=details)


    def on_selection_changed(self, event=None):
        value = self.connection_selector_combo.GetStringSelection()
        if value in self.connections:
            connection = self.connections[value]
            self.connection_name_input.SetValue(connection.name)
            self.db_username_input.SetValue(connection.user)
            self.db_password_input.SetValue(connection.password)
            self.hostname_input.SetValue(connection.address)
            self.database_name_input.SetValue(connection.database)
            self.port_input.SetValue(connection.port)
            self.engine_selector_combo.SetSelection(1 if connection.engine == 'psql' else 0)
        else:
            self.connection_name_input.SetValue("")
            self.db_username_input.SetValue("")
            self.db_password_input.SetValue("")
            self.hostname_input.SetValue("")
            self.database_name_input.SetValue("")
            self.port_input.SetValue("")
            self.engine_selector_combo.SetSelection(0)
