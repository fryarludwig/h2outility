from functools import partial

import wx
from InputValidator import *
import wx.grid


class Orientation:
    VERTICAL = 1
    HORIZONTAL = 0


class GRID_SELECTION_MODES:
    CELLS = 0
    ROWS = 1
    COLUMNS = 2
    ROWS_OR_COLUMNS = 3


class WxHelper:
    class SeriesGrid(wx.grid.Grid):
        LABELS = [(u'Id', 30),
                  (u'Site Code', 100),
                  (u'Site Name', 150),
                  (u'Variable Name', 150),
                  (u'QC Code', 50),
                  (u'Source Description', 150),
                  (u'Method Description', 150)]

        SERIES_COL = {
            u'Site': 1,
            u'Variable': 3,
            u'QC Code': 4,
            u'Source': 5,
            u'Method': 6
        }

        def __init__(self, app, parent, font=wx.SMALL_FONT, size=None):
            wx.grid.Grid.__init__(self, parent, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, wx.SIMPLE_BORDER)
            if size is not None:
                self.CacheBestSize(size)
                self.SetSizeHints(size)

            self.CreateGrid(0, len(WxHelper.SeriesGrid.LABELS))
            self.EnableEditing(False)
            self.EnableCellEditControl(False)
            self.EnableScrolling(True, True)
            self.EnableGridLines(True)
            self.EnableDragGridSize(False)
            self.SetMargins(4, 4)
            self.LabelFont = font
            self.DefaultCellFont = font
            self.SetSelectionMode(GRID_SELECTION_MODES.ROWS)

            self.DisableCellEditControl()

            self.LastRowSorted = 0
            self.LastSortInverted = False

            for i in range(0, len(WxHelper.SeriesGrid.LABELS)):
                self.SetColLabelValue(i, WxHelper.SeriesGrid.LABELS[i][0])
                self.SetColSize(i, WxHelper.SeriesGrid.LABELS[i][1])

            self.EnableDragColMove(True)
            self.EnableDragColSize(True)
            self.SetColLabelSize(20)
            self.SetColLabelAlignment(wx.ALIGN_CENTRE, wx.ALIGN_CENTRE)

            self.EnableDragRowSize(True)
            self.SetRowLabelSize(1)
            self.SetRowLabelAlignment(wx.ALIGN_CENTRE, wx.ALIGN_CENTRE)
            self.SetDefaultCellAlignment(wx.ALIGN_LEFT, wx.ALIGN_TOP)
            # self.SetDefaultCellAlignment(wx.ALIGN_CENTRE, wx.ALIGN_TOP)

            app.Bind(wx.PyEventBinder(wx.grid.wxEVT_GRID_CELL_RIGHT_CLICK, 1), self.OnCellRightClick, self)
            app.Bind(wx.PyEventBinder(wx.grid.wxEVT_GRID_COL_SORT, 1), self.OnSortClicked, self)

        def OnSortClicked(self, event):
            """

            :type event: wx.grid.GridEvent
            """
            sort_inverted = not self.LastSortInverted if self.LastRowSorted == event.Col else False
            self.SortRowsByColumn(event.Col, sort_inverted)

        def ApplyLastSort(self):
            self.SortRowsByColumn(self.LastRowSorted, self.LastSortInverted)

        def SortRowsByColumn(self, column_number, sort_inverted):
            sorted_list = []
            for i in range(0, self.NumberRows):
                sort_value = self.GetCellValue(i, column_number)
                try:
                    sort_value = float(sort_value)
                except:  # Turns out this is almost as fast an if statement, and the if statement wasn't reliable enough
                    pass
                sorted_list.append((sort_value, self.GetValuesForRow(i)))

            sorted_list.sort(key=lambda x: x[0], reverse=sort_inverted)

            self.Clear()
            for row_values in [item[1] for item in sorted_list]:
                self.AddGridRow(list(row_values))

            self.LastRowSorted = column_number
            self.LastSortInverted = sort_inverted

        def GetValuesForRow(self, row_number):
            return [self.GetCellValue(row_number, column_number) for column_number in range(0, self.NumberCols)]

        def AddGridRow(self, values):
            """

            :type values: list[object]
            :type grid: wx.grid.Grid
            """
            num_cols = len(values) if len(values) <= self.NumberCols else self.NumberCols
            self.AppendRows(1)
            for i in range(0, num_cols):
                self.SetCellValue(self.GetNumberRows() - 1, i, unicode(values[i]))

        def AppendSeries(self, series):
            values = [series.id, series.site_code, series.site_name, series.variable_name,
                      series.quality_control_level_code, series.source_description,
                      series.method_description]
            self.AddGridRow(values)

        def InsertSeriesList(self, series_list, do_sort=True):
            for series in series_list:
                self.AppendSeries(series)
            if do_sort:
                self.ApplyLastSort()

        def InsertSeries(self, series, do_sort=True):
            self.AppendSeries(series)
            if do_sort:
                self.ApplyLastSort()

        def RemoveSelectedRows(self):
            for i in range(0, self.NumberRows):
                while i in self.GetSelectedRows():
                    self.DeleteRows(i)

        def GetSelectedSeries(self):
            return [int(self.GetCellValue(row, 0)) for row in self.GetSelectedRows()]

        def GetSeries(self):
            series = []
            for row in range(0, self.NumberRows):
                series.append(int(self.GetCellValue(row, 0)))
            return series

        def Clear(self):
            if self.NumberRows > 0:
                self.DeleteRows(0, self.NumberRows)

        def OnCellRightClick(self, event):
            """

            :type event: wx.grid.GridEvent
            """
            menu = wx.Menu()
            WxHelper.AddNewMenuItem(self, menu, 'Select All', on_click=partial(self._category_selection,
                                                                               command='All', row=event.GetRow()))
            WxHelper.AddNewMenuItem(self, menu, 'Deselect All', on_click=partial(self._category_selection,
                                                                                 command='None', row=event.GetRow()))
            for text in WxHelper.SeriesGrid.SERIES_COL.iterkeys():
                select = text + ': Select All'
                deselect = text + ': Deselect All'
                WxHelper.AddNewMenuItem(self, menu, select, on_click=partial(self._category_selection,
                                                                             command=select, row=event.GetRow()))
                WxHelper.AddNewMenuItem(self, menu, deselect, on_click=partial(self._category_selection,
                                                                               command=deselect, row=event.GetRow()))
            self.PopupMenu(menu)

        def _category_selection(self, event, command, row):
            if command == 'All':
                self.SelectAll()
            elif command == 'None':
                self.ClearSelection()
            else:
                category, action = command.split(u': ')
                check_column = WxHelper.SeriesGrid.SERIES_COL[category]
                check_value = self.GetCellValue(row, check_column)

                if check_value is None or len(check_value) == 0:
                    print('Unable to parse information for row {} and column {}'.format(row, check_column))
                    return

                for i in range(0, self.NumberRows):
                    cell_value = self.GetCellValue(i, check_column)
                    if cell_value == check_value:
                        if action == 'Select All':
                            self.SelectRow(i, addToSelected=True)
                        elif action == 'Deselect All':
                            self.DeselectRow(i)

    @staticmethod
    def GetFlags(flags=0, expand=True, top=True, bottom=True, left=True, right=True):
        flags |= wx.EXPAND if expand else 0
        flags |= wx.TOP if top else 0
        flags |= wx.LEFT if left else 0
        flags |= wx.RIGHT if right else 0
        flags |= wx.BOTTOM if bottom else 0
        return flags

    @staticmethod
    def GetBitmap(path, size_x=None, size_y=None):
        image = wx.Bitmap.ConvertToImage(wx.Bitmap(path, wx.BITMAP_TYPE_ANY))
        if size_x is not None and size_y is not None:
            image = image.Scale(size_x, size_y, wx.IMAGE_QUALITY_HIGH)
        return wx.Bitmap(image)

    @staticmethod
    def GetGridBagSizer(padding_x=8, padding_y=8):
        sizer = wx.GridBagSizer(vgap=padding_y, hgap=padding_x)
        sizer.SetFlexibleDirection(direction=wx.BOTH)
        sizer.SetNonFlexibleGrowMode(mode=wx.FLEX_GROWMODE_ALL)
        return sizer

    @staticmethod
    def GetRadioBox(parent, label, options, orientation=Orientation.VERTICAL):
        radiobox = wx.RadioBox(parent, wx.ID_ANY, label, wx.DefaultPosition, wx.DefaultSize, options, orientation,
                               wx.RA_SPECIFY_ROWS)
        radiobox.SetSelection(0)
        return radiobox

    @staticmethod
    def GetWxSize(size_x, size_y):
        size_x = -1 if size_x is None else size_x
        size_y = -1 if size_y is None else size_y
        return wx.Size(size_x, size_y)

    @staticmethod
    def GetTextInput(parent, placeholder_text=u'', size_x=None, size_y=None, valid_input=PATTERNS.ANY,
                     max_length=None, wrap_text=False, style=7, **kwargs):
        if wrap_text:
            style = style | wx.TE_BESTWRAP | wx.TE_MULTILINE

        text_ctrl = wx.TextCtrl(parent, wx.ID_ANY,
                                value=placeholder_text,
                                pos=wx.DefaultPosition,
                                size=wx.DefaultSize,
                                style=style,
                                validator=CharValidator(valid_input),
                                **kwargs)

        text_ctrl.SetMinSize(WxHelper.GetWxSize(size_x, size_y))
        text_ctrl.SetMaxSize(WxHelper.GetWxSize(size_x, size_y))
        if max_length is not None:
            text_ctrl.SetMaxLength(max_length)
        return text_ctrl

    @staticmethod
    def GetStaticText(parent, **kwargs):
        return wx.StaticText(parent, **kwargs)

    @staticmethod
    def GetListBox(app, parent, items, on_right_click=None, size_x=None, size_y=None, font=None, flags=wx.LB_EXTENDED):
        listbox = wx.ListBox(parent, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, items, flags)
        if size_x is not None and size_y is not None:
            listbox.SetMinSize(wx.Size(size_x, size_y))
            listbox.SetMaxSize(wx.Size(size_x, size_y))
        if font is not None:
            listbox.SetFont(font)
        if on_right_click is not None:
            app.Bind(wx.EVT_CONTEXT_MENU, on_right_click, listbox)
        return listbox

    @staticmethod
    def GetButton(app, parent, label, on_click=None, size_x=None, size_y=None, **kwargs):
        button = wx.Button(parent, id=wx.ID_ANY, label=label, pos=wx.DefaultPosition, size=wx.DefaultSize, **kwargs)
        if size_x is not None and size_y is not None:
            button.SetMinSize(wx.Size(size_x, size_y))
            button.SetMaxSize(wx.Size(size_x, size_y))
        if on_click is not None:
            app.Bind(wx.EVT_BUTTON, on_click, button)
        return button

    @staticmethod
    def GetChoice(app, parent, choices, on_change=None, size_x=None, size_y=None, font=None):
        choice = wx.Choice(parent, wx.ID_ANY, choices=choices)

        if size_x is not None and size_y is not None:
            choice.SetMinSize(wx.Size(size_x, size_y))
            choice.SetMaxSize(wx.Size(size_x, size_y))
        if on_change is not None:
            app.Bind(wx.EVT_CHOICE, on_change, choice)
        if font is not None:
            choice.SetFont(font)

        choice.SetSelection(0)
        return choice

    @staticmethod
    def GetCheckBox(app, parent, label, on_change=None, checked=False):
        checkbox = wx.CheckBox(parent, wx.ID_ANY, label, wx.Point(-1, -1), wx.DefaultSize, 0)
        if checked:
            checkbox.SetValue(wx.CHK_CHECKED)
        if on_change is not None:
            app.Bind(wx.EVT_CHECKBOX, on_change, checkbox)
        return checkbox

    @staticmethod
    def GetLabel(parent, text, font=None, style=7):
        label = wx.StaticText(parent, wx.ID_ANY, text, style=style)
        if font is not None:
            label.SetFont(font)
        return label

    @staticmethod
    def AddNewMenuItem(app, menu, label, on_click=None, return_item=False):
        menu_item = wx.MenuItem(menu, wx.ID_ANY, label)
        if on_click is not None:
            app.Bind(wx.EVT_MENU, on_click, menu_item)
        menu.Append(menu_item)
        if return_item:
            return menu_item

    @staticmethod
    def UpdateChoiceControl(control, choices):
        if control is not None and choices is not None:
            db_index = control.GetCurrentSelection()
            db_name = control.GetStringSelection()

            control.Clear()
            control.SetItems(choices if isinstance(choices, list) else list(choices))

            string_index = control.FindString(db_name)
            if string_index >= 0:
                control.SetSelection(string_index)
            elif db_index < len(control.Items):
                control.SetSelection(db_index)
            else:
                control.SetSelection(0)

    @staticmethod
    def GetMouseClickIndex(event, control):
        evt_pos = event.GetPosition()
        list_pos = control.ScreenToClient(evt_pos)
        return control.HitTest(list_pos)

    @staticmethod
    def ModalConfirm(app, message, caption='Confirm Action'):
        dialog = wx.MessageDialog(app, message, caption, wx.YES_NO | wx.ICON_QUESTION)
        return dialog

class PADDING:
    VERTICAL = WxHelper.GetFlags(left=False, right=False)
    HORIZONTAL = WxHelper.GetFlags(top=False, bottom=False)
    ALL = WxHelper.GetFlags()


class ALIGN:
    CENTER = wx.ALIGN_CENTER | wx.EXPAND | wx.ALIGN_CENTER_VERTICAL
    LEFT = wx.ALIGN_LEFT | wx.EXPAND | wx.ALIGN_CENTER_VERTICAL
    RIGHT = wx.ALIGN_RIGHT | wx.EXPAND | wx.ALIGN_CENTER_VERTICAL
