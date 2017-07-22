"""

User interface for configuring and running the HydroShare update utility

"""

__title__ = 'Visual H20 Utility'

import wx
from GuiComponents.VisualH2OMainWindow import VisualH2OWindow
from Common import *

if __name__ == "__main__":
    app = wx.App()
    frame = VisualH2OWindow(None, -1, __title__)
    app.MainLoop()

