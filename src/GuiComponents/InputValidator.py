import string
import wx
import wx.xrc

class PATTERNS:
    ALPHANUMERIC = string.letters + string.digits
    WORD = ALPHANUMERIC + '_'
    DIGIT_ONLY = string.digits
    ALPHA_ONLY = string.letters
    DENY_CUSTOM = ''
    HOSTNAME = ALPHANUMERIC + '.://&'
    USERNAME = ALPHANUMERIC + '_.@'
    ANY = string.printable

class CharValidator(wx.Validator):
    ''' Validates data as it is entered into the text controls. '''

    #----------------------------------------------------------------------
    def __init__(self, allow, deny=None):
        wx.Validator.__init__(self)
        self.allowed = allow if allow is not None else ""
        self.denied = deny if deny is not None else ""
        self.Bind(wx.EVT_CHAR, self.OnChar)

    #----------------------------------------------------------------------
    def Clone(self):
        '''Required Validator method'''
        return CharValidator(self.allowed, self.denied)

    #----------------------------------------------------------------------
    def Validate(self, win):
        return True

    #----------------------------------------------------------------------
    def TransferToWindow(self):
        return True

    #----------------------------------------------------------------------
    def TransferFromWindow(self):
        return True

    #----------------------------------------------------------------------
    def OnChar(self, event):
        keycode = int(event.GetKeyCode())
        if 31 < keycode < 256:
            key = chr(keycode)
            if key in self.denied:
                wx.Bell()
                return
            if key not in self.allowed:
                wx.Bell()
                return
        event.Skip()
