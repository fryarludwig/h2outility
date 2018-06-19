
class UIController:
    """
    HydroShareController provides controls for hydroshare UI elements
    """

    inputs = []
    buttons = []
    dropdowns = []
    checkboxes = []
    grids = []

    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

    @property
    def elements(self):
        return self.inputs + self.buttons + self.dropdowns + self.grids + self.checkboxes

    def DisableControls(self):
        """disables inputs and buttons"""
        self.__disable_elements(self.buttons + self.inputs)

    def EnableControls(self):
        """enables inputs and buttons"""
        self.__enable_elements(self.buttons + self.inputs)

    def DisableInputs(self):
        """disables inputs"""
        self.__disable_elements(self.inputs)

    def EnableInputs(self):
        """enables inputs"""
        self.__enable_elements(self.inputs)

    def DisableButtons(self):
        self.__disable_elements(self.buttons)

    def EnableButtons(self):
        self.__enable_elements(self.buttons)

    def EnableDropdown(self):
        """enables dropdown UI elements"""
        self.__enable_elements(self.dropdowns)

    def DisableDropdown(self):
        """disable dropdown UI elements"""
        self.__disable_elements(self.dropdowns)

    def Disable(self):
        """disables all elements"""
        self.__disable_elements(self.elements)

    def Enable(self):
        """enables all elements"""
        self.__enable_elements(self.elements)

    def DisableGrids(self):
        self.__disable_elements(self.grids)

    def EnableGrids(self):
        self.__enable_elements(self.grids)

    def __disable_elements(self, elements):
        """disables the elements passed into method"""
        for el in elements:
            if hasattr(el, 'Disable'):
                el.Disable()

    def __enable_elements(self, elements):
        """enables the elements passed into method"""
        for el in elements:
            if hasattr(el, 'Enable'):
                el.Enable()

# __all__ = ['UIController']