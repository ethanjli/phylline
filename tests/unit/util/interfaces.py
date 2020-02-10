"""Test the util.interfaces module."""

# Builtins

import abc

# Packages

from phylline.util.interfaces import InterfaceClass, SetterProperty


class Interface(object, metaclass=InterfaceClass):
    """Interface class for testing interfaces."""

    def __init__(self):
        """Initialize members."""
        self.value = None

    @abc.abstractmethod
    def method(self):
        """Do nothing."""
        pass


class SecondInterface(object, metaclass=InterfaceClass):
    """Interface class for testing intefaces."""

    @abc.abstractmethod
    def method(self):
        pass


class Derived(Interface):
    """Derived class for testing interfaces."""

    def method(self):
        self.value = 0

    @SetterProperty
    def settable(self, value):
        """Modify the value member."""
        self.value = value


class SecondDerived(SecondInterface):
    """Derived class for testing interfaces."""

    def method(self):
        return True


def test_interface_class():
    """Test whether the InterfaceClass works for docstring inheritance."""
    test_object = Derived()
    assert test_object.value is None
    assert test_object.method.__doc__ == 'Do nothing.'
    test_object.method()
    assert test_object.value == 0

    test_object = SecondDerived()
    assert test_object.method()
    assert test_object.method.__doc__ == None


def test_setter_property():
    """Test whether the SetterProperty descriptor works for setting values."""
    test_object = Derived()
    assert test_object.value is None
    test_object.settable = 42
    assert test_object.value == 42
