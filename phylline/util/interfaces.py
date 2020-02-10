"""Support for specifying interface classes."""

# Builtins

import abc

# Packages


class InterfaceClass(abc.ABCMeta):
    """Metaclass that allows docstring inheritance.

    References:
    https://github.com/sphinx-doc/sphinx/issues/3140#issuecomment-259704637

    """

    def __new__(cls, classname, bases, cls_dict):
        """Initialize class."""
        cls = abc.ABCMeta.__new__(cls, classname, bases, cls_dict)
        mro = cls.__mro__[1:]
        for (name, member) in cls_dict.items():
            if not getattr(member, '__doc__'):
                for base in mro:
                    try:
                        member.__doc__ = getattr(base, name).__doc__
                        break
                    except AttributeError:
                        pass
        return cls


class SetterProperty(object):
    """Data descriptor which is like a property but only supports setting.

    References:
    https://stackoverflow.com/a/17576095

    """

    def __init__(self, func, doc=None):
        """Initialize members."""
        self.func = func
        self.__doc__ = doc if doc is not None else func.__doc__

    def __set__(self, obj, value):
        """Set the value."""
        return self.func(obj, value)
