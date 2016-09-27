"""
modbpm.conf
===========
"""


class ModBPMSettings(object):

    def __init__(self):
        from django.conf import settings as django_settings
        from modbpm.conf import default_settings

        self._django_settings = django_settings

        for _setting in dir(default_settings):
            if _setting == _setting.upper():
                setattr(self, _setting, getattr(default_settings, _setting))

    def __getattr__(self, key):
        if key == key.upper():
            return getattr(self._django_settings, key)
        else:
            raise AttributeError("%r object has no attribute %r"
                                 % (self.__class__.__name__, key))


settings = ModBPMSettings()
