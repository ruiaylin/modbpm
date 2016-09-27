"""
modbpm.apps
===========
"""
from django.apps import AppConfig


class ModBPMConfig(AppConfig):
    name = 'modbpm'
    label = name
    verbose_name = "ModBPM"

    def ready(self):
        from modbpm.signals import dispatch
        dispatch.dispatch()
