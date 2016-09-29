# -*- coding: utf-8 -*-

from modbpm.core.activity.process import (
    AbstractBaseProcess,
    AbstractLooseProcess,
    AbstractStrictProcess)
from example import tasks


class SerialProcess(AbstractBaseProcess):

    def on_start(self, name):
        hrdb = self.start(tasks.Register)(name)
        office = self.start(tasks.ProvideOffice)(name)

        self.start(tasks.ProvideComputer, predecessors=[hrdb, office])(name)
        self.start(tasks.HealthCheckUp, predecessors=[hrdb])(name)


class ErrorInsensitiveProcess(AbstractLooseProcess):
    """自动忽略子任务错误"""
    def on_start(self, name):
        self.start(tasks.Register)(name)
        self.start(tasks.FailedTask)(name)
        self.start(tasks.Register)(name)


class ErrorSensitiveProcess(AbstractStrictProcess):
    """子任务出错时自动失败"""
    def on_start(self, name):
        self.start(tasks.Register)(name)
        self.start(tasks.FailedTask)(name)
        self.start(tasks.Register)(name)
        self.start(tasks.Register)(name)  # 多向前执行了异步
