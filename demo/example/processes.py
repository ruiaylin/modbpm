from modbpm.core.activity.process import (
    AbstractBaseProcess,
)
from example import tasks


class SerialProcess(AbstractBaseProcess):

    def on_start(self, name):
        hrdb = self.start(tasks.Register)(name)
        office = self.start(tasks.ProvideOffice)(name)

        self.start(tasks.ProvideComputer, predecessors=[hrdb, office])(name)
        self.start(tasks.HealthCheckUp, predecessors=[hrdb])(name)
