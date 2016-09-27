from modbpm.core.activity.process import (
    AbstractProcess,
    AbstractParallelProcess
)
from example import tasks


class ExampleProcess(AbstractProcess):

    def on_start(self, name):
        hrdb = self.start(tasks.Register)(name)
        office = self.start(tasks.ProvideOffice)(name)

        self.start(tasks.ProvideComputer, predecessors=[hrdb, office])(name)
        self.start(tasks.HealthCheckUp, predecessors=[hrdb])(name)
