from modbpm.core.activity.task import AbstractTask


class Register(AbstractTask):

    def on_start(self, name):
        self.finish()


class ProvideOffice(AbstractTask):

    def on_start(self, name):
        self.set_default_scheduler(self.on_schedule)

    def on_schedule(self):
        if self.schedule_count >= 3:
            self.finish()


class ProvideComputer(AbstractTask):

    def on_start(self, name):
        self.set_static_scheduler(self.on_schedule, 5)

    def on_schedule(self):
        if self.schedule_count >= 3:
            self.finish()


class HealthCheckUp(AbstractTask):

    def on_start(self, name):
        self.set_default_scheduler(self.on_schedule)

    def on_schedule(self):
        if self.schedule_count >= 3:
            self.finish()
