
from luigi import ExternalTask
from luigi import Parameter
from luigi import LocalTarget


class StaticJSON(ExternalTask):
    filename = Parameter()

    def output(self):
        return LocalTarget("data/{}.json".format(self.filename))
