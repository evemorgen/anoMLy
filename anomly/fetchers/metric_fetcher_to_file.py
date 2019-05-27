from luigi import Task, Parameter
from luigi import LocalTarget


from fetchers import MetricFetcher


class MetricFetcherToFile(Task):
    filename = Parameter()
    path_prefix = Parameter()
    time_from = Parameter(default="14days")
    time_to = Parameter(default="now")


    def requires(self):
        return MetricFetcher(path_prefix=self.path_prefix, time_from=self.time_from, time_to=self.time_to)

    def output(self):
        return LocalTarget("data/{}.json".format(self.filename))

    def run(self):
        _in = self.input().open('r')
        _out = self.output().open('w')
        _out.write(_in.read())
        _in.close()
        _out.close()
