import matplotlib.pyplot as plt
import json

from fetchers import MetricFetcher
from luigi import Task
from luigi import Parameter
from luigi import LocalTarget


class PlotMetric(Task):
    paths = Parameter()

    def requires(self):
        return MetricFetcher(path_prefix=self.paths)

    def output(self):
        return LocalTarget('output/{}.png'.format("whatever"))

    def run(self):
        for line in self.input().open('r').readlines():
            try:
                json_input = json.loads(line)[0]
                target = json_input["target"]
                values = json_input["datapoints"]
                plt.figure()
                plt.plot([x[1] for x in values], [x[0] for x in values])
                plt.title(target)
                plt.savefig('output/{}.png'.format(target.replace(".", "_")))
            except Exception:
                print("OMG, something went wrong")