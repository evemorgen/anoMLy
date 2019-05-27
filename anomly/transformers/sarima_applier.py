import pickle
import yaml

from luigi import Task
from luigi import Parameter

from fetchers import SarimaModelFetcher, MetricFetcher
        

class SarimaApplier(Task):
    path = Parameter()

    def requires(self):
        return SarimaModelFetcher(path=self.path), MetricFetcher(path_prefix=self.path)

    def run(self):
        model, metrics = self.input()
        thing = model.open('r').read()
        a = yaml.load(thing)
        print(a)

        res = model.predict()
        print(metrics)
        print(res)
        
