import requests
import hashlib
import json

from luigi import Task, Parameter
from luigi.mock import MockFile

from fetchers import ElasticPathsFetcher


class MetricFetcher(Task):

    path_prefix = Parameter()
    api_key = Parameter()
    render_url = Parameter()
    time_from = Parameter()
    time_to = Parameter()

    def requires(self):
        return ElasticPathsFetcher(monitoring_key=self.path_prefix)

    def output(self):
        return MockFile("metrics", mirror_on_stderr=True)

    def run(self):
        _out = self.output().open('w')
        with self.input().open('r') as file:
            for metric in file.readlines():
                url = f"{self.render_url}?from=-{self.time_from.strip()}&until={self.time_to.strip()}&target={metric.strip()}&format=json"
                key = hashlib.md5((url + self.api_key).encode('utf-8')).hexdigest()
                headers = {'Authorization': 'MaaS ' + key}
                thing = requests.get("http://" + url, headers=headers)
                _out.write(json.dumps(thing.json()) + "\n")

        _out.close()
