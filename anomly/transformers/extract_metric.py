import json

from fetchers import StaticJSON
from luigi import Task, Parameter
from luigi import LocalTarget


class ExtractMetrics(Task):
    filename = Parameter()

    def requires(self):
        return StaticJSON(self.filename)

    def run(self):
        out = self.output().open('w')
        out.write("target,timestamp,value\n")
        try:
            js = json.loads(self.input().open('r').read())
            for metric in js:
                for value, ts in metric["datapoints"]:
                    out.write('{},{},{}\n'.format(metric["target"], ts, value if value else ""))
                    #out.write('{},{}'.format(metric["target"], json.dumps(metric["datapoints"])))
            out.close()
        except Exception:
            print("OMG, bad json")

    def output(self):
        return LocalTarget('data/{}.csv'.format(self.filename))
