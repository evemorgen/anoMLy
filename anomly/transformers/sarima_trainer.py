import time
import json
import pickle
from pandas import Series
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statistics import mean

from luigi import Task
from luigi import format
from luigi import TupleParameter, Parameter, IntParameter
from luigi import LocalTarget

from fetchers import MetricFetcher


def sarima_model(history, config):
    order, sorder, trend = config
    model = SARIMAX(history, order=order, seasonal_order=sorder, trend=trend, enforce_stationarity=False, enforce_invertibility=False)    
    model_fit = model.fit(disp=False)
    return model_fit


def fetch_data(file, n):
    raw_json = json.loads(file.read())
    data = [value for value, timestamp in raw_json[0]['datapoints'] if value is not None]
    agregatated = [mean(data[i:i + n]) for i in range(0, len(data), n)]
    return data, agregatated


def build_model(n, config, input, output):
    _in = input().open('r')
    _out = output().open('wb')
    data, aggregated = fetch_data(_in, n)
    _in.close()
    one_day = int(24 * 1 * (60 / n))
    # rolling = Series(aggregated).rolling(one_day).std()
    start = time.time()
    model = sarima_model(
        aggregated,
        config
    )

    pickle.dump(model, _out)
    _out.close()
    end = time.time()
    print(f"Done for {n}, it took {end-start}")


class SarimaModeler(Task):
    order = TupleParameter(default=(2, 1, 0))
    seasonal_order = TupleParameter(default=(2, 0, 0))
    season = IntParameter(default=24)
    granularity = IntParameter(default=60)
    trend = Parameter(default='n')
    path = Parameter()

    def requires(self):
        return MetricFetcher(path_prefix=self.path)

    def output(self):
        return LocalTarget(f'data/{self.path.replace(".", "_")}.pickle', format=format.Nop)

    def run(self):
        cfg = [self.order, (*self.seasonal_order, self.season * int(60 / self.granularity)), self.trend]
        build_model(self.granularity, cfg, self.input, self.output)
