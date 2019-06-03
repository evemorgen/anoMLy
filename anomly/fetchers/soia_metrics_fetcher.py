import sqlite3
import json
import logging
from datetime import datetime, timedelta
from sqlalchemy import BigInteger, Integer, bindparam, Text
from luigi.contrib import sqla


from luigi import Task
from luigi import Parameter, DateParameter

from fetchers import SoiaEmailFetcher, MetricFetcher


def deduplicated(list_of_things):
    return list(set(list_of_things))


class SoiaMetricsFetcher(sqla.CopyToTable):

    columns = [
        (["id", Integer()], {"autoincrement": True, "primary_key": True}),
        (["start", BigInteger()], {}),
        (["end", BigInteger()], {}),
        (["insert_date", BigInteger()], {}),
        (["path", Text()], {}),
        (["metric_anomaly", Text()], {}),
        (["metric_whole", Text()], {})
    ]
    connection_string = "sqlite:///data/soia_email.db"
    table = "soia_with_values"

    path = Parameter()
    date = DateParameter()

    def requires(self):
        return SoiaEmailFetcher(date=datetime.now()), MetricFetcher(path_prefix=self.path)

    def copy(self, conn, ins_rows, table):
        bound_cols = dict((c, bindparam("_" + c.key)) for c in table.columns if c.key != "id")
        ins = table.insert().values(bound_cols)
        conn.execute(ins, ins_rows)

    def rows(self):
        for start, end, path, metric, whole in deduplicated(self.generate_rows()):
            yield "auto", start, end, datetime.now().strftime('%s'), path, metric, whole

    def generate_rows(self):

        now = int(datetime.now().strftime('%s'))
        _14_days_ago = int((datetime.now() - timedelta(days=14)).strftime('%s'))
        _, preloaded_metrics = self.input()

        metrics = json.loads(preloaded_metrics.open('r').read())

        conn = sqlite3.connect('data/soia_email.db')
        c = conn.cursor()
        c.execute("select distinct start, end from soia;")
        rows = c.fetchall()
        conn.close()
        formed_rows = []
        for start, end in rows:
            if start < _14_days_ago or end < _14_days_ago:
                logging.warning(f"date to early :C - {datetime.fromtimestamp(start)}, {datetime.fromtimestamp(end)}")
            else:
                logging.info(f"date good to go! - {datetime.fromtimestamp(start)}, {datetime.fromtimestamp(end)}")
                for metric in metrics:
                    shorter = list(filter(lambda tup: tup[1] >= start and tup[1] <= end, metric['datapoints']))
                    formed_rows.append(
                        (start, end, metric['target'], json.dumps(shorter), json.dumps(metric['datapoints']))
                    )
                    print(len(formed_rows[0]))

        return formed_rows
