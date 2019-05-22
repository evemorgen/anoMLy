from luigi import Task
from luigi.mock import MockFile
from luigi import Parameter, IntParameter

from elasticsearch import Elasticsearch


class ElasticPathsFetcher(Task):

    hosts = Parameter()
    chunk_size = IntParameter()
    monitoring_key = Parameter()

    def output(self):
        return MockFile("in-memory-paths")

    def run(self):

        output = self.output().open('w')
        elastic = Elasticsearch(hosts=self.hosts)
        query = '{"query": {"prefix": {"path": "%s"}}}' % self.monitoring_key
        count = elastic.count(index="disthene", body=query)['count']
        limit = 5000

        sum = 0
        for chunk in range(int(count / self.chunk_size) + 1):
            result = elastic.search(
                index="disthene",
                body=query,
                size=self.chunk_size,
                from_=sum,
                stored_fields="path"
            )
            for path in result["hits"]["hits"]:
                output.write("{}\n".format(path["_source"]["path"]))

            sum += self.chunk_size
            if sum > limit:
                break
        output.close()
