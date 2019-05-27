import pickle
import yaml

from luigi import Task
from luigi import Parameter
from luigi import Target
from luigi.mock import MockFile, MockFileSystem


class MockObject(Target):

    mock_file = MockFile('whatever')

    def exists(self):
        return True

    def write(self, obj):
        with self.mock_file.open('w') as _out:
            _out.write(pickle.dumps(obj))

    def read(self):
        with self.mock_file.open('r') as _in:
            return pickle.reads(_in.read())


class SarimaModelFetcher(Task):
    path = Parameter()

    def output(self):
        return MockFile('whatever')

    def run(self):
        sarima = pickle.load(open(f'data/{self.path.replace(".", "_")}.pickle', 'rb'))

        _out = self.output().open('w')
        _out.write(str(yaml.dump({'a': 'b', 'c': 1})))
        #_out.write("abc")
        _out.close()
        
