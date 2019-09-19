from http.client import HTTPConnection
from urllib.parse import urlencode


class HDFSConsole:
    def __init__(self, host='localhost', port='50070', user=''):
        self._user = user
        self._path = '/webhdfs/v1/'
        self._method = 'GET'
        self._data = {'user.name': self._user, 'op': ''}
        self._con = HTTPConnection(host, port)

    def __del__(self):
        self._con.close()


if __name__ == '__main__':
    hdfs = HDFSConsole(user='samat')
