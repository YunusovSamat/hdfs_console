import requests


class HDFSConsole:
    def __init__(self, protocol='http', host='localhost',
                 port='50070', user='dr.who'):
        self._url = (protocol + '://' + host + ':' + port
                     + '/webhdfs/v1')
        self._user = user
        self._path = ''
        self._params = {'user.name': self._user, 'op': ''}

    def operation_processing(self, op, extra_path):
        if op == 'mkdir':
            self._params['op'] = 'MKDIRS'
            for path in extra_path.split():
                self.http_request('PUT', path)
        elif op == 'put':

            self._params['op'] = 'CREATE'

        elif op == 'get':
            self._params['op'] = 'OPEN'
            self.http_request('GET')

        elif op == 'append':
            self._params['op'] = 'APPEND'
            self.http_request('POST')

        elif op == 'delete':
            self._params['op'] = 'DELETE'
            self.http_request('GET')

        elif op == 'ls':
            self._params['op'] = 'LISTSTATUS'
            self.http_request('GET')

        elif op == 'cd':
            pass

        elif op == 'lls':
            pass

        elif op == 'lcd':
            pass

        else:
            return


if __name__ == '__main__':
    hdfs = HDFSConsole(user='samat')
    hdfs.operation_processing('mkdir', '/user/samat/test2')
