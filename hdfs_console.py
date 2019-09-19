import requests
import json


class HDFSConsole:
    def __init__(self, protocol='http', host='localhost',
                 port='50070', user='dr.who'):
        self._url = (protocol + '://' + host + ':' + port
                     + '/webhdfs/v1/')
        self._user = user
        self._hdfs_path = ''
        self._params = {'user.name': self._user, 'op': ''}

    def http_request(self, method, path=''):
        temp_path = self._hdfs_path
        for block in path.split('/'):
            if (block == '') or (block == '.'):
                pass
            elif block == '..':
                temp_path = temp_path.rpartition('/')[0]
            else:
                temp_path += block + '/'
        response = requests.request(method, self._url + temp_path,
                                    params=self._params)
        return response

    def operation_processing(self, op, extra_path):
        if op == 'mkdir':
            self._params['op'] = 'MKDIRS'
            for path in extra_path.split():
                result = self.http_request('PUT', path)
                if result.reason == 'OK':
                    print('Successfully created directory')
                else:
                    print('No created directory')
        elif op == 'put':
            self._params['op'] = 'CREATE'
            self.http_request('PUT')
        elif op == 'get':
            self._params['op'] = 'OPEN'
            self.http_request('GET')
        elif op == 'append':
            self._params['op'] = 'APPEND'
            self.http_request('POST')
        elif op == 'delete':
            self._params['op'] = 'DELETE'
            for path in extra_path.split():
                result = self.http_request('DELETE', path)
                if result.reason == 'OK':
                    print('Successfully deleted')
                else:
                    print('No deleted')
        elif op == 'ls':
            self._params['op'] = 'LISTSTATUS'
            
        elif op == 'cd':
            pass
        elif op == 'lls':
            pass
        elif op == 'lcd':
            pass
        else:
            print('Command not found')
            return


if __name__ == '__main__':
    hdfs = HDFSConsole(user='samat')
    hdfs.operation_processing('ls', '')
