import requests


class HDFSConsole:
    def __init__(self, protocol='http', host='localhost',
                 port='50070', user='dr.who'):
        self._url = (protocol + '://' + host + ':' + port
                     + '/webhdfs/v1')
        self._user = user
        self._hdfs_path = ['user', 'samat']
        self._params = {'user.name': self._user, 'op': ''}

    def make_paths(self, path):
        full_list_path = list() if path[0] == '/' else self._hdfs_path.copy()
        list_path = tuple(filter(None, path.split('/')))
        for block in list_path:
            if block == '.':
                pass
            elif block == '..':
                if full_list_path:
                    full_list_path.pop()
            else:
                full_list_path.append(block)
        return full_list_path

    def http_request(self, method, full_path):
        return requests.request(
                    method, self._url + full_path,
                    params=self._params)

    def operation_processing(self, op, cmd_path):
        if op == 'mkdir':
            self._params['op'] = 'MKDIRS'
            for path in cmd_path.split():
                full_path = '/' + '/'.join(self.make_paths(path))
                response = self.http_request('PUT', full_path)
                print(full_path, ':', sep='')
                if response.reason == 'OK':
                    if response.json()['boolean']:
                        print('\tCreated')
                    else:
                        print('\tNo created')
                else:
                    print('\t', response.reason, sep='')
        elif op == 'delete':
            self._params['op'] = 'DELETE'
            for path in cmd_path.split():
                full_path = '/' + '/'.join(self.make_paths(path))
                response = self.http_request('DELETE', full_path)
                print(full_path, ':', sep='')
                if response.reason == 'OK':
                    if response.json()['boolean']:
                        print('\tDeleted')
                    else:
                        print('\tNot found')
                else:
                    print('\t', response.reason, sep='')
                    print('\tNo deleted')
        elif op == 'ls':
            self._params['op'] = 'LISTSTATUS'
            cmd_path = cmd_path if cmd_path else '.'
            for path in cmd_path.split():
                full_path = '/' + '/'.join(self.make_paths(path))
                response = self.http_request('GET', full_path)
                print(full_path, ':', sep='')
                if response.reason == 'OK':
                    for file_status in response.json()['FileStatuses']['FileStatus']:
                        print('\t', file_status['pathSuffix'], sep='')
                else:
                    print(response.reason)
        elif op == 'cd':
            self._params['op'] = 'GETFILESTATUS'
            if len(cmd_path.split()) == 1:
                full_path = '/' + '/'.join(self.make_paths(cmd_path))
                response = self.http_request('GET', full_path)
                if response.reason == 'OK':
                    if response.json()['FileStatus']['type'] == 'DIRECTORY':
                        self._hdfs_path = tuple(filter(None, full_path.split('/')))
                        print(full_path)
                    else:
                        print("This is not directory")
                else:
                    print(response.reason)
            else:
                print('The number of arguments is not one')
        else:
            print('Command not found')
            return

    def get_pwd(self):
        return self._hdfs_path


if __name__ == '__main__':
    hdfs_console = HDFSConsole(user='samat')
    hdfs_console.operation_processing('cd', '/user/samat/dir1/')
    # print(hdfs_console.get_pwd())
