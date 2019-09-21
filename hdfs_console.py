import os
import requests


class HDFSConsole:
    def __init__(self, protocol='http', host='localhost',
                 port='50070', user='dr.who'):
        self._url = (protocol + '://' + host + ':' + port
                     + '/webhdfs/v1')
        self._user = user
        self._hdfs_path = ['user', 'samat']
        os.chdir('/home/samat')
        self._params = {'user.name': self._user, 'op': ''}

    def make_paths(self, path):
        full_list_path = list() if os.path.isabs(path) else self._hdfs_path[:]
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

    def http_request(self, method, path, data=None):
        full_path = '/' + '/'.join(self.make_paths(path))
        return requests.request(method, self._url + full_path,
                                params=self._params, data=data), full_path

    def command_processing(self, op, cmd_list_paths):
        if op == 'mkdir':
            if not len(cmd_list_paths):
                print('The number of arguments is less than one')
                return
            self._params['op'] = 'MKDIRS'
            for path in cmd_list_paths:
                response, full_path = self.http_request('PUT', path)
                print(full_path, ':', sep='')
                if response.reason == 'OK':
                    if response.json()['boolean']:
                        print('\tCreated')
                    else:
                        print('\tNo created')
                else:
                    print('\t', response.reason, sep='')
        elif op in ('put', 'append'):
            if len(cmd_list_paths) != 2:
                print('The number of arguments is not two')
                return
            if op == 'put':
                self._params['op'] = 'CREATE'
                op_str = 'Created'
                method = 'PUT'
                # self._params['permission'] = '777'
            else:
                self._params['op'] = 'APPEND'
                op_str = 'Append'
                method = 'POST'
            if os.path.isfile(cmd_list_paths[0]):
                file = open(cmd_list_paths[0], 'rb')
                if not os.path.basename(cmd_list_paths[1]):
                    cmd_list_paths[1] += os.path.basename(cmd_list_paths[0])
                response, full_path = self.http_request(
                    method, cmd_list_paths[1], data=file)
                print(full_path, ':', sep='')
                if response.reason in ('Created', 'OK'):
                    print('\t', op_str, sep='')
                else:
                    print('\t', response.reason, sep='')
                    print(response.text)
            else:
                print(cmd_list_paths[0],
                      ':\n\tNot found or object not file', sep='')
        elif op == 'get':
            if len(cmd_list_paths) != 2:
                print('The number of arguments is not two')
                return
            self._params['op'] = 'OPEN'
            path_dir, path_name = os.path.split(cmd_list_paths[1])
            if not os.path.isdir(path_dir):
                print(cmd_list_paths[1], ':\n\tNot found', sep='')
                return
            elif not path_name:
                cmd_list_paths[1] += os.path.basename(cmd_list_paths[0])
            response, full_path = self.http_request(
                'GET', cmd_list_paths[0])
            if response.reason == 'OK':
                print(cmd_list_paths[1], ':', sep='')
                with open(cmd_list_paths[1], 'w') as file:
                    file.write(response.text)
                    print('\tWrited')
            else:
                print(full_path, ':\n\t', response.reason, sep='')
        elif op == 'delete':
            if not len(cmd_list_paths):
                print('The number of arguments is less than one')
                return
            self._params['op'] = 'DELETE'
            for path in cmd_list_paths:
                response, full_path = self.http_request('DELETE', path)
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
            cmd_list_paths = cmd_list_paths if cmd_list_paths else ['.']
            for path in cmd_list_paths:
                response, full_path = self.http_request('GET', path)
                print(full_path, ':', sep='')
                if response.reason == 'OK':
                    for file in response.json()['FileStatuses']['FileStatus']:
                        print('\t', file['pathSuffix'], sep='')
                else:
                    print('\t', response.reason, sep='')
        elif op == 'cd':
            if len(cmd_list_paths) == 1:
                self._params['op'] = 'GETFILESTATUS'
                response, full_path = self.http_request('GET', cmd_list_paths[0])
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
        elif op == 'lls':
            cmd_list_paths = cmd_list_paths if cmd_list_paths else ['.']
            for path in cmd_list_paths:
                print(path, ':', sep='')
                if os.path.isdir(path):
                    for block in os.listdir(path):
                        print('\t', block, sep='')
                else:
                    print('\tNot found or object not directory')
        elif op == 'lcd':
            if len(cmd_list_paths) == 1:
                print(cmd_list_paths[0], ':', sep='')
                if os.path.isdir(cmd_list_paths[0]):
                    os.chdir(cmd_list_paths)
                else:
                    print('\tPath not found or object not directory')
            else:
                print('The number of arguments is not one')
        elif op in ('exit', 'quit'):
            exit(0)
        else:
            print('Command not found')
            return


if __name__ == '__main__':
    hdfs_console = HDFSConsole()
    hdfs_console.command_processing('get', [])
