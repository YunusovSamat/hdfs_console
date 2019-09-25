import os
import requests


class HDFSConsole:
    def __init__(self):
        self._url = ''
        # Установка текущего пути в hdfs.
        self._hdfs_path = ['user', 'samat']
        # Установка текущего пути в локальной fs.
        os.chdir('/home/samat')
        # Словарь для формирования параметров.
        self._params = {'user.name': '', 'op': ''}

    # Метод для добавления относительного пути (path) к текущему.
    def make_paths(self, path):
        # Если путь абсолютный, то создать пустой список,
        # иначе получить текущий путь.
        full_list_path = list() if os.path.isabs(path) else self._hdfs_path[:]
        # Разбить относительный путь и удалить пустые блоки.
        list_path = tuple(filter(None, path.split('/')))
        # Перебираем каталоги и/или файлы, чтобы добавить в абсолютный путь.
        for block in list_path:
            # Если текущий каталог, то не добавлять в путь.
            if block == '.':
                pass
            # Если родительский каталог, то удалить из пути последний блок.
            elif block == '..':
                if full_list_path:
                    full_list_path.pop()
            # Если блок простой
            else:
                full_list_path.append(block)
        return full_list_path

    # Метод для отправки запроса.
    def http_request(self, method, path, data=None):
        full_path = '/' + '/'.join(self.make_paths(path))
        return requests.request(method, self._url + full_path,
                                params=self._params, data=data), full_path

    # Метод для обработки командных операций.
    def command_processing(self, op, cmd_list_paths):
        # Создание каталогов
        if op == 'mkdir':
            # Если нет пути, то завершить операцию.
            if not len(cmd_list_paths):
                print('The number of arguments is less than one')
                return
            self._params['op'] = 'MKDIRS'
            # Перебираем все пути
            for path in cmd_list_paths:
                # Получаем ответ от сервера и полный путь каталога.
                response, full_path = self.http_request('PUT', path)
                print(full_path, ':', sep='')
                if response.reason == 'OK':
                    if response.json()['boolean']:
                        print('\tCreated')
                    else:
                        print('\tNo created')
                else:
                    print('\t', response.reason, sep='')
        # Передача локальных данных на сервер.
        elif op in ('put', 'append'):
            if op == 'put':
                # Если прописан только локальный путь,
                # то добавить текущий hdfs путь.
                if len(cmd_list_paths) == 1:
                    cmd_list_paths.append('./')
                elif len(cmd_list_paths) != 2:
                    print('The number of arguments is not two')
                    return
                self._params['op'] = 'CREATE'
                op_write = 'Created'
                method = 'PUT'
                # self._params['permission'] = '777'
            else:
                if len(cmd_list_paths) != 2:
                    print('The number of arguments is not two')
                    return
                self._params['op'] = 'APPEND'
                op_write = 'Append'
                method = 'POST'
            # Если локальный объкт является файлом и существует.
            if os.path.isfile(cmd_list_paths[0]):
                # Открыть файл на чтение в бинарном режиме.
                file = open(cmd_list_paths[0], 'rb')
                # Если у hdfs пути нет названия файла
                if not os.path.basename(cmd_list_paths[1]):
                    # Присвоить локальное название к hdfs пути.
                    cmd_list_paths[1] += os.path.basename(cmd_list_paths[0])
                response, full_path = self.http_request(
                    method, cmd_list_paths[1], data=file)
                print(full_path, ':', sep='')
                if response.reason in ('Created', 'OK'):
                    print('\t', op_write, sep='')
                else:
                    print('\t', response.reason, sep='')
                    print(response.text)
            else:
                print(cmd_list_paths[0],
                      ':\n\tNot found or object not file', sep='')
        # Передача данных сервера на локальную машину.
        elif op == 'get':
            # Если прописан только путь сервера,
            # то добавить текущий локальный путь.
            if len(cmd_list_paths) == 1:
                cmd_list_paths.append('./')
            elif len(cmd_list_paths) != 2:
                print('The number of arguments is not two')
                return
            self._params['op'] = 'OPEN'
            # Отделяю путь от имени файла.
            path_dir, path_name = os.path.split(cmd_list_paths[1])
            # Если путь каталога не существует, то завершить операцию
            if path_dir and (not os.path.isdir(path_dir)):
                print(cmd_list_paths[1], ':\n\tNot found', sep='')
                return
            # Если у локального пути нет названия файла
            elif not path_name:
                # Присвоить hdfs название к локальному пути.
                cmd_list_paths[1] += os.path.basename(cmd_list_paths[0])
            response, full_path = self.http_request('GET', cmd_list_paths[0])
            if response.reason == 'OK':
                print(cmd_list_paths[1], ':', sep='')
                # открыть или создать файл на запись.
                with open(cmd_list_paths[1], 'w') as file:
                    # Записать в локальный файл данные из серверного файла.
                    file.write(response.text)
                    print('\tWrited')
            else:
                print(full_path, ':\n\t', response.reason, sep='')
        # Удалине файлов и каталогов с сервра.
        elif op == 'delete':
            # Если нет путей, то завершить операцию.
            if not len(cmd_list_paths):
                print('The number of arguments is less than one')
                return
            self._params['op'] = 'DELETE'
            # Перебираем все пути.
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
        # Вывод на экран файлов и каталогов в каталоге сервера.
        elif op == 'ls':
            self._params['op'] = 'LISTSTATUS'
            # Если нет пути,то установить текущий.
            cmd_list_paths = cmd_list_paths if cmd_list_paths else ['./']
            # Перебираем все пути.
            for path in cmd_list_paths:
                response, full_path = self.http_request('GET', path)
                print(full_path, ':', sep='')
                if response.reason == 'OK':
                    # Вывод всех файлов и каталогов в данном каталоге.
                    for file in response.json()['FileStatuses']['FileStatus']:
                        print('\t', file['pathSuffix'], sep='')
                else:
                    print('\t', response.reason, sep='')
        # Установка текущего пути сервера.
        elif op == 'cd':
            if len(cmd_list_paths) == 1:
                self._params['op'] = 'GETFILESTATUS'
                response, full_path = self.http_request('GET',
                                                        cmd_list_paths[0])
                if response.reason == 'OK':
                    if response.json()['FileStatus']['type'] == 'DIRECTORY':
                        self._hdfs_path = list(
                            filter(None, full_path.split('/')))
                        print(full_path)
                    else:
                        print("This is not directory")
                else:
                    print(response.reason)
            else:
                print('The number of arguments is not one')
        # Вывод файлов и каталогов в каталоге локальной машины.
        elif op == 'lls':
            # Если нет пути,то установить текущий.
            cmd_list_paths = cmd_list_paths if cmd_list_paths else ['./']
            # Перебираем все пути.
            for path in cmd_list_paths:
                print(path, ':', sep='')
                # Если путь является каталогом и существует
                if os.path.isdir(path):
                    # Выводим все файлы и каталоги.
                    for block in os.listdir(path):
                        print('\t', block, sep='')
                else:
                    print('\tNot found or object not directory')
        # Установка текущего пути локальной машины.
        elif op == 'lcd':
            if len(cmd_list_paths) == 1:
                print(cmd_list_paths[0])
                # Если путь является каталогом и существует
                if os.path.isdir(cmd_list_paths[0]):
                    # Установить как текущий каталог
                    os.chdir(cmd_list_paths[0])
                else:
                    print('Path not found or object not directory')
            else:
                print('The number of arguments is not one')
        # Выход из программы.
        elif op in ('exit', 'quit'):
            exit(0)
        else:
            print('Command not found')
            return

    # Метод для бесконечного ввода команд.
    def inf_cycle(self):
        print('Welcome')
        # while True:
        #     data = input('Input host port user:\n>>> ')
        #     if len(data.split()) == 3:
        #         host, port, user = data.split()
        #         self._url = 'http://' + host + ':' + port + '/webhdfs/v1'
        #         self._params['user.name'] = user
        #         self._params['op'] = 'LISTSTATUS'
        #         try:
        #             self.http_request('GET', '/')
        #         except requests.ConnectionError:
        #             print('Wrong host, port or user')
        #         else:
        #             print('Connect')
        #             break
        self._url = 'http://localhost:50070/webhdfs/v1'
        self._params['user.name'] = 'samat'
        while True:
            cmd_str = input('>>> ')
            # Выделяем название команды
            cmd = cmd_str.partition(' ')[0]
            # Выделяем строку с  путями
            paths = cmd_str.partition(' ')[2]
            # Разбиваем струку на пути и удалить пустые строки.
            paths = list(filter(None, paths.split(' ')))
            # Вызываем метод обработки команд.
            self.command_processing(cmd, paths)


if __name__ == '__main__':
    hdfs_console = HDFSConsole()
    hdfs_console.inf_cycle()
