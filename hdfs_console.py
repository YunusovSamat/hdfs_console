import os
import requests


class HDFSCommands:
    def __init__(self):
        self.url = str()
        # Установка текущего пути в hdfs.
        self.hdfs_path = list()
        # Установка текущего пути в локальной fs.
        os.chdir('/')
        self.params = dict()
        self.method = str()

    def connect(self, host='localhost', port='50070', user='dr.who') -> bool:
        self.url = f'http://{host}:{port}/webhdfs/v1'
        # Словарь для формирования параметров.
        self.params = {'user.name': user, 'op': 'LISTSTATUS'}
        self.method = 'GET'
        print(f'host={host};\tport={port};\tuser={user}')
        try:
            self._http_request('/')
        except requests.ConnectionError:
            print('\tWrong host or port')
            return False
        else:
            print('\tConnect.')
            return True

    # Метод для добавления относительного пути (path) к текущему.
    def _make_paths(self, path: str) -> list:
        # Если путь абсолютный, то создать пустой список,
        # иначе получить текущий путь.
        full_list_path = list() if os.path.isabs(path) else self.hdfs_path[:]
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
    def _http_request(self, path: str, data=None) -> (object, str):
        full_path = '/' + '/'.join(self._make_paths(path))
        return requests.request(self.method, self.url + full_path,
                                params=self.params, data=data), full_path

    # Метод для обработки командных операций.
    def command_processing(self, line: str):
        # Выделяем название команды
        op = line.partition(' ')[0]
        # Выделяем строку с  путями
        paths = line.partition(' ')[2]
        # Разбиваем струку на пути и удалить пустые строки.
        paths_list = list(filter(None, paths.split(' ')))
        # Создание каталогов
        if op == 'mkdir':
            # Если нет пути, то завершить операцию.
            if not len(paths_list):
                print('The number of arguments is less than one')
                return
            self.params['op'] = 'MKDIRS'
            self.method = 'PUT'
            # Перебираем все пути
            for path in paths_list:
                # Получаем ответ от сервера и полный путь каталога.
                response, full_path = self._http_request(path)
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
                if len(paths_list) == 1:
                    paths_list.append('./')
                elif len(paths_list) != 2:
                    print('The number of arguments is not two')
                    return
                self.params['op'] = 'CREATE'
                op_write = 'Created'
                self.method = 'PUT'
                # self.params['permission'] = '777'
            else:
                if len(paths_list) != 2:
                    print('The number of arguments is not two')
                    return
                self.params['op'] = 'APPEND'
                op_write = 'Append'
                self.method = 'POST'
            # Если локальный объкт является файлом и существует.
            if os.path.isfile(paths_list[0]):
                # Открыть файл на чтение в бинарном режиме.
                with open(paths_list[0], 'rb') as file:
                    # Если у hdfs пути нет названия файла
                    if not os.path.basename(paths_list[1]):
                        # Присвоить локальное название к hdfs пути.
                        paths_list[1] += os.path.basename(paths_list[0])
                    response, full_path = self._http_request(paths_list[1],
                                                            data=file)
                print(full_path, ':', sep='')
                if response.reason in ('Created', 'OK'):
                    print('\t', op_write, sep='')
                else:
                    print('\t', response.reason, sep='')
                    print(response.text)
            else:
                print(paths_list[0],
                      ':\n\tNot found or object not file', sep='')
        # Передача данных сервера на локальную машину.
        elif op == 'get':
            # Если прописан только путь сервера,
            # то добавить текущий локальный путь.
            if len(paths_list) == 1:
                paths_list.append('./')
            elif len(paths_list) != 2:
                print('The number of arguments is not two')
                return
            self.params['op'] = 'OPEN'
            self.method = 'GET'
            # Отделяю путь от имени файла.
            path_dir, path_name = os.path.split(paths_list[1])
            # Если путь каталога не существует, то завершить операцию
            if path_dir and (not os.path.isdir(path_dir)):
                print(paths_list[1], ':\n\tNot found', sep='')
                return
            # Если у локального пути нет названия файла
            elif not path_name:
                # Присвоить hdfs название к локальному пути.
                paths_list[1] += os.path.basename(paths_list[0])
            response, full_path = self._http_request(paths_list[0])
            if response.reason == 'OK':
                print(paths_list[1], ':', sep='')
                # открыть или создать файл на запись.
                with open(paths_list[1], 'w') as file:
                    # Записать в локальный файл данные из серверного файла.
                    file.write(response.text)
                    print('\tRecord')
            else:
                print(full_path, ':\n\t', response.reason, sep='')
        # Удалине файлов и каталогов с сервра.
        elif op == 'delete':
            # Если нет путей, то завершить операцию.
            if not len(paths_list):
                print('The number of arguments is less than one')
                return
            self.params['op'] = 'DELETE'
            self.method = 'DELETE'
            # Перебираем все пути.
            for path in paths_list:
                response, full_path = self._http_request(path)
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
            self.params['op'] = 'LISTSTATUS'
            self.method = 'GET'
            # Если нет пути,то установить текущий.
            paths_list = paths_list if paths_list else ['./']
            # Перебираем все пути.
            for path in paths_list:
                response, full_path = self._http_request(path)
                print(full_path, ':', sep='')
                if response.reason == 'OK':
                    # Вывод всех файлов и каталогов в данном каталоге.
                    for file in response.json()['FileStatuses']['FileStatus']:
                        print('\t', file['pathSuffix'], sep='')
                else:
                    print('\t', response.reason, sep='')
        # Установка текущего пути сервера.
        elif op == 'cd':
            if len(paths_list) == 1:
                self.params['op'] = 'GETFILESTATUS'
                self.method = 'GET'
                response, full_path = self._http_request(paths_list[0])
                if response.reason == 'OK':
                    if response.json()['FileStatus']['type'] == 'DIRECTORY':
                        self.hdfs_path = list(
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
            paths_list = paths_list if paths_list else ['./']
            # Перебираем все пути.
            for path in paths_list:
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
            if len(paths_list) == 1:
                print(paths_list[0])
                # Если путь является каталогом и существует
                if os.path.isdir(paths_list[0]):
                    # Установить как текущий каталог
                    os.chdir(paths_list[0])
                else:
                    print('Path not found or object not directory')
            else:
                print('The number of arguments is not one')
        else:
            print('Command not found')
            return


class HDFSConsole(HDFSCommands):
    def __init__(self, user='samat'):
        super().__init__()
        self.hdfs_path = ['user', 'samat']
        os.chdir('/home/samat')
        self.inf_cycle()

    # Метод для бесконечного ввода команд.
    def inf_cycle(self):
        break_bool = False
        while not break_bool:
            data = input('Input host port user:\n>>> ')
            if data in ('Exit', 'exit', 'quit'):
                break
            if len(data.split()) == 3:
                host, port, user = data.split()
                break_bool = self.connect(host, port, user)
        else:
            while True:
                cmd_line = input('>>> ')
                if cmd_line in ('Exit', 'exit', 'quit'):
                    break
                self.command_processing(cmd_line)


if __name__ == '__main__':
    console = HDFSConsole()
