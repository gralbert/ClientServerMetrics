import asyncio
import tempfile
import os
import json

storage_path = os.path.join(tempfile.gettempdir(), 'storage.data')
with open(storage_path, 'w') as file:
    json.dump(dict(), file)


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


class ClientServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = self.process_data(data.decode())
        self.transport.write(resp.encode())

    def process_data(self, data):
        try:
            command, information = data.split(maxsplit=1)
        except ValueError:
            return 'error\nwrong command\n\n'

        if command == 'get':
            return self.get(information.strip())
        elif command == 'put':
            return self.put(information.strip())
        else:
            return 'error\nwrong command\n\n'

    def put(self, data):
        try:
            key, value, timestamp = data.split()
        except ValueError:
            return 'error\nwrong command\n\n'

        storage = self.reading()

        if key in storage:
            storage[key][timestamp] = value
        else:
            storage[key] = dict()
            storage[key][timestamp] = value

        self.writing(storage)

        return 'ok\n\n'

    def get(self, key):
        answer = 'ok\n'
        storage = self.reading()

        if key != '*':
            try:
                dict_of_metrics = storage[key]
                for timestamp in sorted(dict_of_metrics):
                    answer += '{} {} {}\n'.format(key,
                                                  dict_of_metrics[timestamp],
                                                  timestamp)
                return answer + '\n'
            except KeyError:
                return 'ok\n\n'

        else:
            try:
                for key in storage:
                    for timestamp in sorted(storage[key]):
                        answer += '{} {} {}\n'.format(key,
                                                      storage[key][timestamp],
                                                      timestamp)
                return answer + '\n'
            except KeyError:
                return 'ok\n\n'

    @staticmethod
    def reading():
        try:
            with open(storage_path, 'r') as f:
                data = json.load(f)
                return data
        except FileNotFoundError:
            return dict()

    @staticmethod
    def writing(data):
        with open(storage_path, 'w') as f:
            json.dump(data, f)


if __name__ == '__main__':
    run_server('127.0.0.1', 8888)

