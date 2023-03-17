import ujson
from aiohttp import ClientSession

from config import config


class Coind:
    def __init__(self, username, password, port=5996, host='127.0.0.1'):
        self.url = f'http://{username}:{password}@{host}:{port}'
        self.id = 0

    def __getattr__(self, name):
        if name.startswith('__') and name.endswith('__'):
            # Python internal stuff
            raise AttributeError

        async def ret(*args):
            self.id += 1
            data = {
                'method': name,
                'params': list(args),
                'id': self.id,
                'jsonrpc': '2.0',
            }
            async with ClientSession() as session:
                async with session.post(self.url,
                                        headers={'Content-Type': 'application/json'},
                                        data=ujson.dumps(data)) as resp:
                    try:
                        json_obj = await resp.json()
                        if json_obj.get('error', None):
                            return json_obj.get('error', None)
                    except Exception as e:
                        print(e)
                    else:
                        return json_obj

        return ret


node = Coind(config.coind.rpc_user,
             config.coind.rpc_password,
             config.coind.rpc_port,
             config.coind.rpc_host)
