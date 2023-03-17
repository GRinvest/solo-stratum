import asyncio
import importlib.util
import pathlib
import sys

from loguru import logger

from config import config

BASE_DIR = pathlib.Path(__file__).parent
COIN = config.general.coin.lower()


def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MODULE = module_from_file('server', f"{BASE_DIR}/stratum/{COIN}/server.py")


async def run_proxy():
    server = await asyncio.start_server(
        MODULE.handle_client,
        config.server.host,
        config.server.port)
    logger.success(f'Proxy server is running on port {config.server.port}')
    async with server:
        await server.serve_forever()


async def execute():
    while True:
        logger.success('Running Session Program')
        try:
            await run_proxy()
        except Exception as e:
            logger.exception(e)


if __name__ == '__main__':
    logger.remove()
    logger.add(sys.stderr,
               colorize=True,
               format="{time:DD-MM-YYYY at HH:mm:ss} - <level>{message}</level>")
    logger.add("Stratum_{time}.log", rotation="10 MB", enqueue=True)

    try:
        asyncio.run(execute())
    except KeyboardInterrupt:
        pass
    finally:
        logger.warning('Closed Session Program')
