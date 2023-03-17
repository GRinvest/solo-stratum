import asyncio
import random
from time import time

import ujson
from loguru import logger

from coindrpc import node
from .connector import manager
from .state import TemplateState
from .job import job_manager


class Proxy:
    last_time_reported_hs = 0
    hashrate_dict = {}

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, state: TemplateState):
        self._reader = reader
        self._writer = writer
        self.state = state
        self.user: str | None = None
        self.wallet: str | None = None
        self.worker = 'anonymous'
        self.extra_nonce: str = ''
        self.time_block_fond: int = int(time())

    async def handle_subscribe(self, msg: dict):
        while True:
            self.extra_nonce = '%0x' % random.getrandbits(4 * 4)
            if len(self.extra_nonce) == 4:
                break
        await manager.send_personal_message(self._writer, None, [None, self.extra_nonce], msg['id'])

    async def handle_authorize(self, msg: dict):
        while self.state.job_counter == 0:
            await asyncio.sleep(0.1)
        user_list = msg['params'][0].split('.')
        self.wallet = user_list[0]
        self.state.address = self.wallet
        if len(user_list) == 2:
            self.worker = user_list[1]
        self.user = f"{self.wallet}.{self.worker}"
        await manager.connect(self._writer, msg)
        logger.success(f"User {self.user} connected | ExtraNonce: {self.extra_nonce}")

    async def handle_submit(self, msg: dict):

        job_id = msg['params'][1]
        nonce_hex = msg['params'][2]
        header_hex = msg['params'][3]
        mixhash_hex = msg['params'][4]

        logger.success(f'Possible solution user: {self.user}')
        logger.info(job_id)
        logger.info(header_hex)

        # We can still propogate old jobs; there may be a chance that they get used
        state_ = self.state

        if nonce_hex[:2].lower() == '0x':
            nonce_hex = nonce_hex[2:]
        nonce_hex = bytes.fromhex(nonce_hex)[::-1].hex()
        if mixhash_hex[:2].lower() == '0x':
            mixhash_hex = mixhash_hex[2:]
        mixhash_hex = bytes.fromhex(mixhash_hex)[::-1].hex()

        block_hex = state_.build_block(nonce_hex, mixhash_hex)
        logger.info(block_hex)
        res = await node.submitblock(block_hex)
        logger.info(res)
        result = res.get('result', None)
        if result == 'inconclusive':
            # inconclusive - valid submission but other block may be better, etc.
            logger.error('Valid block but inconclusive')
        elif result == 'duplicate':
            logger.error('Valid block but duplicate')
            await manager.send_personal_message(self._writer, None, False, msg['id'], [22, 'Duplicate share'])
        elif result == 'duplicate-inconclusive':
            logger.error('Valid block but duplicate-inconclusive')
        elif result == 'inconclusive-not-best-prevblk':
            logger.error('Valid block but inconclusive-not-best-prevblk')
        elif result == 'high-hash':
            logger.error('low diff')
            await manager.send_personal_message(self._writer, None, False, msg['id'], [23, 'Low difficulty share'])
        if result not in (
                None, 'inconclusive', 'duplicate', 'duplicate-inconclusive', 'inconclusive-not-best-prevblk',
                'high-hash'):
            logger.error(res['result'])
            await manager.send_personal_message(self._writer, None, False, msg['id'], [20, res.get('result')])

        if res.get('result', 0) is None:
            self.time_block_fond = int(time())
            self.state.timestamp_block_fond = time()
            block_height = int.from_bytes(
                bytes.fromhex(block_hex[(4 + 32 + 32 + 4 + 4) * 2:(4 + 32 + 32 + 4 + 4 + 4) * 2]), 'little',
                signed=False)
            msg_ = f'Found block user {self.user} (may or may not be accepted by the chain): {block_height}'
            logger.success(msg_)
            await manager.send_personal_message(self._writer, None, True, msg['id'])
            await manager.send_personal_message(self._writer, 'client.show_message', [msg_])

    async def handle_eth_submitHashrate(self, msg: dict):
        res = await node.getmininginfo()
        json_obj = res['result']
        difficulty_int: int = json_obj['difficulty']
        networkhashps_int: int = json_obj['networkhashps']

        hashrate = int(msg['params'][0], 16)
        self.hashrate_dict.update({self.worker: hashrate})
        totalHashrate = 0
        for x, y in self.hashrate_dict.items():
            totalHashrate += y
        if totalHashrate != 0:
            if time() - self.state.last_time_reported_hs > 10 * 60:
                self.state.last_time_reported_hs = time()
                TTF = difficulty_int * 2 ** 32 / totalHashrate
                logger.debug(
                    f'Total Solo Pool Reported Hashrate: {round(totalHashrate / 1000000, 2)} Mh/s | Estimated time to find: {round(TTF / 60, 2)} minute')
                logger.debug(f'Network Hashrate: {round(networkhashps_int / 1000000000, 2)} Gh/s')
            if hashrate > 0:
                TTF = difficulty_int * 2 ** 32 / hashrate
                await manager.send_personal_message(self._writer, None, True, msg['id'])
                await manager.send_personal_message(self._writer, 'client.show_message',
                                                    [f'Estimated time to find: {round(TTF / 3600, 2)} hours'])
                if time() - self.last_time_reported_hs > 60 * 60:
                    self.last_time_reported_hs = time()
                    logger.debug(f'Worker {self.worker} Reported Hashrate: {round(hashrate / 1000000, 2)} Mh/s ')

    async def adapter_handle(self):
        while not self._reader.at_eof():
            try:
                data = await asyncio.wait_for(self._reader.readline(), timeout=10)
                if not data:
                    break
                j: dict = ujson.loads(data)
            except (TimeoutError, asyncio.TimeoutError):
                if time() - self.time_block_fond > 60 * 60:
                    break
                else:
                    continue
            except (ValueError, ConnectionResetError):
                break
            else:
                method = j.get('method')
                if method == 'mining.subscribe':
                    await self.handle_subscribe(j)
                elif method == 'mining.authorize':
                    await self.handle_authorize(j)
                elif method == 'mining.submit':
                    await self.handle_submit(j)
                elif method == 'eth_submitHashrate':
                    await self.handle_eth_submitHashrate(j)
                elif method is not None:
                    await manager.send_personal_message(self._writer, None, False, None,
                                                        [20, f'Method {method} not supported'])
                else:
                    logger.error(j)
                    break


async def handle_client(reader, writer):
    """Создание и проверка подключения"""
    state = TemplateState()
    proxy = Proxy(reader, writer, state)
    try:
        await asyncio.wait([
            proxy.adapter_handle(),
            job_manager(state, writer)
        ], return_when=asyncio.FIRST_COMPLETED)
    except Exception as e:
        logger.error(e)
    finally:
        if proxy.user:
            await manager.disconnect(writer)
            proxy.hashrate_dict.update({proxy.worker: 0})
            logger.warning(f"worker disconnected {proxy.user}")
