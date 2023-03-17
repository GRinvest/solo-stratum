import asyncio
import hashlib
import random
from time import time

import base58
import ujson
from loguru import logger

from coindrpc import node
from config import config
from utils import op_push, var_int, merkle_from_txids, dsha256


async def send_msg(writer: asyncio.StreamWriter,
                   method: str | None,
                   params: list | bool,
                   id_: int | None = None,
                   error: list | None = None):
    if method:
        s = ujson.dumps({'jsonrpc': '2.0', 'id': id_, 'method': method, 'params': params})
    else:
        s = ujson.dumps({'jsonrpc': '2.0', 'id': id_, 'error': error, 'result': params})
    s += '\n'
    if not writer.is_closing():
        writer.write(s.encode())
        try:
            await asyncio.wait_for(writer.drain(), timeout=10)
        except asyncio.TimeoutError:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            if writer in state.all_sessions:
                state.all_sessions.remove(writer)


async def send_new_job(writer):
    await send_msg(writer, 'mining.set_target', [state.target])
    await send_msg(writer,
                   'mining.notify',
                   [hex(state.job_counter)[2:],
                    state.headerHash,
                    state.seedHash.hex(),
                    state.target,
                    True,
                    state.height,
                    state.bits])


class TemplateState:
    # These refer to the block that we are working on
    height: int = -1

    timestamp: int = -1

    # The address of the miner that first connects is
    # the one that is used
    address = config.general.mining_address
    timestamp_block_fond = 0
    update_new_job = config.general.update_new_job

    # We store the following in hex because they are
    # Used directly in API to the miner
    bits: str | None = None
    target: str | None = None
    headerHash: str | None = None

    version: int = -1
    prevHash: bytes | None = None
    externalTxs: list[str] = []
    seedHash: bytes | None = None
    header: bytes | None = None
    coinbase_tx: bytes | None = None
    coinbase_txid: bytes | None = None

    current_commitment: str | None = None

    all_sessions: set[asyncio.StreamWriter] = set()

    lock = asyncio.Lock()

    job_counter = 0
    bits_counter = 0
    last_time_reported_hs = 0

    def __repr__(self):
        return f'Height:\t\t{self.height}\nAddress:\t\t{self.address}\nBits:\t\t{self.bits}\nTarget:\t\t{self.target}\nHeader Hash:\t\t{self.headerHash}\nVersion:\t\t{self.version}\nPrevious Header:\t\t{self.prevHash.hex()}\nExtra Txs:\t\t{self.externalTxs}\nSeed Hash:\t\t{self.seedHash.hex()}\nHeader:\t\t{self.header.hex()}\nCoinbase:\t\t{self.coinbase_tx.hex()}\nCoinbase txid:\t\t{self.coinbase_txid.hex()}\nNew sessions:\t\t{self.new_sessions}\nSessions:\t\t{self.all_sessions}'

    def build_block(self, nonce: str, mixHash: str) -> str:
        return self.header.hex() + nonce + mixHash + var_int(
            len(self.externalTxs) + 1).hex() + self.coinbase_tx.hex() + ''.join(self.externalTxs)


state = TemplateState()


class Proxy:
    last_time_reported_hs = 0
    hashrate_dict = {}

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self.user: str | None = None
        self.wallet: str | None = None
        self.worker = 'anonymous'
        self.extra_nonce: str = ''
        self.time_block_fond: int = int(time())

    async def send_msg(self,
                       method: str | None,
                       params: list | bool,
                       id_: int | None = None,
                       error: list | None = None):
        if method:
            s = ujson.dumps({'jsonrpc': '2.0', 'id': id_, 'method': method, 'params': params})
        else:
            s = ujson.dumps({'jsonrpc': '2.0', 'id': id_, 'error': error, 'result': params})
        s += '\n'
        if not self._writer.is_closing():
            self._writer.write(s.encode())
            try:
                await asyncio.wait_for(self._writer.drain(), timeout=10)
            except asyncio.TimeoutError:
                if not self._writer.is_closing():
                    self._writer.close()
                    await self._writer.wait_closed()
                if self._writer in state.all_sessions:
                    state.all_sessions.remove(self._writer)

    async def handle_subscribe(self, msg: dict):
        while True:
            self.extra_nonce = '%0x' % random.getrandbits(4 * 4)
            if len(self.extra_nonce) == 4:
                break
        await self.send_msg(None, [None, self.extra_nonce], msg['id'])

    async def handle_authorize(self, msg: dict):
        user_list = msg['params'][0].split('.')
        self.wallet = user_list[0]
        if len(user_list) == 2:
            self.worker = user_list[1]
        self.user = f"{self.wallet}.{self.worker}"
        await self.send_msg(None, True, msg['id'])
        await send_new_job(self._writer)
        while state.lock.locked():
            await asyncio.sleep(0.01)
        state.all_sessions.add(self._writer)
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
        state_ = state

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
            await self.send_msg(None, False, msg['id'], [22, 'Duplicate share'])
        elif result == 'duplicate-inconclusive':
            logger.error('Valid block but duplicate-inconclusive')
        elif result == 'inconclusive-not-best-prevblk':
            logger.error('Valid block but inconclusive-not-best-prevblk')
        elif result == 'high-hash':
            logger.error('low diff')
            await self.send_msg(None, False, msg['id'], [23, 'Low difficulty share'])
        if result not in (
                None, 'inconclusive', 'duplicate', 'duplicate-inconclusive', 'inconclusive-not-best-prevblk',
                'high-hash'):
            logger.error(res['result'])
            await self.send_msg(None, False, msg['id'], [20, res.get('result')])

        if res.get('result', 0) is None:
            self.time_block_fond = int(time())
            state.timestamp_block_fond = time()
            block_height = int.from_bytes(
                bytes.fromhex(block_hex[(4 + 32 + 32 + 4 + 4) * 2:(4 + 32 + 32 + 4 + 4 + 4) * 2]), 'little',
                signed=False)
            msg_ = f'Found block user {self.user} (may or may not be accepted by the chain): {block_height}'
            logger.success(msg_)
            await self.send_msg(None, True, msg['id'])
            await self.send_msg('client.show_message', [msg_])

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
            if time() - state.last_time_reported_hs > 10 * 60:
                state.last_time_reported_hs = time()
                TTF = difficulty_int * 2 ** 32 / totalHashrate
                logger.debug(
                    f'Total Solo Pool Reported Hashrate: {round(totalHashrate / 1000000, 2)} Mh/s | Estimated time to find: {round(TTF / 60, 2)} minute')
                logger.debug(f'Network Hashrate: {round(networkhashps_int / 1000000000, 2)} Gh/s')
            if hashrate > 0:
                TTF = difficulty_int * 2 ** 32 / hashrate
                await self.send_msg(None, True, msg['id'])
                await self.send_msg('client.show_message',
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
                    await self.send_msg(None, False, None, [20, f'Method {method} not supported'])
                else:
                    logger.error(j)
                    break


async def handle_client(reader, writer):
    """Создание и проверка подключения"""
    proxy = Proxy(reader, writer)
    try:
        await proxy.adapter_handle()
    except Exception as e:
        logger.error(e)
    finally:
        if proxy.user:
            while state.lock.locked():
                await asyncio.sleep(0.01)
            if writer in state.all_sessions:
                state.all_sessions.remove(writer)
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            proxy.hashrate_dict.update({proxy.worker: 0})
            logger.warning(f"worker disconnected {proxy.user}")


async def state_updater():
    try:
        res = await node.getblocktemplate()
        while res.get('code', 0) < 0:
            if res['code'] == -10:
                msg = res['message']
                res = await node.getblockchaininfo()
                logger.warning(f"{msg} | Blocks: {res['result']['blocks']} | Headers: {res['result']['headers']}, sleeping 120 sec...")
            else:
                logger.warning(f'Error getting block template: {res.get("message", "Not found message")}, sleeping 120 sec...')
            await asyncio.sleep(120)
            res = await node.getblocktemplate()
        json_obj = res['result']
        version_int: int = json_obj['version']
        height_int: int = json_obj['height']
        bits_hex: str = json_obj['bits']
        prev_hash_hex: str = json_obj['previousblockhash']
        txs_list: list = json_obj['transactions']
        coinbase_sats_int: int = json_obj['coinbasevalue']
        witness_hex: str = json_obj['default_witness_commitment']
        target_hex: str = json_obj['target']

        ts = int(time())
        new_witness = witness_hex != state.current_commitment
        state.current_commitment = witness_hex
        state.target = target_hex
        state.bits = bits_hex
        state.version = version_int
        state.prevHash = bytes.fromhex(prev_hash_hex)[::-1]

        new_block = False

        # The following will only change when there is a new block.
        # Force update is unnecessary
        if state.height == -1 or state.height != height_int:
            # New block, update everything
            logger.debug(f"New block {height_int - 1}, update state. New target: {target_hex}")
            new_block = True

            # Generate seed hash #
            if state.height == - 1 or height_int > state.height:
                if not state.seedHash:
                    seed_hash = bytes(32)
                    for _ in range(height_int // config.general.kawpow_epoch_length):
                        k = hashlib.new("sha3_256")
                        k.update(seed_hash)
                        seed_hash = k.digest()
                    logger.debug(f'Initialized seedhash to {seed_hash.hex()}')
                    state.seedHash = seed_hash
                elif state.height % config.general.kawpow_epoch_length == 0:
                    # Hashing is expensive, so want use the old val
                    k = hashlib.new("sha3_256")
                    k.update(state.seedHash)
                    seed_hash = k.digest()
                    logger.debug(f'updated seed hash to {seed_hash.hex()}')
                    state.seedHash = seed_hash
            elif state.height > height_int:
                # Maybe a chain reorg?

                # If the difference between heights is greater than how far we are into the epoch
                if state.height % config.general.kawpow_epoch_length - (state.height - height_int) < 0:
                    # We must go back an epoch; recalc
                    seed_hash = bytes(32)
                    for _ in range(height_int // config.general.kawpow_epoch_length):
                        k = hashlib.new("sha3_256")
                        k.update(seed_hash)
                        seed_hash = k.digest()
                    logger.debug(f'Reverted seedhash to {seed_hash}')
                    state.seedHash = seed_hash

            # Done with seed hash #
            state.height = height_int

        # The following occurs during both new blocks & new txs & nothing happens for 60s (magic number)
        if new_block or new_witness or state.timestamp + state.update_new_job < ts:
            # Generate coinbase #

            bytes_needed_sub_1 = 0
            while True:
                if state.height <= (2 ** (7 + (8 * bytes_needed_sub_1))) - 1:
                    break
                bytes_needed_sub_1 += 1

            bip34_height = state.height.to_bytes(bytes_needed_sub_1 + 1, 'little')

            # Note that there is a max allowed length of arbitrary data.
            # I forget what it is (TODO lol) but note that this string is close
            # to the max.
            arbitrary_data = b'Jonathan Livingston Seagull'
            coinbase_script = op_push(len(bip34_height)) + bip34_height + b'\0' + op_push(
                len(arbitrary_data)) + arbitrary_data
            coinbase_txin = bytes(32) + b'\xff' * 4 + var_int(len(coinbase_script)) + coinbase_script + b'\xff' * 4
            if time() - state.timestamp_block_fond > 60 * 60:
                state.update_new_job = config.general.update_new_job
            else:
                state.update_new_job = 120
            vout_to_miner = b'\x76\xa9\x14' + base58.b58decode_check(state.address)[1:] + b'\x88\xac'
            vout_to_devfund = b'\xa9\x14' + base58.b58decode_check("eHNUGzw8ZG9PGC8gKtnneyMaQXQTtAUm98")[1:] + b'\x87'

            # Concerning the default_witness_commitment:
            # https://github.com/bitcoin/bips/blob/master/bip-0141.mediawiki#commitment-structure
            # Because the coinbase tx is '00'*32 in witness commit,
            # We can take what the node gives us directly without changing it
            # (This assumes that the txs are in the correct order, but I think
            # that is a safe assumption)

            witness_vout = bytes.fromhex(witness_hex)

            state.coinbase_tx = (int(1).to_bytes(4, 'little') +
                                 b'\x00\x01' +
                                 b'\x01' + coinbase_txin +
                                 b'\x03' +
                                 int(coinbase_sats_int * 0.9).to_bytes(8, 'little') + op_push(
                        len(vout_to_miner)) + vout_to_miner +
                                 int(coinbase_sats_int * 0.1).to_bytes(8, 'little') + op_push(
                        len(vout_to_devfund)) + vout_to_devfund +
                                 bytes(8) + op_push(len(witness_vout)) + witness_vout +
                                 b'\x01\x20' + bytes(32) + bytes(4))

            coinbase_no_wit = int(1).to_bytes(4, 'little') + b'\x01' + coinbase_txin + b'\x03' + \
                              int(coinbase_sats_int * 0.9).to_bytes(8, 'little') + op_push(
                len(vout_to_miner)) + vout_to_miner + \
                              int(coinbase_sats_int * 0.1).to_bytes(8, 'little') + op_push(
                len(vout_to_devfund)) + vout_to_devfund + \
                              bytes(8) + op_push(len(witness_vout)) + witness_vout + \
                              bytes(4)
            state.coinbase_txid = dsha256(coinbase_no_wit)

            # Create merkle & update txs
            txids = [state.coinbase_txid]
            incoming_txs = []
            for tx_data in txs_list:
                incoming_txs.append(tx_data['data'])
                txids.append(bytes.fromhex(tx_data['txid'])[::-1])
            state.externalTxs = incoming_txs
            merkle = merkle_from_txids(txids)

            # Done create merkle & update txs

            state.header = version_int.to_bytes(4, 'little') + state.prevHash + merkle + ts.to_bytes(4,
                                                                                                     'little') + bytes.fromhex(
                bits_hex)[::-1] + state.height.to_bytes(4, 'little')

            state.headerHash = dsha256(state.header)[::-1].hex()
            state.timestamp = ts

            state.job_counter += 1

            async with state.lock:
                tasks = []
                for writer in state.all_sessions:
                    tasks.append(
                        asyncio.create_task(send_new_job(writer))
                    )
                if len(tasks):
                    await asyncio.gather(*tasks)

    except Exception as e:
        logger.error(f'Error {e}')
        logger.error('Failed to query blocktemplate from node Sleeping for 3 sec.')
        await asyncio.sleep(3)
