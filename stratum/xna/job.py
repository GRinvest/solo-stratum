import asyncio
import hashlib
import random
from time import time

import base58
from loguru import logger

from coindrpc import node
from config import config
from utils import op_push, var_int, dsha256, merkle_from_txids
from stratum.xna.connector import manager
from stratum.xna.state import TemplateState
from .state import EVENT_NEW_BLOCK


async def state_updater(state: TemplateState, writer: asyncio.StreamWriter):
    try:
        res = await node.getblocktemplate()
        while res.get('code', 0) < 0:
            if res['code'] == -10:
                msg = res['message']
                res = await node.getblockchaininfo()
                logger.warning(
                    f"{msg} | Blocks: {res['result']['blocks']} | Headers: {res['result']['headers']}, sleeping 120 sec...")
            else:
                logger.warning(
                    f'Error getting block template: {res.get("message", "Not found message")}, sleeping 120 sec...')
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
        state.target = target_hex
        state.bits = bits_hex
        state.version = version_int
        state.prevHash = bytes.fromhex(prev_hash_hex)[::-1]

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
            state.update_new_job = random.randint(45, 80)
        else:
            state.update_new_job = random.randint(80, 120)
        address_ = state.address if state.address != '' else config.general.mining_address
        vout_to_miner = b'\x76\xa9\x14' + base58.b58decode_check(address_)[1:] + b'\x88\xac'

        # Concerning the default_witness_commitment:
        # https://github.com/bitcoin/bips/blob/master/bip-0141.mediawiki#commitment-structure
        # Because the coinbase tx is '00'*32 in witness commit,
        # We can take what the node gives us directly without changing it
        # (This assumes that the txs are in the correct order, but I think
        # that is a safe assumption)

        witness_vout = bytes.fromhex(witness_hex)

        state.coinbase_tx = (int(1).to_bytes(4, 'little') + \
                             b'\x00\x01' + \
                             b'\x01' + coinbase_txin + \
                             b'\x02' + \
                             coinbase_sats_int.to_bytes(8, 'little') + op_push(len(vout_to_miner)) + vout_to_miner + \
                             bytes(8) + op_push(len(witness_vout)) + witness_vout + \
                             b'\x01\x20' + bytes(32) + bytes(4))

        coinbase_no_wit = int(1).to_bytes(4, 'little') + \
                          b'\x01' + coinbase_txin + \
                          b'\x02' + \
                          coinbase_sats_int.to_bytes(8, 'little') + op_push(len(vout_to_miner)) + vout_to_miner + \
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
        if state.address != '':
            await manager.send_new_job(writer, state)

    except Exception as e:
        logger.error(f'Error {e}')
        logger.error('Failed to query blocktemplate from node Sleeping for 3 sec.')
        await asyncio.sleep(3)


async def job_manager(state: TemplateState, writer: asyncio.StreamWriter):
    while not state.close:
        await state_updater(state, writer)
        await EVENT_NEW_BLOCK.wait()
