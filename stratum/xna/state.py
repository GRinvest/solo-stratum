import asyncio

from config import config
from utils import var_int


class TemplateState:
    # These refer to the block that we are working on
    close = False
    height: int = -1

    timestamp: int = -1

    # The address of the miner that first connects is
    # the one that is used
    address = ''
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

    lock = asyncio.Lock()

    job_counter = 0
    bits_counter = 0
    last_time_reported_hs = 0

    def __repr__(self):
        return f'Height:\t\t{self.height}\nAddress:\t\t{self.address}\nBits:\t\t{self.bits}\nTarget:\t\t{self.target}\nHeader Hash:\t\t{self.headerHash}\nVersion:\t\t{self.version}\nPrevious Header:\t\t{self.prevHash.hex()}\nExtra Txs:\t\t{self.externalTxs}\nSeed Hash:\t\t{self.seedHash.hex()}\nHeader:\t\t{self.header.hex()}\nCoinbase:\t\t{self.coinbase_tx.hex()}\nCoinbase txid:\t\t{self.coinbase_txid.hex()}\nNew sessions:\t\t{self.new_sessions}\nSessions:\t\t{self.all_sessions}'

    def build_block(self, nonce: str, mixHash: str) -> str:
        return self.header.hex() + nonce + mixHash + var_int(
            len(self.externalTxs) + 1).hex() + self.coinbase_tx.hex() + ''.join(self.externalTxs)

    def __init__(self, event):
        self.event = event


EVENT_NEW_BLOCK = asyncio.Event()
state = TemplateState(EVENT_NEW_BLOCK)
