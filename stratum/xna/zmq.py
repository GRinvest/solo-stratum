import zmq
import zmq.asyncio
from loguru import logger

from config import config
from .state import EVENT_NEW_BLOCK


async def run():
    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 0)
    socket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
    socket.connect("tcp://127.0.0.1:%i" % config.coind.zmq_port)
    try:
        while True:
            _, body, _ = await socket.recv_multipart()
            EVENT_NEW_BLOCK.set()
            logger.info(f'NEW BLOCK {body.hex()}')
            EVENT_NEW_BLOCK.clear()
    finally:
        ctx.destroy()

