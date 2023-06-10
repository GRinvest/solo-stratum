import asyncio
import zmq
import zmq.asyncio
from config import config
from loguru import logger
from .state import state


async def run():
    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 0)
    socket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
    socket.connect("tcp://127.0.0.1:%i" % config.coind.zmq_port)
    try:
        while True:
            _, body, _ = await socket.recv_multipart()
            state.event.set()
            logger.info(f'NEW BLOCK {body.hex()}')
            state.event.clear()
    finally:
        ctx.destroy()

