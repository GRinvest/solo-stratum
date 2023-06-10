import asyncio
import zmq
import zmq.asyncio
from config import config
from loguru import logger


async def run():
    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 0)
    socket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
    socket.connect("tcp://127.0.0.1:%i" % config.coind.zmq_port)
    while True:
        _, body, _ = await socket.recv_multipart()
        logger.info(f'NEW BLOCK {body.hex()}')

