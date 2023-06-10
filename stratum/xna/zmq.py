import asyncio
import zmq
import zmq.asyncio
from config import config
import struct


async def run():
    ctx = zmq.asyncio.Context()
    socket = ctx.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 0)
    socket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
    socket.connect("tcp://127.0.0.1:%i" % config.coind.zmq_port)
    topic, body, seq = await socket.recv_multipart()
    sequence = "Unknown"
    if len(seq) == 4:
        sequence = str(struct.unpack('<I', seq)[-1])
    if topic == b"hashblock":
        print('- HASH BLOCK (' + sequence + ') -')
        print(body.hex())
