import asyncio

import ujson

from stratum.evr.state import TemplateState


class ConnectorManager:

    def __init__(self):
        self.active_connectors: set[asyncio.StreamWriter] = set()

    async def connect(self, writer: asyncio.StreamWriter, message: dict):
        await self.send_personal_message(writer, None, True, message['id'])
        self.active_connectors.add(writer)

    async def disconnect(self, writer):
        if writer in self.active_connectors:
            self.active_connectors.remove(writer)
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()

    async def send_personal_message(self,
                                    writer: asyncio.StreamWriter,
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
                if writer in self.active_connectors:
                    self.active_connectors.remove(writer)

    async def send_new_job(self, writer: asyncio.StreamWriter, state: TemplateState):
        await self.send_personal_message(writer, 'mining.set_target', [state.target])
        await self.send_personal_message(writer,
                                         'mining.notify',
                                         [hex(state.job_counter)[2:],
                                          state.headerHash,
                                          state.seedHash.hex(),
                                          state.target,
                                          True,
                                          state.height,
                                          state.bits])


manager = ConnectorManager()
