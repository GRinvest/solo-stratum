from typing import Optional

from pydantic import BaseModel


class General(BaseModel):
    coin: Optional[str] = 'XNA'
    kawpow_epoch_length: Optional[int] = 7500
    mining_address: Optional[str] = 'NdkLagwpZTmuGYHWL2DrhC5y38ZDcwZn4x'
    update_new_job: Optional[int] = 45


class Server(BaseModel):
    host: Optional[str] = '0.0.0.0'
    port: Optional[int] = 9755


class Coind(BaseModel):
    rpc_host: Optional[str] = '127.0.0.1'
    rpc_port: Optional[int] = 19001
    rpc_user: Optional[str] = 'GRinvest'
    rpc_password: Optional[str] = 'GR458099'
    zmq_port: Optional[int] = 19002


class CoindWallet(BaseModel):
    rpc_host: Optional[str] = '127.0.0.1'
    rpc_port: Optional[int] = 19004
    rpc_user: Optional[str] = 'GRinvest'
    rpc_password: Optional[str] = 'GR458099'


class Config(BaseModel):
    general: General
    server: Server
    coind: Coind
    coind_wallet: CoindWallet

