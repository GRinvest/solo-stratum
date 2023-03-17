from hashlib import sha256


def op_push(i: int) -> bytes:
    if i < 0x4C:
        return i.to_bytes(1, 'little')
    elif i <= 0xff:
        return b'\x4c' + i.to_bytes(1, 'little')
    elif i <= 0xffff:
        return b'\x4d' + i.to_bytes(2, 'little')
    else:
        return b'\x4e' + i.to_bytes(4, 'little')


def dsha256(b):
    return sha256(sha256(b).digest()).digest()


def merkle_from_txids(txids: list[bytes]):
    # https://github.com/maaku/python-bitcoin/blob/master/bitcoin/merkle.py
    if not txids:
        return dsha256(b'')
    if len(txids) == 1:
        return txids[0]
    while len(txids) > 1:
        txids.append(txids[-1])
        txids = list(dsha256(l + r) for l, r in zip(*(iter(txids),) * 2))
    return txids[0]


def var_int(i: int) -> bytes:
    # https://en.bitcoin.it/wiki/Protocol_specification#Variable_length_integer
    # https://github.com/bitcoin/bitcoin/blob/efe1ee0d8d7f82150789f1f6840f139289628a2b/src/serialize.h#L247
    # "CompactSize"
    assert i >= 0, i
    if i < 0xfd:
        return i.to_bytes(1, 'little')
    elif i <= 0xffff:
        return b'\xfd' + i.to_bytes(2, 'little')
    elif i <= 0xffffffff:
        return b'\xfe' + i.to_bytes(4, 'little')
    else:
        return b'\xff' + i.to_bytes(8, 'little')


def lookup_old_state(queue, id_: str):
    return queue[1].get(id_, None)
