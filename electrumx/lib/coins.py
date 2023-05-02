# Copyright (c) 2016-2017, Neil Booth
# Copyright (c) 2017, the ElectrumX authors
#
# All rights reserved.
#
# The MIT License (MIT)
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

'''Module providing coin abstraction.

Anything coin-specific should go in this file and be subclassed where
necessary for appropriate handling.
'''

from collections import namedtuple
import re
from decimal import Decimal
from hashlib import sha256

from electrumx.lib import util
from electrumx.lib.hash import Base58, double_sha256, hash_to_hex_str, tagged_sha256d
from electrumx.lib.hash import HASHX_LEN
from electrumx.lib.script import ScriptPubKey
import electrumx.lib.tx as lib_tx
import electrumx.server.block_processor as block_proc
from electrumx.server import daemon
from electrumx.server.session import ElectrumX

Block = namedtuple("Block", "raw header transactions")


class CoinError(Exception):
    '''Exception raised for coin-related errors.'''


class Novo:
    NAME = "Novo"
    SHORTNAME = "NVO"
    NET = "mainnet"
    REORG_LIMIT = 200
    RPC_URL_REGEX = re.compile('.+@(\\[[0-9a-fA-F:]+\\]|[^:]+)(:[0-9]+)?')
    VALUE_PER_COIN = 100000000
    SESSIONCLS = ElectrumX
    DEFAULT_MAX_SEND = 10000000000
    DESERIALIZER = lib_tx.Deserializer
    DAEMON = daemon.Daemon
    BLOCK_PROCESSOR = block_proc.BlockProcessor
    P2PKH_VERBYTE = bytes.fromhex("19")
    P2SH_VERBYTES = [bytes.fromhex("08")]
    RPC_PORT = 8665
    GENESIS_HASH = '0000000000b3de1ef5bd7c20708dbafc3df0441877fa4a59cda22b4c2d4f39ce'  
    GENESIS_ACTIVATION = 100_000_000
    PEER_DEFAULT_PORTS = {'t': '50001', 's': '50002'}
    PEERS = [
        "electron-novo.com:50012",
        "electrumx.novochain.ovh:50012",
        "electrumx1.novochain.ovh:50012",
        "electrumx2.novochain.ovh:50012
        "electrumx3.novochain.ovh:50012",
        "electrumx4.novochain.ovh:50012",
        "electrumx5.novochain.ovh:50012",
    ]

    TX_COUNT = 1000
    TX_COUNT_HEIGHT = 2000
    TX_PER_BLOCK = 10
    CHAIN_SIZE = 1_623_944_264_227
    CHAIN_SIZE_HEIGHT = 709_728
    AVG_BLOCK_SIZE = 150_000_000

    @classmethod
    def max_fetch_blocks(cls, height):
        if height < 130000:
            return 1000
        return 100

    @classmethod
    def header_hash(cls, header):
        '''Given a header return hash'''
        return tagged_sha256d(header)

    @classmethod
    def header_prevhash(cls, header):
        '''Given a header return previous hash'''
        return header[4:36]

    @classmethod
    def block(cls, raw_block):
        '''Return a Block namedtuple given a raw block and its height.'''
        header = raw_block[:80]
        txs = cls.DESERIALIZER(raw_block, start=len(header)).read_tx_block()
        return Block(raw_block, header, txs)
