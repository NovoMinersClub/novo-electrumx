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
# and warranty status of this software.

'''Transaction-related classes and functions.'''

from collections import namedtuple

from electrumx.lib.hash import double_sha256, hash_to_hex_str, sha256
from electrumx.lib.util import (
    unpack_le_int32_from, unpack_le_int64_from, unpack_le_uint16_from,
    unpack_be_uint16_from,
    unpack_le_uint32_from, unpack_le_uint64_from, pack_le_int32, pack_varint,
    pack_le_uint32, pack_le_int64, pack_varbytes, pack_le_uint64
)
from electrumx.lib.script import OpCodes

ZERO = bytes(32)
MINUS_1 = 4294967295


class Tx(namedtuple("Tx", "version inputs outputs locktime")):
    '''Class representing a transaction.'''

    def serialize(self):
        return b''.join((
            pack_le_int32(self.version),
            pack_varint(len(self.inputs)),
            b''.join(tx_in.serialize() for tx_in in self.inputs),
            pack_varint(len(self.outputs)),
            b''.join(tx_out.serialize() for tx_out in self.outputs),
            pack_le_uint32(self.locktime)
        ))


class TxInput(namedtuple("TxInput", "prev_hash prev_idx script sequence")):
    '''Class representing a transaction input.'''
    def __str__(self):
        script = self.script.hex()
        prev_hash = hash_to_hex_str(self.prev_hash)
        return ("Input({}, {:d}, script={}, sequence={:d})"
                .format(prev_hash, self.prev_idx, script, self.sequence))

    def is_generation(self):
        '''Test if an input is generation/coinbase like'''
        return self.prev_idx == MINUS_1 and self.prev_hash == ZERO

    def serialize(self):
        return b''.join((
            self.prev_hash,
            pack_le_uint32(self.prev_idx),
            pack_varbytes(self.script),
            pack_le_uint32(self.sequence),
        ))


class TxOutput(namedtuple("TxOutput", "value pk_script")):

    def serialize(self):
        return b''.join((
            pack_le_int64(self.value),
            pack_varbytes(self.pk_script),
        ))


class Deserializer(object):
    '''Deserializes blocks into transactions.

    External entry points are read_tx(), read_tx_and_hash(),
    read_tx_and_vsize() and read_block().

    This code is performance sensitive as it is executed 100s of
    millions of times during sync.
    '''

    def __init__(self, binary, start=0):
        assert isinstance(binary, bytes)
        self.binary = binary
        self.binary_length = len(binary)
        self.cursor = start

    def read_tx(self):
        '''Return a deserialized transaction.'''
        #TODO: implement proper 0.3 support
        version = self._read_le_int32()
        inputs = self._read_inputs()
        outputs = self._read_outputs()
        if (self.binary_length - self.cursor) < 4:
            self.cursor = self.binary_length - 4

        return Tx(
            version,  # version
            inputs,    # inputs
            outputs,   # outputs
            self._read_le_uint32()  # locktime
        )

    def read_tx_and_hash(self):
        '''Return a (deserialized TX, tx_hash) pair.

        The hash needs to be reversed for human display; for efficiency
        we process it in the natural serialized order.
        '''
        start = self.cursor
        #return self.read_tx(), double_sha256(self.binary[start:self.cursor])
        the_tx = self.read_tx()
        if the_tx.version == 2:
            return the_tx, self.get_richtransaction(the_tx)
        else:
          return the_tx, double_sha256(self.binary[start:self.cursor])  


    def get_richtransaction(self, tx):
        hashInputs = self.get_hashinputs(tx)
        hashoutputs = self.get_hashoutputs(tx)

        preimage = b''.join((
            pack_le_uint32(tx.version),
            pack_le_int32(len(tx.inputs)),
            hashInputs,
            pack_le_int32(len(tx.outputs)),
            hashoutputs,
            pack_le_uint32(tx.locktime)
        ))
        h = double_sha256(preimage)
        return h
    
    def get_hashinputs(self, tx):
        inputs = b''
        for txin in tx.inputs:
            inputhash = b''.join((
                txin.prev_hash,
                pack_le_uint32(txin.prev_idx),
                sha256(txin.script),
                pack_le_uint32(txin.sequence)
            ))
            inputs = b''.join((
                inputs,
                sha256(inputhash)
                ))
        return sha256(inputs)

    def get_state(self, script):
        pc = len(script)
        # opreturn + state + stateLen + version
        if len(script) < 1+0+4+1:
            return False

        pc -= 5

        stateLen, = unpack_le_uint32_from(script[pc:])
        if len(script) < 1 + stateLen + 4 + 1:
            return False

        pc -= stateLen

        if script[pc - 1] != OpCodes.OP_RETURN:
            return False
        
        return pc

    def get_hashoutputs(self, tx):
        outputs = b''
        for txout in tx.outputs:
            outputhash = b''.join((
                pack_le_uint64(txout.value),
                sha256(txout.pk_script)
                ))
            pc = self.get_state(txout.pk_script)
            if pc:
                outputhash = b''.join((
                    outputhash,
                    sha256(txout.pk_script[0:pc]),
                    sha256(txout.pk_script[pc:len(txout.pk_script)]),
                ))
                print("success")
            outputs = b''.join((
                outputs,
                sha256(outputhash)
            ))
        return sha256(outputs)

    def read_tx_and_vsize(self):
        '''Return a (deserialized TX, vsize) pair.'''
        return self.read_tx(), self.binary_length

    def read_tx_block(self):
        '''Returns a list of (deserialized_tx, tx_hash) pairs.'''
        read = self.read_tx_and_hash
        # Some coins have excess data beyond the end of the transactions
        return [read() for _ in range(self._read_varint())]

    def _read_inputs(self):
        read_input = self._read_input
        inputs = list(filter(None, 
            [read_input() for i in range(self._read_varint())]
        ))
        return inputs

    def _read_input(self):
        cursor = self.cursor
        try:
            return TxInput(
                self._read_nbytes(32),   # prev_hash
                self._read_le_uint32(),  # prev_idx
                self._read_varbytes(),   # script
                self._read_le_uint32()   # sequence
            )
        except AssertionError: #TODO: implement proper 0.3 support
            self.cursor = cursor
            return None

    def _read_outputs(self):
        read_output = self._read_output
        outputs = list(filter(None, 
            [read_output() for i in range(self._read_varint())]
        ))
        return outputs

    def _read_output(self):
        cursor = self.cursor
        try:
            return TxOutput(
                self._read_le_int64(),  # value
                self._read_varbytes(),  # pk_script
            )
        except Exception: #TODO: implement proper 0.3 support
            self.cursor = cursor
            return None

    def _read_byte(self):
        cursor = self.cursor
        self.cursor += 1
        return self.binary[cursor]

    def _read_nbytes(self, n):
        cursor = self.cursor
        self.cursor = end = cursor + n
        assert self.binary_length >= end
        return self.binary[cursor:end]

    def _read_varbytes(self):
        return self._read_nbytes(self._read_varint())

    def _read_varint(self):
        n = self.binary[self.cursor]
        self.cursor += 1
        if n < 253:
            return n
        if n == 253:
            return self._read_le_uint16()
        if n == 254:
            return self._read_le_uint32()
        return self._read_le_uint64()

    def _read_le_int32(self):
        result, = unpack_le_int32_from(self.binary, self.cursor)
        self.cursor += 4
        return result

    def _read_le_int64(self):
        result, = unpack_le_int64_from(self.binary, self.cursor)
        self.cursor += 8
        return result

    def _read_le_uint16(self):
        result, = unpack_le_uint16_from(self.binary, self.cursor)
        self.cursor += 2
        return result

    def _read_be_uint16(self):
        result, = unpack_be_uint16_from(self.binary, self.cursor)
        self.cursor += 2
        return result

    def _read_le_uint32(self):
        result, = unpack_le_uint32_from(self.binary, self.cursor)
        self.cursor += 4
        return result

    def _read_le_uint64(self):
        result, = unpack_le_uint64_from(self.binary, self.cursor)
        self.cursor += 8
        return result
