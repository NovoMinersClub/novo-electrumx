from decimal import Decimal
from electrumx.lib import coins
from electrumx.lib.script import Script

class Novo(coins.BitcoinMixin, coins.Coin):
    NAME = "Novo"
    SHORTNAME = "NOVO"
    NET = "mainnet"
    P2PKH_VERBYTE = bytes.fromhex("76")
    P2SH_VERBYTES = [bytes.fromhex("05")]
    WIF_BYTE = bytes.fromhex("80")
    GENESIS_HASH = ('000000b97664fd7f0486b8fd6f2131ffed2a7b689f67b1728fa7a0b6cacf6196')
    TX_COUNT = 1
    TX_COUNT_HEIGHT = 1
    TX_PER_BLOCK = 1
    RPC_PORT = 8665
    REORG_LIMIT = 800

    @classmethod
    def header_hash(cls, header):
        '''Given a header return the hash.'''
        import novo
        return novo.header_hash(header)

    @classmethod
    def header2hash(cls, header):
        '''Given a header return the hash.'''
        return cls.header_hash(header)

    @classmethod
    def electrum_header(cls, header, height):
        '''Return the raw Electrum header for the given header.'''
        h = cls.header(header, height)
        return h

    @classmethod
    def hashX_from_pubkey(cls, pubkey):
        return Script.P2PKH_script(pubkey).hash160()

    @staticmethod
    def total_supply(height):
        # The total supply calculation for Novo
        subsidy = 50 * 10**8
        halvings = height // 210000

        if halvings >= 64:
            return 0

        for _ in range(halvings):
            subsidy //= 2

        return subsidy

    @classmethod
    def tx_outputs(cls, tx, *, header=None, lines=None):
        for n, (script_pub_key, value) in enumerate(tx.outputs):
            hashX = cls.hashX_from_script(script_pub_key)
            if hashX:
                yield hashX, value, n

    @classmethod
    def history_len(cls, hist):
        '''Return the raw history length in number of entries.'''
        return len(hist) // 13

    def base_units(self, value):
        return Decimal(value) / self.COIN
