'''
Distributed under the MIT License, see accompanying file LICENSE.txt
'''
import base64
import hashlib
import sys
import binascii

sys.path.insert(0, 'python-sha3')
from python_sha3 import *
from config import config

def convert_to_address(signer):
    pubkey = binascii.unhexlify(signer)
    
    s = sha3_256() #hashlib.new('sha3_256')
    s.update(pubkey)
    sha3_pubkey = s.digest()
    
    h = hashlib.new('ripemd160')
    h.update(sha3_pubkey)
    ripe = h.digest()
    
    if config.network == 'testnet':
        version = "\x98" + ripe
    else:
        version = "\x68" + ripe
    
    #s2 = hashlib.new('sha3_256')
    s2 = sha3_256() #hashlib.new('sha3_256')
    s2.update(version)
    checksum = s2.digest()[0:4]
    
    address = base64.b32encode(version + checksum)
    
    return address
   
