'''
Distributed under the MIT License, see accompanying file LICENSE.txt
'''
import base64
import sys

sys.path.insert(0, 'python-sha3')
from python_sha3 import *

def address_is_valid(address, isTestNet=True):
    try:
        b32Address = base64.b32decode(address)
    except:
        return False
    
    networkRipe = b32Address[:-4]
    checksum = b32Address[-4:]
    
    s = sha3_256()
    s.update(networkRipe)
    networkRipeHash = s.digest()
    
    return networkRipeHash[0:4] == checksum and networkRipe[0] == "\x98" if isTestNet else "\x68"
