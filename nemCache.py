from binascii import hexlify
from toolbox.hash_converter import convert_to_address as pk2address
import json

def success(x):
	print " [+]",x

class Accounts:
	def __init__(self, db):
		self.db = db
		self.accountsCache = {}
		self._fillAccountsCache()

	def _fillAccountsCache(self):
		def addAcc(x):
			if x[2] == None:
				self.accountsCache[x[1]] = {'id':x[0]}
			else:
				self.accountsCache[x[1]] = {'id':x[0], 'pubKey':hexlify(bytes(x[2]))}
		self.db.accounts(addAcc)
		success("processed accounts")

	def addByPublic(self, pubKey):
		printable = pk2address(pubKey)
		if printable in self.accountsCache:
			acc = self.accountsCache[printable]
			if 'pubKey' in acc:
				return acc['id']

			accId = self.db.updateAccount(acc['id'], pubKey, printable)
		else:
			accId = self.db.addAccount(pubKey, printable)
		self.accountsCache[printable] = {'id':accId, 'pubKey':pubKey}
		return accId

	def addByPrint(self, printable):
		if printable in self.accountsCache:
			return self.accountsCache[printable]['id']
		accId = self.db.addAccountPrint(printable)
		self.accountsCache[printable] = {'id':accId}
		return accId
	

class Delegations:
	def __init__(self, db):
		self.db = db
		self.delegationsCache = {}
		self._fillDelegationsCache()
		
	def _fillDelegationsCache(self):
		self.db.delegations(self.add)
		success("processed delegations")

	def save(self, filename):
		with open(filename, 'w') as f:
			f.write(json.dumps(self.delegationsCache))

	def add(self, tx):
		if tx['mode'] == 1:
			self.delegationsCache[tx['remote_id']] = {'owner':tx['signer_id'], 'start_height':tx['block_height']}
		else:
			if tx['remote_id'] in self.delegationsCache:
				self.delegationsCache[tx['remote_id']]['stop_height'] = tx['block_height']
				return
		
			for rem,ctx in self.delegationsCache.iteritems():
				if ctx['owner'] == tx['signer_id'] and tx['mode'] == 2:
					self.delegationsCache[rem]['stop_height'] = tx['block_height']
					
		#print tx['remote_id'], self.delegationsCache[tx['remote_id']]

	
	def get(self, block):
		""" returns owner of an account """
		signer = block['signer_id']
		if signer not in self.delegationsCache:
			return signer

		ownerCtx = self.delegationsCache[signer]
		if block['height'] >= ownerCtx['start_height']+360:
			if 'stop_height' not in ownerCtx:
				return ownerCtx['owner']

			if block['height'] >= ownerCtx['stop_height']+360:
				return signer
			else:
				return ownerCtx['owner']
		else:
			return signer
	


