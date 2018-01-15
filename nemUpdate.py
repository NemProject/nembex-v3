import nemdb
from nemProxy import NemProxy
from nemCache import Accounts,Delegations
from toolbox.hash_converter import convert_to_address as pk2address
import datetime,json,sys,time

def success(x):
	print " [+]",x

class Updater:
	def __init__(self):
		self.nemEpoch = datetime.datetime(2015, 3, 29, 0, 6, 25, 0, None)
		self.db = nemdb.Db()
		self.accounts = Accounts(self.db)
		self.delegations = Delegations(self.db)
		self.p = NemProxy()

	def _calc_unix(self, nemStamp):
		r = self.nemEpoch + datetime.timedelta(seconds=nemStamp)
		return (r - datetime.datetime.utcfromtimestamp(0)).total_seconds()

	def _calc_timestamp(self, timestamp):
		r = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))
		return r

	def calculateFee(self, tx):
		fee = tx['fee']
		if 'signatures' in tx:
			for sig in tx['signatures']:
				fee += sig['fee']

		if 'otherTrans' in tx:
			fee += tx['otherTrans']['fee']
		
			tx['total_fee'] = fee

		return fee

	def _addAccounts(self, srcTx):
		sid = self.accounts.addByPublic(srcTx['signer'])
		srcTx['signer_id'] = sid
		if 'remoteAccount' in srcTx:
			rid = self.accounts.addByPublic(srcTx['remoteAccount'])
			srcTx['remote_id'] = rid
		if 'modifications' in srcTx:
			for mod in srcTx['modifications']:
				cid = self.accounts.addByPublic(mod['cosignatoryAccount'])
				mod['cosignatory_id'] = cid
		if 'recipient' in srcTx:
			rid = self.accounts.addByPrint(srcTx['recipient'])
			srcTx['recipient_id'] = rid

		if 'rentalFeeSink' in srcTx:
			rid = self.accounts.addByPrint(srcTx['rentalFeeSink'])
			srcTx['rentalFeeSink_id'] = rid
			print "fee sink, GOT:", rid
			print "for: ", srcTx

		if 'creationFeeSink' in srcTx:
			rid = self.accounts.addByPrint(srcTx['creationFeeSink'])
			srcTx['creationFeeSink_id'] = rid
			print "fee sink, GOT:", rid
			print "for: ", srcTx
			mosLevy = srcTx['mosaicDefinition']['levy']
			if 'recipient' in mosLevy:
				lid = self.accounts.addByPrint(mosLevy['recipient'])
				mosLevy['recipient_id'] = lid
				print "levy, acct:", rid
				print "for: ", srcTx

	def addAllAccounts(self, tx):
		self._addAccounts(tx)
		# inner multisig tx
		if 'otherTrans' in tx:
			self._addAccounts(tx['otherTrans'])

		# all of signatures
		if 'signatures' in tx:
			for sig in tx['signatures']:
				self._addAccounts(sig)


	def addTxSepcifics(self, block, tx):
		def fixTxData(tx):
			tx['timestamp_unix'] = self._calc_unix(tx['timeStamp'])
			tx['timestamp'] = self._calc_timestamp(tx['timestamp_unix'])
			tx['timestamp_nem'] = tx['timeStamp']
			del tx['timeStamp']

			tx['block_height'] = block['height']
			
		fixTxData(tx)
		if 'otherTrans' in tx:
			itx = tx['otherTrans']
			fixTxData(itx)

		if 'signatures' in tx:
			map(fixTxData, tx['signatures'])

		if tx['type'] == 2049:
			self.delegations.add(tx)

	def processInouts(self, block, txes):
		self.db.addInouts(block, txes)
		owner_id = self.delegations.get(block)
		self.db.addInout(owner_id, block['height'], 3, 0, block['fees'])

	def processBlock(self, block):
		blockHarvesterAddress = pk2address(block['signer'])

		block['timestamp_unix'] = self._calc_unix(block['timeStamp'])
		block['timestamp'] = self._calc_timestamp(block['timestamp_unix'])
		block['timestamp_nem'] = block['timeStamp']
		del block['timeStamp']

		sid = self.accounts.addByPublic(block['signer'])
		block['signer_id'] = sid
		
		feesTotal = 0
		
		txes = block["transactions"]
		del block['transactions']
		success("processing block[{}] : {} : {}".format(block['height'], blockHarvesterAddress, block['hash']))
		
		# first add all accounts
		for tx in txes:
			feesTotal += self.calculateFee(tx);
			self.addAllAccounts(tx)

		# commit block and accounts
		block['fees'] = feesTotal
		block['tx_count'] = len(txes)
		dbBlock = self.db.findBlock(block['height'])
		if dbBlock == None:
			blockId = self.db.addBlock(block)
			self.db.commit()
		else:
			blockId = dbBlock[0]
		print "BLOCK ID:", blockId
	
		# fix fields and cache delegation info
		for tx in txes:
			self.addTxSepcifics(block, tx)

		# add txes to db
		self.db.processTxes(block, txes)
		self.processInouts(block, txes)

		self.db.commit()


	def run(self):
		self.processedInLastRun = 0
		while True:
			self._run()
			if self.processedInLastRun:
				success("saving delegations")
				self.delegations.save('delegations.json')
			else:
				success("falling asleep")
				time.sleep(10)
			self.processedInLastRun = 0
			
			
	def _run(self):
		last = self.p.getLastHeight()
		localLast = self.db.getLastHeight()
		print('last height vs nembex height', last, localLast)

		for i in xrange(localLast+1,last-3):
			print('height', i)
			self.processedInLastRun += 1

			data = self.p.getBlockAt(i)
			if data == None:
				break
			with open('../blocks/b'+str(i)+'.json', 'w') as f:
				f.write(json.dumps(data))
			# fixes for /local/block/at api
			block = data['block']
			block['hash'] = data['hash']
			block['difficulty'] = data['difficulty']
			def fixTx(a):
				tx = a['tx']
				tx['hash'] = a['hash']
				if 'innerHash' in a:
					tx['otherTrans']['hash'] = a['innerHash']
				return tx
			block['transactions'] = map(fixTx, data['txes'])
			self.processBlock(block)

u = Updater()
u.run()
