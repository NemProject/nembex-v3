import psycopg2
from binascii import hexlify
from psycopg2.extras import RealDictConnection
from config import config

def tobin(x):
	return bytearray(x.decode('hex'))

class Db:
	def __init__(self, retDict=False):
		if retDict:
			self.conn = RealDictConnection(config.connection_string)
		else:
			self.conn = psycopg2.connect(config.connection_string)
			self.createTables()
	
	def commit(self):
		self.conn.commit()

	def accounts(self, fun):
		cur = self.conn.cursor()
		cur.execute("SELECT * FROM accounts")
		for r in cur:
			fun(r)
		cur.close()

	def delegations(self, fun):
		cur = self.conn.cursor()
		cur.execute("SELECT block_height,signer_id,remote_id,mode FROM delegates")
		for r in cur:
			fun({'block_height':r[0],'signer_id':r[1],'remote_id':r[2],'mode':r[3]})
		cur.close()

	def createTables(self):
		cur = self.conn.cursor()
		cur.execute("""
SELECT 0 FROM pg_class where relname = 'common_transactions_seq_id'
""")
		result = cur.fetchone()
		if result is None:
			cur.execute(" CREATE SEQUENCE common_transactions_seq_id;")

		cur.execute("""
CREATE TABLE IF NOT EXISTS accounts
(id BIGSERIAL PRIMARY KEY,
 printablekey varchar UNIQUE,
 publickey bytea)
""")
		cur.execute("""
CREATE TABLE IF NOT EXISTS blocks
(height BIGINT PRIMARY KEY,
 hash bytea NOT NULL,
 timestamp varchar NOT NULL,
 timestamp_unix BIGINT NOT NULL,
 timestamp_nem BIGINT NOT NULL,
 signer_id bigint REFERENCES accounts(id),
 signature bytea NOT NULL,
 type int NOT NULL,
 difficulty bigint NOT NULL,
 tx_count int NOT NULL,
 fees bigint NOT NULL
)
""")
		cur.execute("""
CREATE TABLE IF NOT EXISTS transfers
(id BIGINT DEFAULT nextval('common_transactions_seq_id') PRIMARY KEY,
 block_height BIGINT REFERENCES blocks(height),
 hash bytea NOT NULL UNIQUE,
 timestamp varchar NOT NULL,
 timestamp_unix BIGINT NOT NULL,
 timestamp_nem BIGINT NOT NULL,
 signer_id BIGINT REFERENCES accounts(id),
 signature bytea,
 deadline BIGINT NOT NULL,

 recipient_id BIGINT REFERENCES accounts(id),
 amount BIGINT NOT NULL,
 fee BIGINT NOT NULL,
 message_type int,
 message_data bytea
 )
""")
		cur.execute("""
CREATE TABLE IF NOT EXISTS modifications
(id BIGINT DEFAULT nextval('common_transactions_seq_id') PRIMARY KEY,
 block_height BIGINT REFERENCES blocks(height),
 hash bytea NOT NULL UNIQUE,
 timestamp varchar NOT NULL,
 timestamp_unix BIGINT NOT NULL,
 timestamp_nem BIGINT NOT NULL,
 signer_id BIGINT REFERENCES accounts(id),
 signature bytea,
 deadline BIGINT NOT NULL,

 fee BIGINT NOT NULL,
 min_cosignatories INT)
			""")

		cur.execute("""
CREATE TABLE IF NOT EXISTS modification_entries
(id BIGSERIAL PRIMARY KEY,
 modification_id BIGINT REFERENCES modifications(id),
 type int NOT NULL,
 cosignatory_id BIGINT REFERENCES accounts(id)
)
""")

		cur.execute("""
CREATE TABLE IF NOT EXISTS delegates
(id BIGINT DEFAULT nextval('common_transactions_seq_id') PRIMARY KEY,
 block_height BIGINT REFERENCES blocks(height),
 hash bytea NOT NULL UNIQUE,
 timestamp varchar NOT NULL,
 timestamp_unix BIGINT NOT NULL,
 timestamp_nem BIGINT NOT NULL,
 signer_id BIGINT REFERENCES accounts(id),
 signature bytea,
 deadline BIGINT NOT NULL,

 remote_id BIGINT REFERENCES accounts(id),
 fee BIGINT NOT NULL,
 mode int NOT NULL
)
""")

		cur.execute("""
CREATE TABLE IF NOT EXISTS multisigs
(id BIGINT DEFAULT nextval('common_transactions_seq_id') PRIMARY KEY,
 block_height BIGINT REFERENCES blocks(height),
 hash bytea NOT NULL UNIQUE,
 timestamp varchar NOT NULL,
 timestamp_unix BIGINT NOT NULL,
 timestamp_nem BIGINT NOT NULL,
 signer_id BIGINT REFERENCES accounts(id),
 signature bytea,
 deadline BIGINT NOT NULL,

 fee BIGINT NOT NULL,
 total_fees BIGINT NOT NULL,
 signatures_count INT NOT NULL,
 inner_id BIGINT NOT NULL,
 inner_type INT NOT NULL
)
""")

		# we don't need common seq id in this one
		cur.execute("""
CREATE TABLE IF NOT EXISTS signatures
(id BIGSERIAL PRIMARY KEY,
 block_height BIGINT REFERENCES blocks(height),
 hash bytea NOT NULL,
 timestamp varchar NOT NULL,
 timestamp_unix BIGINT NOT NULL,
 timestamp_nem BIGINT NOT NULL,
 signer_id BIGINT REFERENCES accounts(id),
 signature bytea,
 deadline BIGINT NOT NULL,

 fee BIGINT NOT NULL,
 multisig_id BIGINT REFERENCES multisigs(id)
)
""")
		cur.execute("""
CREATE TABLE IF NOT EXISTS inouts_type
(id INT PRIMARY KEY,
 name VARCHAR NOT NULL
)
""")
		cur.execute("SELECT count(id) FROM inouts_type");
		ret = cur.fetchone()[0]
		if ret == 0:
			cur.execute("INSERT INTO inouts_type VALUES (%s,%s)", (1, 'incoming'))
			cur.execute("INSERT INTO inouts_type VALUES (%s,%s)", (2, 'outgoing + fees'))
			cur.execute("INSERT INTO inouts_type VALUES (%s,%s)", (3, 'harvesting'))
			cur.execute("INSERT INTO inouts_type VALUES (%s,%s)", (4, 'incoming multisig'))
			cur.execute("INSERT INTO inouts_type VALUES (%s,%s)", (5, 'outgoing multisig + fees'))

		cur.execute("""
CREATE TABLE IF NOT EXISTS inouts
(id BIGSERIAL PRIMARY KEY,
 account_id BIGINT REFERENCES accounts(id),
 block_height BIGINT REFERENCES blocks(height),
 type INT REFERENCES inouts_type(id),
 tx_id BIGSERIAL,
 amount BIGINT
)
""")
		if ret == 0:
			cur.execute("CREATE INDEX inouts_type_idx on inouts(type)");	
			cur.execute("CREATE INDEX transfers_blockheight_idx ON transfers(block_height)");
			cur.execute("CREATE INDEX modifications_blockheight_idx ON modifications(block_height)");
			cur.execute("CREATE INDEX delegates_blockheight_idx ON delegates(block_height)");
			cur.execute("CREATE INDEX multisigs_blockheight_idx ON multisigs(block_height)");
			cur.execute("CREATE INDEX signatures_blockheight_idx ON signatures(block_height)");

		cur.execute("""
CREATE TABLE IF NOT EXISTS harvests
(id BIGSERIAL PRIMARY KEY,
 account_id BIGINT REFERENCES accounts(id),
 block_height BIGINT REFERENCES blocks(height)
)
""")

		cur.close()
		self.conn.commit()

	def _addHarvested(self, cur, account_id, height):
		sql = "INSERT INTO harvests (account_id,block_height) VALUES (%s,%s)";
		obj = (account_id, height)
		cur.execute(sql, obj)

	def _addInout(self, cur, account_id, height, inout_type, tx_id, amount):
		sql = "INSERT INTO inouts (account_id,block_height,type,tx_id,amount) VALUES (%s,%s,%s,%s,%s)";
		obj = (account_id, height, inout_type, tx_id, amount)
		cur.execute(sql, obj)

	def addInout(self, account_id, height, inout_type, tx_id, amount):
		cur = self.conn.cursor()
		if amount > 0:
			self._addInout(cur, account_id, height, inout_type, tx_id, amount)
		self._addHarvested(cur, account_id, height)
		cur.close()

	def addInouts(self, block, txes):
		def inoutTransfer(cur, height, tx):
			srcId = tx['signer_id']
			dstId = tx['recipient_id']
			if srcId == dstId:
				self._addInout(cur, srcId, height, 2, tx['id'], tx['fee'])
			else:
				self._addInout(cur, srcId, height, 2, tx['id'], tx['amount']+tx['fee'])
				self._addInout(cur, dstId, height, 1, tx['id'], tx['amount'])

		def inoutFee(cur, height, tx):
			srcId = tx['signer_id']
			self._addInout(cur, srcId, height, 2, tx['id'], tx['fee'])
			
		def inoutMultisig(cur, height, tx):
			itx = tx['otherTrans']
			if itx['type'] != 257 and itx['type'] != 4097 and itx['type'] != 2049:
				raise RuntimeError("not-handled-multisig")

			if itx['type'] == 257:
				# do not reuse
				srcId = itx['signer_id']
				dstId = itx['recipient_id']
				if srcId == dstId:
					self._addInout(cur, srcId, height, 5, tx['id'], tx['total_fee'])
				else:
					self._addInout(cur, srcId, height, 5, tx['id'], itx['amount']+tx['total_fee'])
					self._addInout(cur, dstId, height, 4, tx['id'], itx['amount'])

			elif itx['type'] == 4097:
				srcId = itx['signer_id']
				self._addInout(cur, srcId, height, 5, tx['id'], tx['total_fee'])
			
			elif itx['type'] == 2049:
				srcId = itx['signer_id']
				self._addInout(cur, srcId, height, 5, tx['id'], tx['total_fee'])

			#
		cur = self.conn.cursor()
		handlers = {
			257: inoutTransfer
			, 2049: inoutFee
			, 4097: inoutFee
			, 4100: inoutMultisig
		}
		blockHeight = block['height']
		for tx in txes:
			txid = handlers[tx['type']](cur, blockHeight, tx)
		
		cur.close()
			

	def _addMultisig(self, cur, block, tx):
		handlers = {
			257: self._addTransfer
			, 2049: self._addDelegated
			, 4097: self._addAggregateModification
		}
		#inner tx
		itx = tx['otherTrans']
		if itx['type'] not in handlers:
			print "ITX TYPE", itx['type']
		innerId = handlers[itx['type']](cur, block, itx)
		totalFee = tx['total_fee']

		#print tx
		sql = "INSERT INTO multisigs (block_height,hash,timestamp,timestamp_unix,timestamp_nem, signer_id,signature,deadline, fee,total_fees,signatures_count,inner_id,inner_type) VALUES (%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s) RETURNING id";
		obj = (block['height'],
			tobin(tx['hash']),
			tx['timestamp'],
			tx['timestamp_unix'],
			tx['timestamp_nem'],
			tx['signer_id'],
			tobin(tx['signature']),
			tx['deadline'],

			tx['fee'],
			totalFee,
			len(tx['signatures']),
			innerId,
			itx['type']
		)

		#print " [+] adding to db: ", obj,
		cur.execute(sql, obj)
		retId = cur.fetchone()[0]
		sql = "INSERT INTO signatures (block_height,hash,timestamp,timestamp_unix,timestamp_nem, signer_id,signature,deadline, fee,multisig_id) VALUES (%s,%s, %s,%s,%s, %s,%s,%s, %s,%s) RETURNING id";
		for sig in tx['signatures']:
			obj = (block['height'],
				tobin(hexlify("N/A")), #tobin(sig['hash']),
				sig['timestamp'],
				sig['timestamp_unix'],
				sig['timestamp_nem'],
				sig['signer_id'],
				tobin(sig['signature']),
				sig['deadline'],

				sig['fee'],
				retId
			)
			cur.execute(sql,obj)
		#
		return retId

	def _addDelegated(self,cur,block,tx):
		sql = "INSERT INTO delegates (block_height,hash,timestamp,timestamp_unix,timestamp_nem, signer_id,signature,deadline, remote_id,fee,mode) VALUES (%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s) RETURNING id";
		obj = (block['height'],
			tobin(tx['hash']),
			tx['timestamp'],
			tx['timestamp_unix'],
			tx['timestamp_nem'],
			tx['signer_id'],
			None if 'signature' not in tx else tobin(tx['signature']),
			tx['deadline'],

			tx['remote_id'],
			tx['fee'],
			tx['mode']
		)
		#print " [+] adding to db: ", obj,
		cur.execute(sql, obj)
		retId = cur.fetchone()[0]
		#print retId
		return retId


	def _addAggregateModification(self, cur, block, tx):
		sql = "INSERT INTO modifications (block_height,hash,timestamp,timestamp_unix,timestamp_nem, signer_id, signature,deadline, fee, min_cosignatories) VALUES (%s,%s, %s,%s,%s, %s,%s,%s, %s, %s) RETURNING id";
		locdb = Db(True)
		ret = locdb.getModification('signer_id', int(tx['signer_id']))
		if 'minCosignatories' in tx:
			relative = tx['minCosignatories']['relativeChange']
			if ret is None:
				print "MODIFICATION NO old, rel is", relative, len(tx['modifications'])
				min_cosignatories = relative 
			else:
				min_cosignatories = ret['min_cosignatories'] + relative
				print "MODIFICATION has old, rel is", relative, " prev ",  ret['min_cosignatories']
		else:
			# old txes...
			min_cosignatories = 0
			print "MODIFICATION OLD", min_cosignatories

		obj = (block['height'],
			tobin(tx['hash']),
			tx['timestamp'],
			tx['timestamp_unix'],
			tx['timestamp_nem'],
			tx['signer_id'],
			None if 'signature' not in tx else tobin(tx['signature']),
			tx['deadline'],
			tx['fee'],
			min_cosignatories
		)
		print " [+] adding to db: ", obj,
		cur.execute(sql, obj)
		retId = cur.fetchone()[0]
		print retId
		sql = "INSERT INTO modification_entries (modification_id,type,cosignatory_id) VALUES(%s,%s,%s)"
		for modification in tx['modifications']:
			obj = (retId, modification['modificationType'], modification['cosignatory_id'])
			cur.execute(sql,obj)
		#
		return retId

	def _addTransfer(self, cur, block, tx):
		sql = "INSERT INTO transfers (block_height,hash,timestamp,timestamp_unix,timestamp_nem, signer_id, signature,deadline, recipient_id,amount,fee,message_type,message_data) VALUES (%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s) RETURNING id";
		obj = (block['height'],
			tobin(tx['hash']),
			tx['timestamp'],
			tx['timestamp_unix'],
			tx['timestamp_nem'],
			tx['signer_id'],
			None if 'signature' not in tx else tobin(tx['signature']),
			tx['deadline'],
			tx['recipient_id'],
			tx['amount'],
			tx['fee'],
			None if len(tx['message']) == 0 else tx['message']['type'],
			None if len(tx['message']) == 0 else tobin(tx['message']['payload'])
		)
		#print " [+] adding to db: ", obj,
		cur.execute(sql, obj)
		retId = cur.fetchone()[0]
		#print retId
		return retId

	def _addTxes(self, cur, block, txes):
		handlers = {
			257: self._addTransfer
			, 2049: self._addDelegated
			, 4097: self._addAggregateModification
			, 4100: self._addMultisig
		}
		for tx in txes:
			print tx['type']
			txid = handlers[tx['type']](cur, block, tx)
			tx['id'] = txid
		
	def processTxes(self, block, txes):
		cur = self.conn.cursor()
		self._addTxes(cur, block, txes)
		cur.close()

	def addBlock(self, block):
		cur = self.conn.cursor()
		sql = "INSERT INTO blocks (height,hash,timestamp,timestamp_unix, timestamp_nem, signer_id, signature, type, difficulty, tx_count, fees) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING height";
		obj = (block['height'],
			tobin(block['hash']),
			block['timestamp'],
			block['timestamp_unix'],
			block['timestamp_nem'],
			block['signer_id'],
			tobin(block['signature']),
			block['type'],
			block['difficulty'],
			block['tx_count'],
			block['fees'])
		#print " [+] adding to db: ", obj
		cur.execute(sql, obj)
		retId = cur.fetchone()[0]
		cur.close()
		return retId

	def findBlock(self, height):
		cur = self.conn.cursor()
		cur.execute("SELECT * FROM blocks WHERE height = %s", (height,))
		data = cur.fetchone()
		cur.close()
		return data

	def getLastHeight(self):
		cur = self.conn.cursor()
		cur.execute('SELECT height FROM blocks ORDER BY height DESC LIMIT 1')
		data = cur.fetchone()
		cur.close()
		if data is None:
			return 0
		return data[0]
		
	def getBlocks(self, height):
		cur = self.conn.cursor()
		cur.execute('SELECT a.printablekey as "s_printablekey",a.publickey as "s_publickey",b.* FROM blocks b,accounts a WHERE b.height < %s AND b.signer_id=a.id ORDER BY height DESC LIMIT 25', (height,))
		data = cur.fetchall()
		cur.close()
		return data

	def getBlocksStats(self):
		cur = self.conn.cursor()
		cur.execute('SELECT height,timestamp_nem,difficulty,fees FROM blocks ORDER BY height DESC LIMIT 5000')
		data = cur.fetchall()
		cur.close()
		return data


	def getBlock(self, height):
		cur = self.conn.cursor()
		cur.execute('SELECT a.printablekey as "s_printablekey",a.publickey as "s_publickey",b.* FROM blocks b,accounts a WHERE b.height=%s AND b.signer_id=a.id ORDER BY height DESC LIMIT 1', (height,))
		data = cur.fetchone()
		cur.close()
		return data

	def getBlockByHash(self, blockHash):
		cur = self.conn.cursor()
		cur.execute('SELECT a.printablekey as "s_printablekey",a.publickey as "s_publickey",b.* FROM blocks b,accounts a WHERE b.hash=%s AND b.signer_id=a.id ORDER BY height DESC LIMIT 1', (tobin(blockHash),))
		data = cur.fetchone()
		cur.close()
		return data

	def getInouts(self, accId, txId, limit):
		cur = self.conn.cursor()
		cur.execute("SELECT * FROM inouts WHERE account_id = %s AND type<>3 AND id < %s ORDER BY id DESC LIMIT {}".format(limit), (accId,txId))
		data = cur.fetchall()
		cur.close()
		return data

	def getInoutsNext(self, accId, txId):
		cur = self.conn.cursor()
		cur.execute('SELECT id FROM inouts WHERE account_id = %s AND type<>3 AND id > %s ORDER BY id DESC LIMIT 10', (accId, txId))
		data = cur.fetchall()
		cur.close()
		return data


	def getTransferSql(self,compare, comparator, limit):
		return 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",r.printablekey as "r_printablekey",r.publickey as "r_publickey",t.* FROM transfers t,accounts s,accounts r WHERE t.{}{}%s AND t.signer_id=s.id AND t.recipient_id=r.id ORDER BY id DESC {}'.format(compare,comparator,limit)

	def getMatchingTransfers(self, ids):
		cur = self.conn.cursor()
		cur.execute(self.getTransferSql('id', ' IN ', 'LIMIT 10'), (tuple(ids),))
		#print cur.mogrify(self.getTransferSql('id', ' IN ', 'LIMIT 10'), (tuple(ids),))
		data = cur.fetchall()
		cur.close()
		return data
	
	def getTransfer(self, compare, dataCompare):
		cur = self.conn.cursor()
		cur.execute(self.getTransferSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		data = cur.fetchone()
		cur.close()
		return data
		
	def getMessages(self, spammers, txId, limit):
		spammersFrom = spammers[0]
		spammersTo = spammers[1]
		#print spammersFrom, spammersTo
		cur = self.conn.cursor()
		sql = 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",r.printablekey as "r_printablekey",r.publickey as "r_publickey",t.* FROM transfers t,accounts s,accounts r WHERE t.id<%s AND t.message_type=1 AND t.signer_id=s.id AND t.recipient_id=r.id AND s.printablekey NOT IN %s AND r.printablekey NOT IN %s ORDER BY id DESC LIMIT {}'.format(limit)
		cur.execute(sql, (txId, tuple(spammersFrom), tuple(spammersTo)))
		#print cur.mogrify(sql, (txId, tuple(spammersFrom), tuple(spammersTo)))
		data = cur.fetchall()
		cur.close()
		return data
	
	def getMessagesNext(self, spammers, txId, limit):
		spammersFrom = spammers[0]
		spammersTo = spammers[1]
		cur = self.conn.cursor()
		sql = 'SELECT t.id FROM transfers t,accounts s,accounts r WHERE t.id > %s AND t.message_type=1 AND t.signer_id=s.id AND t.recipient_id=r.id AND s.printablekey NOT IN %s AND r.printablekey NOT IN %s ORDER BY id ASC LIMIT {}'.format(limit)
		cur.execute(sql, (txId, tuple(spammersFrom), tuple(spammersTo)))
		data = cur.fetchall()
		cur.close()
		return data


	def getTransfers(self, txId):
		cur = self.conn.cursor()
		cur.execute(self.getTransferSql('id', '<', 'LIMIT 10'), (txId,))
		data = cur.fetchall()
		cur.close()
		return data

	def getBlockTransfers(self, height):
		cur = self.conn.cursor()
		cur.execute(self.getTransferSql('block_height', '=', ''), (height,))
		data = cur.fetchall()
		cur.close()
		return data

	def getDelegateSql(self, compare, comparator, limit):
		return 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",r.printablekey as "r_printablekey",r.publickey as "r_publickey",d.* FROM delegates d,accounts s,accounts r WHERE d.{}{}%s AND d.signer_id=s.id AND d.remote_id=r.id ORDER BY id DESC {}'.format(compare, comparator, limit)
	
	def getMatchingDelegates(self, ids):
		cur = self.conn.cursor()
		cur.execute(self.getDelegateSql('id', ' IN ', 'LIMIT 10'), (tuple(ids),))
		data = cur.fetchall()
		cur.close()
		return data
	
	def getDelegate(self, compare, dataCompare):
		cur = self.conn.cursor()
		cur.execute(self.getDelegateSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		data = cur.fetchone()
		cur.close()
		return data

	def getDelegates(self, txId):
		cur = self.conn.cursor()
		cur.execute(self.getDelegateSql('id', '<', 'LIMIT 10'), (txId,))
		data = cur.fetchall()
		cur.close()
		return data

	def getBlockDelegates(self, height):
		cur = self.conn.cursor()
		cur.execute(self.getDelegateSql('block_height', '=', ''), (height,))
		data = cur.fetchall()
		cur.close()
		return data

	def _getModificationEntriesCounts(self, cur, txIds):
		cur.execute('SELECT modification_id,count(*) from modification_entries WHERE modification_id in %s GROUP BY modification_id', (tuple(txIds),))
		data = cur.fetchall()
		return data

	def _addCounts(self, cur, data):
		if len(data) == 0:
			return
		# placing this here is not very nice, but will be more convenient
		counts = self._getModificationEntriesCounts(cur, map(lambda x: x['id'], data))
		temp = {}
		for elem in counts:
			temp[elem['modification_id']] = elem['count']
		for tx in data:
			tx['modifications_count'] = temp[tx['id']]

	def getModificationSql(self, compare, comparator, limit):
		return 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",m.* FROM modifications m,accounts s WHERE m.{}{}%s AND m.signer_id=s.id ORDER BY id DESC {}'.format(compare,comparator, limit)

	def getMatchingModifications(self, ids):
		cur = self.conn.cursor()
		cur.execute(self.getModificationSql('id', ' IN ', 'LIMIT 10'), (tuple(ids),))
		data = cur.fetchall()
		cur.close()
		return data
	
	def getModification(self, compare, dataCompare):
		cur = self.conn.cursor()
		cur.execute(self.getModificationSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		data = cur.fetchone()
		if data != None:
			cur.execute('SELECT a.printablekey as "printablekey",a.publickey as "publickey",e.* FROM modification_entries e,accounts a WHERE e.modification_id = %s AND e.cosignatory_id=a.id', (data['id'],))
			data['modifications'] = cur.fetchall()
			data['modifications_count'] = len(data['modifications'])
		cur.close()
		return data


	def getModifications(self, txId):
		cur = self.conn.cursor()
		cur.execute(self.getModificationSql('id', '<', 'LIMIT 10'), (txId,))
		data = cur.fetchall()
		self._addCounts(cur, data)
		cur.close()
		return data

	def getBlockModifications(self, height):
		cur = self.conn.cursor()
		cur.execute(self.getModificationSql('block_height', '=', ''), (height,))
		data = cur.fetchall()
		self._addCounts(cur, data)
		cur.close()
		return data

	def getMultisigSql(self, compare, comparator, limit):
		return 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",m.* FROM multisigs m,accounts s WHERE m.{}{}%s AND m.signer_id=s.id ORDER BY id DESC {}'.format(compare,comparator,limit)

	def getMatchingMultisigs(self, ids):
		cur = self.conn.cursor()
		cur.execute(self.getMultisigSql('id', ' IN ', 'LIMIT 10'), (tuple(ids),))
		data = cur.fetchall()
		cur.close()
		return data
	
	def getMultisig(self, compare, dataCompare):
		cur = self.conn.cursor()
		cur.execute(self.getMultisigSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		data = cur.fetchone()
		if data != None:
			switch = {
				257: self.getTransfer
				, 2049: self.getDelegate
				, 4097: self.getModification
			}
			data['inner'] = switch[data['inner_type']]('id', data['inner_id'])

			cur.execute('SELECT a.printablekey as "s_printablekey",a.publickey as "s_publickey",s.* FROM signatures s,accounts a WHERE s.multisig_id = %s AND s.signer_id=a.id', (data['id'],))
			data['signatures'] = cur.fetchall()
			data['signatures_count'] = len(data['signatures'])
		cur.close()
		return data

	def getMultisigs(self, txId):
		cur = self.conn.cursor()
		cur.execute(self.getMultisigSql('id', '<', 'LIMIT 10'), (txId,))
		data = cur.fetchall()
		cur.close()
		return data

	def getBlockMultisigs(self, height):
		cur = self.conn.cursor()
		cur.execute(self.getMultisigSql('block_height', '=', ''), (height,))
		data = cur.fetchall()
		cur.close()
		return data

	def getTransactionByHash(self, name, txHash):
		switch = {
			'transfers': self.getTransfer
			, 'delegates': self.getDelegate
			, 'modifications': self.getModification
			, 'multisigs': self.getMultisig
		}
		return switch[name]('hash', tobin(txHash))
		
	def getTransactionsByType(self, name, txId):
		switch = {
			'transfers': self.getTransfers
			, 'delegates': self.getDelegates
			, 'modifications': self.getModifications
			, 'multisigs': self.getMultisigs
		}
		return switch[name](txId)

	def getTableNext(self, table, txId):
		cur = self.conn.cursor()
		cur.execute('SELECT id FROM '+table+' WHERE id > %s ORDER BY id ASC LIMIT 10', (txId,))
		data = cur.fetchall()
		cur.close()
		return data

	def getAccount(self, address):
		cur = self.conn.cursor()
		cur.execute('SELECT a.* FROM accounts a WHERE a.printablekey = %s', (address,))
		data = cur.fetchone()
		cur.close()
		return data

	def getAccountById(self, accid):
		cur = self.conn.cursor()
		cur.execute('SELECT a.* FROM accounts a WHERE a.id = %s', (accid,))
		data = cur.fetchone()
		cur.close()
		return data

	def getAccountsByIds(self, ids):
		cur = self.conn.cursor()
		cur.execute('SELECT a.* FROM accounts a WHERE a.id in %s', (tuple(ids),))
		data = cur.fetchall()
		cur.close()
		return data

	def getHarvestedBlocksCount(self, accid):
		cur = self.conn.cursor()
		cur.execute('SELECT COUNT(*) AS "harvested_count" FROM harvests WHERE account_id = %s', (accid,))
		data = cur.fetchone()['harvested_count']
		cur.close()
		return data

	def getHarvestedBlocksReward(self, accid):
		cur = self.conn.cursor()
		cur.execute('SELECT type,cast(SUM(amount) as bigint) AS "sum" FROM inouts WHERE account_id=%s GROUP BY type', (accid,))
		data = cur.fetchall()
		cur.close()
		return data

	def getHarvestersByCount(self):
		cur = self.conn.cursor()
		cur.execute("""SELECT COUNT(h.block_height) AS harvestedblocks,a.id from harvests h,accounts a where a.id=h.account_id GROUP BY a.id ORDER BY harvestedblocks DESC LIMIT 50""")
		data = cur.fetchall()
		cur.close()
		return data

	def getHarvestersFees(self, ids):
		cur = self.conn.cursor()
		cur.execute("""SELECT i.account_id, cast(CEIL(SUM(i.amount)/1000000) AS BIGINT) as fees FROM inouts i WHERE i.account_id IN %s AND i.type=3 GROUP BY i.account_id""", (tuple(ids),));
		data = cur.fetchall()
		cur.close()
		return data
		

#	def getHarvesters(self):
#		cur = self.conn.cursor()
#		cur.execute("""
#SELECT COUNT(h.block_height) AS harvestedblocks,a.#ablekey,cast(CEIL(SUM(i.amount)/1000000) AS BIGINT) as fees FROM
#  harvests h
#INNER JOIN accounts a ON h.account_id=a.id
#LEFT JOIN inouts i ON i.type=3 AND h.block_height=i.block_height
#GROUP BY a.#ablekey ORDER BY harvestedblocks DESC LIMIT 50
#""");
#		data = cur.fetchall()
#		cur.close()
#		return data
	
	def addAccount(self, pub, printable):
		cur = self.conn.cursor()
		sql = "INSERT INTO accounts (printablekey, publickey) VALUES (%s,%s) RETURNING id";
		obj = (printable, bytearray(pub.decode('hex')))
		#print " [+] adding to db: ", pub, obj
		cur.execute(sql, obj)
		retId = cur.fetchone()[0]
		cur.close()
		return retId

	def updateAccount(self, retId, pub, printable):
		cur = self.conn.cursor()
		sql = "UPDATE accounts SET publickey=%s WHERE id=%s";
		obj = (bytearray(pub.decode('hex')), retId)
		print " [+] updating in db: ", pub, obj
		cur.execute(sql, obj)
		cur.close()
		return retId
	
	def addAccountPrint(self, printable):
		cur = self.conn.cursor()
		sql = "INSERT INTO accounts (printablekey) VALUES (%s) RETURNING id";
		# coma at end due to psycopg
		obj = (printable,)
		#print " [+] adding to db: ", obj
		cur.execute(sql, obj)
		retId = cur.fetchone()[0]
		cur.close()
		return retId

