import psycopg2
from datetime import datetime
from binascii import hexlify
from psycopg2.extras import RealDictConnection
from config import config
from collections import *

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

	def createNamespaceTables(self, cur):
		cur.execute("""
			CREATE TABLE IF NOT EXISTS namespaces
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
			 rental_sink BIGINT REFERENCES accounts(id),
			 rental_fee BIGINT NOT NULL,
			 parent_ns BIGINT,
			 namespace_name VARCHAR(148),
			 namespace_part VARCHAR(66)
			)
			""") 
		cur.execute("""
			CREATE TABLE IF NOT EXISTS mosaics
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
			 creation_sink BIGINT REFERENCES accounts(id),
			 creation_fee BIGINT NOT NULL,
			 parent_ns BIGINT REFERENCES namespaces(id),
			 mosaic_name VARCHAR(34),
			 mosaic_fqdn VARCHAR(180),
			 mosaic_description VARCHAR(516)
			)
			""")

		cur.execute("""
			CREATE TABLE IF NOT EXISTS mosaic_levys
			(id BIGSERIAL PRIMARY KEY,
			 block_height BIGINT REFERENCES blocks(height),
			 mosaic_id BIGINT REFERENCES mosaics(id),

			 type INT NOT NULL,
			 recipient_id BIGINT REFERENCES accounts(id),
			 fee_mosaic_id BIGINT REFERENCES mosaics(id),
			 fee BIGINT NOT NULL
			)""")
		cur.execute("""
			CREATE TABLE IF NOT EXISTS mosaic_properties
			(id BIGSERIAL PRIMARY KEY,
			 block_height BIGINT REFERENCES blocks(height),
			 mosaic_id BIGINT REFERENCES mosaics(id),

			 name VARCHAR(64),
			 value VARCHAR(64)
			)""")
		cur.execute("""
			CREATE TABLE IF NOT EXISTS transfer_attachments
			(id BIGSERIAL PRIMARY KEY,
			 block_height BIGINT REFERENCES blocks(height),
			 transfer_id BIGINT REFERENCES transfers(id),

			 type INT REFERENCES inouts_type(id),
			 mosaic_id BIGINT REFERENCES mosaics(id),
			 quantity BIGINT NOT NULL
			)""")
		cur.execute("""
			CREATE TABLE IF NOT EXISTS mosaic_inouts
			(id BIGSERIAL PRIMARY KEY,
			 account_id BIGINT REFERENCES accounts(id),
			 block_height BIGINT REFERENCES blocks(height),
			 mosaic_id BIGINT REFERENCES mosaics(id),

			 type INT REFERENCES inouts_type(id),
			 tx_id BIGSERIAL,
			 quantity BIGINT
			)
			""")
		
		cur.execute("""
			CREATE TABLE IF NOT EXISTS mosaic_amounts
			(id BIGSERIAL PRIMARY KEY,
			 account_id BIGINT REFERENCES accounts(id),
			 block_height BIGINT REFERENCES blocks(height),
			 mosaic_id BIGINT REFERENCES mosaics(id),
			 amount BIGINT
			)
			""")

		cur.execute("""
			CREATE TABLE IF NOT EXISTS mosaic_state_supply
			(id BIGSERIAL PRIMARY KEY,
			 block_height BIGINT REFERENCES blocks(height),
			 mosaic_id BIGINT REFERENCES mosaics(id),

			 quantity BIGINT
			)""")

		# block heights indexes
		cur.execute("CREATE INDEX namespaces_blockheight_idx ON namespaces(block_height)");
		cur.execute("CREATE INDEX mosaics_blockheight_idx ON mosaics(block_height)");
		cur.execute('CREATE INDEX mosaic_levys_blockheight_idx ON mosaic_levys(block_height)')
		cur.execute('CREATE INDEX mosaic_properties_blockheight_idx ON mosaic_properties(block_height)')
		cur.execute('CREATE INDEX mosaic_inouts_blockheight_idx ON mosaic_inouts(block_height)')
		cur.execute('CREATE INDEX mosaic_amounts_blockheight_idx ON mosaic_amounts(block_height)')
		cur.execute('CREATE INDEX mosaic_state_supply_blockheight_idx ON mosaic_state_supply(block_height)')

		cur.execute("CREATE INDEX mosaic_levys_mosaic_idx ON mosaic_levys(mosaic_id)");
		cur.execute("CREATE INDEX mosaic_properties_mosaic_idx ON mosaic_properties(mosaic_id)");
		cur.execute("CREATE INDEX transfer_attachments_blockheight_idx ON transfer_attachments(block_height)");
		cur.execute('CREATE INDEX inouts_blockheight_idx ON inouts(block_height)')
		cur.execute('CREATE INDEX inouts_account_id_idx ON inouts(account_id)')
		cur.execute('CREATE INDEX inouts_account_id_3_idx ON inouts(account_id) WHERE type = 3')
		cur.execute('CREATE INDEX harvests_blockheight_idx ON harvests(block_height)')
		cur.execute('CREATE INDEX harvests_account_id_idx ON harvests(account_id)')


		cur.execute("CREATE INDEX transfer_attachments_mosaic_idx ON transfer_attachments(mosaic_id)");
		cur.execute("CREATE INDEX transfer_attachments_transfer_idx ON transfer_attachments(transfer_id)");

		cur.execute("CREATE INDEX mosaic_inouts_type_idx on mosaic_inouts(type)");

		cur.execute("select * from accounts where printablekey = '%s'" % config.nemesis);
		nId = cur.fetchone()[0]
		NA = bytearray('n/a')
		cur.execute("INSERT INTO namespaces (block_height, hash, timestamp, timestamp_unix, timestamp_nem, signer_id, signature, deadline, fee, rental_sink, rental_fee, parent_ns, namespace_name, namespace_part) VALUES (%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s) RETURNING id", (1,NA, '2015-03-29 00:06:25',1427587585,0,nId,NA,0, 0,nId,0,None,"nem","nem"))
		nsId = cur.fetchone()[0]
		print "NEM namespace ID: ", nsId

		cur.execute("INSERT INTO mosaics (block_height, hash, timestamp, timestamp_unix, timestamp_nem, signer_id, signature, deadline, fee, creation_sink, creation_fee, parent_ns, mosaic_name, mosaic_fqdn, mosaic_description) VALUES (%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s) RETURNING id", (1,NA, '2015-03-29 00:06:25',1427587585,0,nId,NA,0,  0,nId,0, nsId,"nem.xem","nem.xem", "Mosaic representing XEM"))
		msId = cur.fetchone()[0]
		print "NEM.XEM mosaic ID: ", msId

		cur.execute("INSERT INTO mosaic_state_supply (block_height, mosaic_id, quantity) VALUES (1, %s, 8999999999000000)", (msId,))
			
		cur.executemany("INSERT INTO mosaic_properties (block_height,mosaic_id,name,value) VALUES (%s,%s, %s,%s)",
			((1,msId, 'divisibility', "6"),
			(1,msId, 'initialSupply', "8999999999"),
			(1,msId, 'mutableSupply', "0"),
			(1,msId, 'transferable', "0")))

		cur.execute("INSERT INTO inouts_type VALUES (%s,%s)", (11, 'levy incoming'))
		cur.execute("INSERT INTO inouts_type VALUES (%s,%s)", (12, 'levy outgoing'))
		cur.execute("INSERT INTO inouts_type VALUES (%s,%s)", (14, 'levy incoming multisig'))
		cur.execute("INSERT INTO inouts_type VALUES (%s,%s)", (15, 'levy outgoing multisig'))


	def createSupplies(self, cur):
		cur.execute("""
			CREATE TABLE IF NOT EXISTS mosaic_supplies
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

			 mosaic_id BIGINT REFERENCES mosaics(id),
			 supply_type INT NOT NULL,
			 delta BIGINT NOT NULL
			)""")

		cur.execute("CREATE INDEX mosaic_supplies_blockheight_idx ON mosaic_supplies(block_height)");

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
		
		cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('namespaces',))
		hasNamespaces = cur.fetchone()[0]

		if not hasNamespaces:
			self.createNamespaceTables(cur)

		cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('mosaic_supplies',))
		hasSupplies = cur.fetchone()[0]
		if not hasSupplies:
			self.createSupplies(cur)

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

	def _clearMosInouts(self, cur, mosDbId):
		sql = "DELETE FROM mosaic_inouts where mosaic_id=%s"
		cur.execute(sql, (mosDbId,))

		sql = "DELETE FROM mosaic_amounts WHERE mosaic_id=%s"
		cur.execute(sql, (mosDbId,))

	def _getPreviousAmount(self, cur, mosDbId, account_id):
		sql = "SELECT amount,block_height,id FROM mosaic_amounts WHERE mosaic_id=%s AND account_id=%s ORDER BY block_height DESC LIMIT 1"
		obj = (mosDbId, account_id)
		cur.execute(sql, obj)
		return cur.fetchone()

	def _addMosInout(self, cur, mosDbId, account_id, height, inout_type, tx_id, amount):
		sql = "INSERT INTO mosaic_inouts (account_id,block_height,mosaic_id, type,tx_id,quantity) VALUES (%s,%s,%s, %s,%s,%s)";
		obj = (account_id, height, mosDbId, inout_type, tx_id, amount)
		cur.execute(sql, obj)

		ret = self._getPreviousAmount(cur, mosDbId, account_id)
		val = ret[0] if ret else 0
		if inout_type in [1, 1+3, 11, 11+3]:
			val += amount
		elif inout_type in [2, 2+3, 12, 12+3]:
			val -= amount
		else:
			print inout_type
			raise 1

		# nembex is not running postgres 9.5, so we can't take advantage of UPSERT
		# but since update is running from a single process we're fine
		if ret and ret[1] == height:
			#print "need to update instead of insert",ret[2]
			sql = "UPDATE  mosaic_amounts SET amount=%s WHERE id=%s"
			obj = (val, ret[2])
		else:
			sql = "INSERT INTO mosaic_amounts (account_id,block_height,mosaic_id, amount) VALUES (%s,%s,%s, %s)"
			obj = (account_id, height, mosDbId, val)
		cur.execute(sql, obj)

	def addInout(self, account_id, height, inout_type, tx_id, amount):
		cur = self.conn.cursor()
		if amount > 0:
			self._addInout(cur, account_id, height, inout_type, tx_id, amount)
		self._addHarvested(cur, account_id, height)
		cur.close()

	@staticmethod
	def _getMosaicFqdn(mosaic):
		mId = mosaic['mosaicId']
		return mId['namespaceId'] + '.' + mId['name']

	def addInouts(self, block, txes):
		def inoutTransfer(cur, height, tx, txId, fee, multi):
			v = tx['version'] & 0xffffff
			srcId = tx['signer_id']
			dstId = tx['recipient_id']

			if v == 1 or ('mosaics' not in tx):
				if srcId == dstId:
					self._addInout(cur, srcId, height, 2+multi, txId, fee)
				else:
					self._addInout(cur, srcId, height, 2+multi, txId, tx['amount']+fee)
					self._addInout(cur, dstId, height, 1+multi, txId, tx['amount'])
			else:
				# amount doesn't mean anything, we need to process attachments
				# to check if there is nem.xem, multiply it
				print tx
				amount = tx['amount'] / 1000000
				qs = defaultdict(long)
				for mosaic in tx['mosaics']:
					mosName = Db._getMosaicFqdn(mosaic)
					qs[mosName] += mosaic['quantity']
				
				locdb = Db(True)
				loccur = locdb.conn.cursor()
				for mosFqdn,_v in qs.iteritems():
					v = _v*amount
					if mosFqdn == 'nem.xem':
						if srcId == dstId:
							self._addInout(cur, srcId, height, 2+multi, txId, fee)
						else:
							self._addInout(cur, srcId, height, 2+multi, txId, v+fee)
							self._addInout(cur, dstId, height, 1+multi, txId, v)
					else:
						mosaic = locdb._getMosaic(loccur, 'mosaic_fqdn', mosFqdn)
						mosId = mosaic['id']
						self._addMosInout(cur, mosId, srcId, height, 2+multi, txId, v)
						self._addMosInout(cur, mosId, dstId, height, 1+multi, txId, v)
						if mosaic['levy']:
							mosId = mosaic['levy']['fee_mosaic']['id']
							dstId = mosaic['levy']['recipient_id']
							levyFee = self._calculateLevy(mosaic['levy']['type'], amount, _v, mosaic['levy']['fee'])
							#print "LEVY:"
							#del mosaic['levy']['fee_mosaic']
							#print mosaic['levy']
							self._addMosInout(cur, mosId, srcId, height, 12+multi, txId, levyFee)
							self._addMosInout(cur, mosId, dstId, height, 11+multi, txId, levyFee)
				loccur.close()

		def inoutFee(cur, height, tx, txId, fee, multi):
			srcId = tx['signer_id']
			self._addInout(cur, srcId, height, 2+multi, txId, fee)

		def inoutSink(cur, height, tx, txId, fee, multi):
			srcId = tx['signer_id']
			dstId = tx['rentalFeeSink_id']

			self._addInout(cur, srcId, height, 2+multi, txId, tx['rentalFee']+fee)
			self._addInout(cur, dstId, height, 1+multi, txId, tx['rentalFee'])

		def getPropsMap(tx):
			_props = {}
			for prop in tx['properties']:
				_props[ prop['name'] ] = prop['value']
			_props['divisibility'] = int(_props['divisibility'], 10)
			_props['initialSupply'] = int(_props['initialSupply'], 10)
			return _props

		def supplyToValue(props, supply):
			mul = 10 ** props['divisibility']
			return supply * mul

		def inoutSinkMosaic(cur, height, tx, txId, fee, multi):
			srcId = tx['signer_id']
			dstId = tx['creationFeeSink_id']

			self._addInout(cur, srcId, height, 2+multi, txId, tx['creationFee']+fee)
			self._addInout(cur, dstId, height, 1+multi, txId, tx['creationFee'])

			#print "----"
			#print tx
			#print "----"
			self._clearMosInouts(cur, tx['id'])
			_props = getPropsMap(tx['mosaicDefinition'])
			v = supplyToValue(_props, _props['initialSupply'])
			self._addMosInout(cur, tx['id'], srcId, height, 1+multi, txId, v)

		def inoutSupply(cur, height, tx, txId, fee, multi):
			srcId = tx['signer_id']

			inoutFee(cur, height, tx, txId, fee, multi)

			locdb = Db(True)
			loccur = locdb.conn.cursor()
			mosFqdn = Db._getMosaicFqdn(tx)
			mosaic = locdb._getMosaic(loccur, 'mosaic_fqdn', mosFqdn)
			loccur.close()
			
			#print mosaic
			_props = getPropsMap(mosaic)
			v = supplyToValue(_props, tx['delta'])
			#print v

			if tx['supplyType'] == 1:
				self._addMosInout(cur, mosaic['id'], srcId, height, 1+multi, txId, v)
			else:
				self._addMosInout(cur, mosaic['id'], srcId, height, 2+multi, txId, v)

		cur = self.conn.cursor()
		handlers = {
			257: inoutTransfer # transfer
			, 2049: inoutFee   # importance
			, 4097: inoutFee   # multisig
			, 8193: inoutSink  # namespace
			, 16385: inoutSinkMosaic # mosic creation
			, 16386: inoutSupply     # mosaicSupply
		}
		blockHeight = block['height']
		for tx in txes:
			#print tx
			txId = tx['id']
			txType = tx['type']
			fee = tx['fee']
			multi = 0
			# handle multisig
			if txType == 4100:
				fee = tx['total_fee']
				txType = tx['otherTrans']['type']
				tx = tx['otherTrans']
				multi = 3
			txid = handlers[txType](cur, blockHeight, tx, txId, fee, multi)
		
		cur.close()
			

	def _addMultisig(self, cur, block, tx):
		handlers = {
			    257: self._addTransfer
			,  2049: self._addDelegated
			,  4097: self._addAggregateModification
			,  8193: self._addNamespace
			, 16385: self._addMosaic
			, 16386: self._addMosaicSupply
		}
		#inner tx
		itx = tx['otherTrans']
		if itx['type'] not in handlers:
			print "ITX TYPE", itx['type']
		innerId = handlers[itx['type']](cur, block, itx)
		itx['id'] = innerId
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
		if ('minCosignatories' in tx) and ('relativeChange' in tx['minCosignatories']):
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
		#print " [+] adding to db: ", obj,
		cur.execute(sql, obj)
		retId = cur.fetchone()[0]
		#print retId
		sql = "INSERT INTO modification_entries (modification_id,type,cosignatory_id) VALUES(%s,%s,%s)"
		for modification in tx['modifications']:
			obj = (retId, modification['modificationType'], modification['cosignatory_id'])
			cur.execute(sql,obj)
		#
		return retId

	def _addNamespace(self, cur, block, tx):
		parent = None
		if tx['parent']:
			locdb = Db(True)
			ret = locdb.getNamespace('namespace_name', tx['parent'])
			parent = ret

		fqdn = (parent['namespace_name'] + '.' if parent else '') + tx['newPart']

		sql = "INSERT INTO namespaces (block_height,hash, timestamp,timestamp_unix,timestamp_nem, signer_id,signature,deadline,fee, rental_sink, rental_fee, parent_ns, namespace_name, namespace_part) VALUES (%s,%s, %s,%s,%s, %s,%s,%s,%s, %s, %s, %s, %s, %s) RETURNING id"
		obj = (block['height'],
			tobin(tx['hash']),
			tx['timestamp'],
			tx['timestamp_unix'],
			tx['timestamp_nem'],
			tx['signer_id'],
			None if 'signature' not in tx else tobin(tx['signature']),
			tx['deadline'],
			tx['fee'],

			tx['rentalFeeSink_id'],
			tx['rentalFee'],
			parent['id'] if parent else None,
			fqdn,
			tx['newPart']
		)
		#print " [+] adding to db: ", obj,
		cur.execute(sql,obj)
		retId = cur.fetchone()[0]
		#print retId
		return retId
	
	def _addMosaic(self, cur, block, tx):
		locdb = Db(True)
		ret = locdb.getNamespace('namespace_name', tx['mosaicDefinition']['id']['namespaceId'])
		parent = ret

		sql = "INSERT INTO mosaics (block_height,hash, timestamp,timestamp_unix,timestamp_nem, signer_id,signature,deadline,fee, creation_sink, creation_fee, parent_ns, mosaic_name, mosaic_fqdn, mosaic_description) VALUES (%s,%s, %s,%s,%s, %s,%s,%s,%s, %s, %s, %s, %s, %s, %s) RETURNING id"
		mosaicFqdn = parent['namespace_name'] + '.' + tx['mosaicDefinition']['id']['name']
		obj = (block['height'],
			tobin(tx['hash']),
			tx['timestamp'],
			tx['timestamp_unix'],
			tx['timestamp_nem'],
			tx['signer_id'],
			None if 'signature' not in tx else tobin(tx['signature']),
			tx['deadline'],
			tx['fee'],

			tx['creationFeeSink_id'],
			tx['creationFee'],
			parent['id'],
			tx['mosaicDefinition']['id']['name'],
			mosaicFqdn,
			tx['mosaicDefinition']['description']
		)
		#print " [+] adding to db: ", obj,
		cur.execute(sql,obj)
		retId = cur.fetchone()[0]
		#print retId
		
		sql = "INSERT INTO mosaic_properties (block_height,mosaic_id,name,value) VALUES(%s,%s,%s,%s)"
		quantity = 0
		for prop in tx['mosaicDefinition']['properties']:
			obj = (block['height'],retId, prop['name'], prop['value'])
			cur.execute(sql,obj)
			
			if prop['name'] == 'initialSupply':
				quantity = int(prop['value'], 10)

		sql = "INSERT INTO mosaic_state_supply (block_height, mosaic_id, quantity) VALUES (%s, %s, %s)"
		obj = (block['height'], retId, quantity)
		cur.execute(sql, obj)

		mosLevy = tx['mosaicDefinition']['levy']
		if 'recipient' in mosLevy:
			levyMosFqdn = Db._getMosaicFqdn(mosLevy)
			if levyMosFqdn == mosaicFqdn:
				levyMosaic = {'id': retId }
			else:
				loccur = locdb.conn.cursor()
				levyMosaic = locdb._getMosaic(loccur, 'mosaic_fqdn', levyMosFqdn)
				loccur.close()
			sql = "INSERT INTO mosaic_levys (block_height,mosaic_id, type,recipient_id,fee_mosaic_id,fee) VALUES(%s,%s, %s,%s,%s,%s)"
			obj = (block['height'],retId, mosLevy['type'],mosLevy['recipient_id'],levyMosaic['id'],mosLevy['fee'])
			cur.execute(sql,obj)

		return retId

	def _getPreviousStateSupply(self, cur, mosDbId):
		sql = "SELECT quantity,block_height,id FROM mosaic_state_supply WHERE mosaic_id=%s ORDER BY block_height DESC LIMIT 1"
		obj = (mosDbId,)
		cur.execute(sql, obj)
		return cur.fetchone()

	def _addMosaicSupply(self, cur, block, tx):
		locdb = Db(True)
		mosFqdn = Db._getMosaicFqdn(tx)
		loccur = locdb.conn.cursor()
		mosaic = locdb._getMosaic(loccur, 'mosaic_fqdn', mosFqdn)
		loccur.close()

		sql = "INSERT INTO mosaic_supplies (block_height,hash, timestamp,timestamp_unix,timestamp_nem, signer_id,signature,deadline,fee, mosaic_id, supply_type, delta) VALUES (%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s) RETURNING id"
		obj = (block['height'],
			tobin(tx['hash']),
			tx['timestamp'],
			tx['timestamp_unix'],
			tx['timestamp_nem'],
			tx['signer_id'],
			None if 'signature' not in tx else tobin(tx['signature']),
			tx['deadline'],
			tx['fee'],

			mosaic['id'],
			tx['supplyType'],
			tx['delta']
		)
		#print " [+] adding to db: ", obj,
		cur.execute(sql,obj)
		retId = cur.fetchone()[0]

		ret = self._getPreviousStateSupply(cur, mosaic['id'])
		val = ret[0] if ret else 0
		print "INSERTING ", val, " -> ",
		if tx['supplyType'] == 1:
			val += tx['delta']
		elif tx['supplyType'] == 2:
			val -= tx['delta']
		else:
			print "FAILED ON A TX:"
			print tx
			raise 1

		print val

		if ret and ret[1] == block['height']:
			sql = "UPDATE mosaic_state_supply SET quantity=%s WHERE id=%s"
			obj = (val, ret[2])
		else:
			sql = "INSERT INTO mosaic_state_supply (block_height, mosaic_id, quantity) VALUES (%s, %s, %s)"
			obj = (block['height'], mosaic['id'], val)
		cur.execute(sql, obj)

		#print retId
		return retId

	def _calculateLevy(self, levyType, multiplier, quantity, levyFee):
		if levyType == 1:
			return levyFee
		elif levyType == 2:
			return multiplier * quantity * levyFee / 10000
		

	def _addTransfer(self, cur, block, tx):
		v = tx['version'] & 0xffffff
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

		if v == 2:
			locdb = Db(True)
			loccur = locdb.conn.cursor()
			for a in tx['mosaics']:
				mosFqdn = Db._getMosaicFqdn(a)
				mosaic = locdb._getMosaic(loccur, 'mosaic_fqdn', mosFqdn)
				sql = "INSERT INTO transfer_attachments (block_height,transfer_id, type,mosaic_id,quantity) VALUES(%s,%s, %s,%s,%s)"

				assert (a['quantity'] * tx['amount']) % 1000000 == 0, "invalid amount in a tx"
				obj = (block['height'],retId, 2, mosaic['id'], tx['amount']*a['quantity'] / 1000000 )
				cur.execute(sql,obj)

				if mosaic['levy']:
					levyFee = self._calculateLevy(mosaic['levy']['type'], tx['amount'], a['quantity'], mosaic['levy']['fee'])
					obj = (block['height'],retId, 12, mosaic['levy']['fee_mosaic']['id'], levyFee)
					cur.execute(sql,obj)
				
			loccur.close()
		return retId

	def _addTxes(self, cur, block, txes):
		handlers = {
			    257: self._addTransfer
			,  2049: self._addDelegated
			,  4097: self._addAggregateModification
			,  4100: self._addMultisig
			,  8193: self._addNamespace
			, 16385: self._addMosaic
			, 16386: self._addMosaicSupply
		}
		for tx in txes:
			print ('processing tx', tx['type'])
			txid = handlers[tx['type']](cur, block, tx)
			tx['id'] = txid
			# ugly hack for multiple txes in same block :/
			if tx['type'] == 8193 or tx['type'] == 16386:
				cur.close()
				self.commit()
				cur = self.conn.cursor()

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

	def getInoutsNext(self, accId, txId,limit):
		cur = self.conn.cursor()
		cur.execute('SELECT id FROM inouts WHERE account_id = %s AND type<>3 AND id > %s ORDER BY id DESC LIMIT '+str(limit), (accId, txId))
		data = cur.fetchall()
		cur.close()
		return data

	def getAccountMosaics(self, accId, limit):
		cur = self.conn.cursor()
		cur.execute('SELECT * FROM (SELECT *,row_number() OVER (PARTITION BY mosaic_id ORDER BY block_height DESC) AS rn FROM mosaic_amounts WHERE account_id=%s) q WHERE amount != 0 AND rn = 1 ORDER BY block_height DESC LIMIT '+str(limit), (accId,))
		data = cur.fetchall()
		print data
		cur.close()
		return data

	def getTransferSql(self,compare, comparator, limit):
		return 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",r.printablekey as "r_printablekey",r.publickey as "r_publickey",t.* FROM transfers t,accounts s,accounts r WHERE t.{}{}%s AND t.signer_id=s.id AND t.recipient_id=r.id ORDER BY id DESC {}'.format(compare,comparator,limit)

	def getMatchingTransfers(self, ids, limit):
		cur = self.conn.cursor()
		cur.execute(self.getTransferSql('id', ' IN ', 'LIMIT '+str(limit)), (tuple(list(ids)),))
		#print cur.mogrify(self.getTransferSql('id', ' IN ', 'LIMIT 10'), (tuple(ids),))
		data = cur.fetchall()
		cur.execute('SELECT * FROM transfer_attachments t WHERE t.transfer_id IN %s ORDER BY id,type DESC', (tuple(list(ids)),))
		attachments = cur.fetchall()
		m = defaultdict(list)
		for e in attachments:
			m[e['transfer_id']].append(e)
		for e in data:
			if e['id'] in m:
				e['attachments'] = m[e['id']]
		cur.close()
		return data
	
	def getTransfer(self, compare, dataCompare):
		cur = self.conn.cursor()
		cur.execute(self.getTransferSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		data = cur.fetchone()
		if data:
			cur.execute('SELECT * FROM transfer_attachments t WHERE t.transfer_id = %s ORDER BY id,type DESC', (data['id'],))
			att = cur.fetchall()

			ids = set(map(lambda e: e['mosaic_id'], att))
			m = {}
			if len(ids) > 0:
				_mosaicDefinitions = self.getMatchingMosaics(ids, 10)
				for e in _mosaicDefinitions:
					m[e['id']] = e
			data['mosaics'] = m
			data['attachments'] = att

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


	def getTransfers(self, txId, limit):
		cur = self.conn.cursor()
		cur.execute(self.getTransferSql('id', '<', 'LIMIT '+str(limit)), (txId,))
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
	
	def getMatchingDelegates(self, ids, limit):
		cur = self.conn.cursor()
		cur.execute(self.getDelegateSql('id', ' IN ', 'LIMIT '+str(limit)), (tuple(list(ids)),))
		data = cur.fetchall()
		cur.close()
		return data
	
	def getDelegate(self, compare, dataCompare):
		cur = self.conn.cursor()
		cur.execute(self.getDelegateSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		data = cur.fetchone()
		cur.close()
		return data

	def getDelegates(self, txId, limit):
		cur = self.conn.cursor()
		cur.execute(self.getDelegateSql('id', '<', 'LIMIT '+str(limit)), (txId,))
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
		cur.execute('SELECT modification_id,count(*) from modification_entries WHERE modification_id in %s GROUP BY modification_id', (tuple(list(txIds)),))
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

	def getMatchingModifications(self, ids, limit):
		cur = self.conn.cursor()
		cur.execute(self.getModificationSql('id', ' IN ', 'LIMIT '+str(limit)), (tuple(list(ids)),))
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


	def getModifications(self, txId, limit):
		cur = self.conn.cursor()
		cur.execute(self.getModificationSql('id', '<', 'LIMIT '+str(limit)), (txId,))
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

	def getMatchingMultisigs(self, ids, limit):
		cur = self.conn.cursor()
		cur.execute(self.getMultisigSql('id', ' IN ', 'LIMIT '+str(limit)), (tuple(list(ids)),))
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
				, 8193: self.getNamespace
				, 16385: self.getMosaic
				#, 16386: self.getMosaicSupply
			}
			data['inner'] = switch[data['inner_type']]('id', data['inner_id'])

			cur.execute('SELECT a.printablekey as "s_printablekey",a.publickey as "s_publickey",s.* FROM signatures s,accounts a WHERE s.multisig_id = %s AND s.signer_id=a.id', (data['id'],))
			data['signatures'] = cur.fetchall()
			data['signatures_count'] = len(data['signatures'])
		cur.close()
		return data

	def getMultisigs(self, txId, limit):
		cur = self.conn.cursor()
		cur.execute(self.getMultisigSql('id', '<', 'LIMIT '+str(limit)), (txId,))
		data = cur.fetchall()
		cur.close()
		return data

	def getBlockMultisigs(self, height):
		cur = self.conn.cursor()
		cur.execute(self.getMultisigSql('block_height', '=', ''), (height,))
		data = cur.fetchall()
		cur.close()
		return data

	def getNamespaceSql(self,compare, comparator, limit):
		return 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",sink.printablekey as "sink_printablekey",sink.publickey as "sink_publickey",t.* FROM namespaces t,accounts s,accounts sink WHERE t.{}{}%s AND t.signer_id=s.id AND t.rental_sink=sink.id ORDER BY id DESC {}'.format(compare,comparator,limit)

	def getNamespace(self, compare, dataCompare):
		cur = self.conn.cursor()
		print cur.mogrify(self.getNamespaceSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		cur.execute(self.getNamespaceSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		data = cur.fetchone()
		cur.close()
		return data

	def getNamespaces(self, txId, limit):
		cur = self.conn.cursor()
		cur.execute(self.getNamespaceSql('id', '<', 'LIMIT '+str(limit)), (txId,))
		data = cur.fetchall()
		cur.close()
		return data

	def getBlockNamespaces(self, height):
		cur = self.conn.cursor()
		cur.execute(self.getNamespaceSql('block_height', '=', ''), (height,))
		data = cur.fetchall()
		cur.close()
		return data


	def getRootNamespaces(self):
		sql = 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",sink.printablekey as "sink_printablekey",sink.publickey as "sink_publickey",t.* FROM namespaces t,accounts s,accounts sink WHERE t.parent_ns IS NULL AND t.signer_id=s.id AND t.rental_sink=sink.id ORDER BY t.namespace_name ASC'
		cur = self.conn.cursor()
		cur.execute(sql)
		data = cur.fetchall()
		cur.close()
		return data

	def getNamespacesFrom(self, nsId):
		sql = 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",sink.printablekey as "sink_printablekey",sink.publickey as "sink_publickey",t.* FROM namespaces t,accounts s,accounts sink WHERE t.parent_ns = %s AND t.signer_id=s.id AND t.rental_sink=sink.id ORDER BY t.namespace_name ASC'
		cur = self.conn.cursor()
		cur.execute(sql, (nsId,))
		data = cur.fetchall()
		cur.close()
		return data


	def getMosaicSql(self, compare, comparator, limit):
		return 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",sink.printablekey as "sink_printablekey",sink.publickey as "sink_publickey",t.* FROM mosaics t,accounts s,accounts sink WHERE t.{}{}%s AND t.signer_id=s.id AND t.creation_sink=sink.id ORDER BY id DESC {}'.format(compare,comparator,limit)

	def getMatchingMosaics(self, ids, limit):
		cur = self.conn.cursor()
		cur.execute(self.getMosaicSql('id', ' IN ', 'LIMIT '+str(limit)), (tuple(list(ids)),))
		data = cur.fetchall()

		cur.execute('SELECT p.mosaic_id as "mosaic_id", p.name as "name",p.value as "value" FROM mosaic_properties p WHERE p.mosaic_id IN %s', (tuple(list(ids)),))
		_properties = cur.fetchall()
		properties = defaultdict(list)
		for e in _properties:
			properties[e['mosaic_id']].append(e)


		#cur.execute('SELECT * FROM mosaic_supplies s WHERE s.mosaic_id IN %s', (tuple(list(ids)),))
		#_supplies = cur.fetchall()

		sql = "SELECT * FROM (SELECT *,row_number() OVER (PARTITION BY mosaic_id ORDER BY block_height DESC) AS rn FROM mosaic_state_supply WHERE mosaic_id in %s) q where rn = 1"
		cur.execute(sql, (tuple(list(ids)),))
		_supplies = cur.fetchall()
	
		supplies = {}
		for e in _supplies:
			supplies[e['mosaic_id']] = e

		print properties
		print "--- --- --- --- ---"
		print supplies
		print "--- --- --- --- ---"

		for e in data:
			print e
			print "---"

			e['properties'] = properties[e['id']]
			e['properties_count'] = len(e['properties'])
			e['supply'] = supplies[e['id']]

		cur.close()
		return data
	
	def _getMosaic(self, cur, compare, dataCompare, level=1):
		cur.execute(self.getMosaicSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		#print "SQL query: ", cur.mogrify(self.getMosaicSql(compare, '=', 'LIMIT 1'), (dataCompare,))
		data = cur.fetchone()
		#print "result", data

		cur.execute('SELECT p.name as "name",p.value as "value" FROM mosaic_properties p WHERE p.mosaic_id = %s', (data['id'],))
		data['properties'] = cur.fetchall()
		data['properties_count'] = len(data['properties'])

		#cur.execute('SELECT * FROM mosaic_supplies s WHERE s.mosaic_id = %s', (data['id'],))
		#data['supplies'] = cur.fetchall()
		#data['supplies_count'] = len(data['supplies'])

		_supplies = self._getPreviousStateSupply(cur, (data['id'], ))
		data['supply'] = _supplies

		cur.execute('SELECT r.printablekey as "r_printablekey",r.publickey as "r_publickey", m.block_height,m.type,m.fee_mosaic_id,m.fee,m.recipient_id FROM mosaic_levys m,accounts r WHERE mosaic_id = %s AND m.recipient_id=r.id ', (data['id'],))
		data['levy'] = cur.fetchone()

		if data['levy']:
			if level != 0:
				levyId = data['levy']['fee_mosaic_id']
				data['levy']['fee_mosaic'] = self._getMosaic(cur, 'id', levyId, level-1)
			else:
				data['levy']['fee_mosaic'] = {0:'unavailable'}

			del data['levy']['fee_mosaic_id']

		return data

	def getMosaic(self, compare, dataCompare):
		cur = self.conn.cursor()
		data = self._getMosaic(cur, compare, dataCompare)

		cur.execute('SELECT * FROM transfer_attachments t WHERE t.mosaic_id = %s ORDER BY id DESC LIMIT 10', (data['id'],))
		att = cur.fetchall()
		ids = set(map(lambda e: e['transfer_id'], att))
		txes = self.getMatchingTransfers(ids, 10) if len(ids) else []
		data['txes'] = txes
		cur.close()
		return data


	def getMosaics(self, txId, limit):
		cur = self.conn.cursor()
		cur.execute(self.getMosaicSql('id', '<', 'LIMIT ' + str(limit)), (txId,))
		data = cur.fetchall()
		cur.close()
		return data
	
	def getBlockMosaics(self, height):
		cur = self.conn.cursor()
		cur.execute(self.getMosaicSql('block_height', '=', ''), (height,))
		data = cur.fetchall()
		cur.close()
		return data


	def getMosaicsFrom(self, nsId):
		sql = 'SELECT s.printablekey as "s_printablekey",s.publickey as "s_publickey",sink.printablekey as "sink_printablekey",sink.publickey as "sink_publickey",t.* FROM mosaics t,accounts s,accounts sink WHERE t.parent_ns = %s AND t.signer_id=s.id AND t.creation_sink=sink.id ORDER BY t.mosaic_fqdn'
		cur = self.conn.cursor()
		cur.execute(sql, (nsId,))
		data = cur.fetchall()
		cur.close()
		return data

	def getTransactionByHash(self, name, txHash):
		switch = {
			'transfers': self.getTransfer
			, 'delegates': self.getDelegate
			, 'modifications': self.getModification
			, 'multisigs': self.getMultisig
			, 'namespaces': self.getNamespace
			, 'mosaics': self.getMosaic
		}
		return switch[name]('hash', tobin(txHash))
		
	def getTransactionsByType(self, name, txId, limit):
		switch = {
			'transfers': self.getTransfers
			, 'delegates': self.getDelegates
			, 'modifications': self.getModifications
			, 'multisigs': self.getMultisigs
			, 'namespaces': self.getNamespaces
			, 'mosaics': self.getMosaics
		}
		return switch[name](txId, limit)

	def getTableNext(self, table, txId, limit):
		cur = self.conn.cursor()
		cur.execute('SELECT id FROM '+table+' WHERE id > %s ORDER BY id ASC LIMIT '+str(limit), (txId,))
		data = cur.fetchall()
		cur.close()
		return data

	def getAccount(self, address):
		start = datetime.now()
		cur = self.conn.cursor()
		cur.execute('SELECT a.* FROM accounts a WHERE a.printablekey = %s', (address,))
		data = cur.fetchone()
		cur.close()
		print "time get", (datetime.now() - start).microseconds
		return data

	def getAccountById(self, accid):
		cur = self.conn.cursor()
		cur.execute('SELECT a.* FROM accounts a WHERE a.id = %s', (accid,))
		data = cur.fetchone()
		cur.close()
		return data

	def getAccountsByIds(self, ids):
		cur = self.conn.cursor()
		cur.execute('SELECT a.* FROM accounts a WHERE a.id in %s', (tuple(list(ids)),))
		data = cur.fetchall()
		cur.close()
		return data

	def getHarvestedBlocksCount(self, accid):
		start = datetime.now()
		cur = self.conn.cursor()
		cur.execute('SELECT COUNT(*) AS "harvested_count" FROM harvests WHERE account_id = %s', (accid,))
		data = cur.fetchone()['harvested_count']
		cur.close()
		print "time count", (datetime.now() - start).microseconds
		return data

	def getHarvestedBlocksReward(self, accid):
		start = datetime.now()
		cur = self.conn.cursor()
		cur.execute('SELECT type,cast(SUM(amount) as bigint) AS "sum" FROM inouts WHERE account_id=%s GROUP BY type', (accid,))
		data = cur.fetchall()
		cur.close()
		print "time reward", (datetime.now() - start).microseconds
		return data

	def getHarvestersByCount(self):
		cur = self.conn.cursor()
		cur.execute("""SELECT COUNT(h.block_height) AS harvestedblocks,MAX(h.block_height) AS lastBlock,h.account_id as id from harvests h GROUP BY h.account_id ORDER BY harvestedblocks DESC LIMIT 50""");
		#cur.execute("""SELECT COUNT(h.block_height) AS harvestedblocks,MAX(h.block_height) AS lastBlock,a.id from harvests h,accounts a where a.id=h.account_id GROUP BY a.id ORDER BY harvestedblocks DESC LIMIT 50""")
		data = cur.fetchall()
		cur.close()
		return data

	def getHarvestersFees(self, ids):
		start = datetime.now()
		cur = self.conn.cursor()
		cur.execute("""SELECT i.account_id, cast(CEIL(SUM(i.amount)/1000000) AS BIGINT) as fees FROM inouts i WHERE i.account_id IN %s AND i.type=3 GROUP BY i.account_id""", (tuple(list(ids)),));
		data = cur.fetchall()
		cur.close()
		print "time fees", (datetime.now() - start).microseconds
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

