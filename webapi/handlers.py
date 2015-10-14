import tornado.web

from binascii import hexlify
from nemdb import Db
from nemProxy import NemProxy
import datetime,json,re,time
from config import config

def tostring(x):
	if x == None:
		return None
	return hexlify(bytes(x))

def fixer(data):
	nemEpoch = datetime.datetime(2015, 3, 29, 0, 6, 25, 0, None)
	def _calc_unix(nemStamp):
		r = nemEpoch + datetime.timedelta(seconds=nemStamp)
		return (r - datetime.datetime.utcfromtimestamp(0)).total_seconds()

	def _calc_timestamp(timestamp):
		r = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))
		return r


	def _fix(elem):
		if isinstance(elem, dict):
			if 'hash' in elem:
				elem['hash'] = tostring(elem['hash'])
			if 'signature' in elem:
				elem['signature'] = tostring(elem['signature'])
			if 'sink_publickey' in elem:
				elem['sink_publickey'] = tostring(elem['sink_publickey'])
			if 's_publickey' in elem:
				elem['s_publickey'] = tostring(elem['s_publickey'])
			if 'r_publickey' in elem:
				elem['r_publickey'] = tostring(elem['r_publickey'])
			if 'publickey' in elem:
				elem['publickey'] = tostring(elem['publickey'])
			if 'message_data' in elem:
				elem['message_data'] = tostring(elem['message_data'])
			if 'deadline' in elem:
				t = elem['deadline']
				elem['deadline'] = _calc_timestamp(_calc_unix(t))
				elem['deadline_nem'] = t

			if 'modifications' in elem:
				for e in elem['modifications']:
					_fix(e)
			if 'signatures' in elem:
				for e in elem['signatures']:
					_fix(e)
			if 'inner' in elem:
				_fix(elem['inner'])

			if 'supplies' in elem:
				for e in elem['supplies']:
					_fix(e)
			if 'txes' in elem:
				for e in elem['txes']:
					_fix(e)
			if elem.get('levy'):
				_fix(elem['levy'])
				_fix(elem['levy']['fee_mosaic'])
			
			if 'mosaics' in elem:
				for e in elem['mosaics'].itervalues():
					_fix(e)
		return elem
	
	if not isinstance(data, list):
		return _fix(data)

	for elem in data:
		_fix(elem)

	return data

class DbFixer(object):
	__slots__ = ["_obj", "__weakref__"]
	def __init__(self, obj):
		object.__setattr__(self, "_obj", obj)
	
	#
	# proxying (special cases)
	#
	def __getattribute__(self, name):
		thiz = object.__getattribute__(self, "_obj")
		meth = getattr(thiz, name)
		def methodproxy(*args):
			ret = meth(*args)
			return fixer(ret)
		return methodproxy

	def __delattr__(self, name):
		delattr(object.__getattribute__(self, "_obj"), name)
	def __setattr__(self, name, value):
		setattr(object.__getattribute__(self, "_obj"), name, value)

	def __nonzero__(self):
		return bool(object.__getattribute__(self, "_obj"))
	def __str__(self):
		return str(object.__getattribute__(self, "_obj"))
	def __unicode__(self):
		return unicode(object.__getattribute__(self, "_obj"))
	def __repr__(self):
		return repr(object.__getattribute__(self, "_obj"))
	
	#
	# factories
	#
	_special_names = [
		'__abs__', '__add__', '__and__', '__call__', '__cmp__', '__coerce__', 
		'__contains__', '__delitem__', '__delslice__', '__div__', '__divmod__', 
		'__eq__', '__float__', '__floordiv__', '__ge__', '__getitem__', 
		'__getslice__', '__gt__', '__hash__', '__hex__', '__iadd__', '__iand__',
		'__idiv__', '__idivmod__', '__ifloordiv__', '__ilshift__', '__imod__', 
		'__imul__', '__int__', '__invert__', '__ior__', '__ipow__', '__irshift__', 
		'__isub__', '__iter__', '__itruediv__', '__ixor__', '__le__', '__len__', 
		'__long__', '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__', 
		'__neg__', '__oct__', '__or__', '__pos__', '__pow__', '__radd__', 
		'__rand__', '__rdiv__', '__rdivmod__', '__reduce__', '__reduce_ex__', 
		'__repr__', '__reversed__', '__rfloorfiv__', '__rlshift__', '__rmod__', 
		'__rmul__', '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__', 
		'__rtruediv__', '__rxor__', '__setitem__', '__setslice__', '__sub__', 
		'__truediv__', '__xor__', 'next',
	]
	
	@classmethod
	def _create_class_proxy(cls, theclass):
		"""creates a proxy for the given class"""
		
		def make_method(name):
			def method(self, *args, **kw):
				return getattr(object.__getattribute__(self, "_obj"), name)(*args, **kw)
			return method
		
		namespace = {}
		for name in cls._special_names:
			if hasattr(theclass, name):
				namespace[name] = make_method(name)
		return type("%s(%s)" % (cls.__name__, theclass.__name__), (cls,), namespace)
	
	def __new__(cls, obj, *args, **kwargs):
		"""
		creates an proxy instance referencing `obj`. (obj, *args, **kwargs) are
		passed to this class' __init__, so deriving classes can define an 
		__init__ method of their own.
		note: _class_proxy_cache is unique per deriving class (each deriving
		class must hold its own cache)
		"""
		try:
			cache = cls.__dict__["_class_proxy_cache"]
		except KeyError:
			cls._class_proxy_cache = cache = {}
		try:
			theclass = cache[obj.__class__]
		except KeyError:
			cache[obj.__class__] = theclass = cls._create_class_proxy(obj.__class__)
		ins = object.__new__(theclass)
		theclass.__init__(ins, obj, *args, **kwargs)
		return ins


class BaseHandler(tornado.web.RequestHandler):
	def __init__(self, *args, **kwargs):	
		super(BaseHandler, self).__init__(*args, **kwargs)
		self.db = DbFixer(Db(True))
		self.api = NemProxy()
		
	def set_default_headers(self):
	 	self.set_header('Content-Type', 'application/json') 

	def getResult(self, table, txHash):
		return self.db.getTransactionByHash(table, txHash)

	def _getNext(self, ret, following, expectedCount):
		if len(following) == expectedCount:
			ret['next'] = following[-1]['id']
			ret['showNext'] = True
		elif len(following) > 0:
			ret['next'] = 0
			ret['showNext'] = True

	def getResults(self, table, txId, limit = 10):
		txes = self.db.getTransactionsByType(table, txId, limit)
		ret = {'txes':txes}
		if len(txes) == limit:
			ret['prev'] = txes[-1]['id']
		following = self.db.getTableNext(table, txId, limit)
		self._getNext(ret, following, limit)
		return ret

	def getMessages(self, spammers, txId, limit):
		txes = self.db.getMessages(spammers, txId, limit)
		ret = {'txes':txes}
		if len(txes) == limit:
			ret['prev'] = txes[-1]['id']
		following = self.db.getMessagesNext(spammers, txId, limit)
		self._getNext(ret, following, limit)
		return ret


	def getInouts(self, accId, iid):
		ret = {}
		txes = self.db.getInouts(accId, iid, 10)
		if len(txes) == 10:
			ret['prev'] = txes[-1]['id']
		following = self.db.getInoutsNext(accId, iid, 10)
		self._getNext(ret, following, 10)

		ret['inouts'] = txes
		ioTransfers = map(lambda x: x['tx_id'], ret['inouts'])
		if len(ioTransfers):
			ret['transfers'] = self.db.getMatchingTransfers(ioTransfers, 10)
			ret['importances'] = self.db.getMatchingDelegates(ioTransfers, 10)
			ret['aggregates'] = self.db.getMatchingModifications(ioTransfers, 10)
			ret['multisigs'] = self.db.getMatchingMultisigs(ioTransfers, 10)
		else:
			ret['transfers'] = None
			ret['importances'] = None
			ret['aggregates'] = None
			ret['multisigs'] = None
		return ret

	def getAccountMosaics(self, accId):
		accountMosaics = self.db.getAccountMosaics(accId, 10)
		mosaicIds = map(lambda x: x['mosaic_id'], accountMosaics)
		_mosaics = self.db.getMatchingMosaics(mosaicIds, 10)
		mosaics = {}
		for e in _mosaics:
			mosaics[e['id']] = e

		for e in accountMosaics:
			e['mosaic'] = mosaics[ e['mosaic_id'] ]
		return accountMosaics


class BlocksHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		height = int(self.get_argument('height', default='0'), 10)
		height = (2**63 - 10) if height == 0 else height
		qHeight = ((height - 1) / 25) * 25 + 26
		ret = self.db.getBlocks(qHeight)
		if (len(ret) > 0):
			cnt = ((ret[0]['height']-1) % 25) + 1
			ret = ret[0:cnt]
		self.write(json.dumps(ret))
		self.finish()
		
class BlocksStatsHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		ret = self.db.getBlocksStats()
		self.write(json.dumps(ret))
		self.finish()


class BlockHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		height = int(self.get_argument('height'), 10)
		ret = self.db.getBlock(height)
		self.write(json.dumps(ret))
		self.finish()

class HarvestersHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		ret = self.db.getHarvestersByCount()
		accounts = self.db.getAccountsByIds(map(lambda x: x['id'], ret))
		fees = self.db.getHarvestersFees(map(lambda x: x['id'], ret))
		accData = {}
		for a in accounts:
			accData[a['id']] = a
		for f in fees:
			accData[f['account_id']]['fees'] = f['fees']
		for h in  ret:
			accId = h['id']
			h.update(accData[accId])
		self.write(json.dumps(ret))
		self.finish()
	
def verifyAddr(addr):
	return re.sub(r'[^A-Z2-7]', '', addr)

def addRemoteInfo(db, mode, remId, dest):
	delegations = {}
	with open('delegations.json', 'r') as f:
		delegations = json.loads(f.read())

	# that's dumb 
	last = 0
	stopH = None
	if mode == 'ACTIVE':
		for k,v in delegations.iteritems():
			if v['owner'] == remId and v['start_height'] > last:
				last = v['start_height']
				if 'stop_height' in v:
					stopH = v['stop_height']
				other = k
	elif mode == 'REMOTE':
		data = delegations[str(remId)]
		other = data['owner']
		last = data['start_height']
		if 'stop_height' in data:
			stopH = data['stop_height']


	if last != 0:
		dest['other'] = db.getAccountById(other)
		dest['other']['start_height'] = last
		if stopH:
			dest['other']['stop_height'] = stopH
		

class AccountHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		addr = verifyAddr(self.get_argument('address'))
		if len(addr) != 40:
			self.write(json.dumps({'error':'invalid address'}))
			self.finish()
		else:
			ret = self.api.getAccount(addr)
			if ret == None:
				self.write(json.dumps({'error':'could not retrieve data'}))
				self.finish()
			else:
				raw = self.db.getAccount(addr)
				ret['raw'] = raw
				if raw != None:
					if ret['meta']['remoteStatus'] in ('ACTIVE', 'REMOTE'):
						addRemoteInfo(self.db, ret['meta']['remoteStatus'], raw['id'], ret['raw'])
				
					ret['raw']['harvestedBlocks'] = self.db.getHarvestedBlocksCount(raw['id'])
					ret['raw']['balance'] = self.db.getHarvestedBlocksReward(raw['id'])
				self.write(json.dumps(ret))
				self.finish()

def verifyHash(txHash):
	return re.sub(r'[^0-9A-F]', "", txHash.upper())

class TransferHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txHash = verifyHash(self.get_argument('txhash'))
		if len(txHash) != 64:
			self.write(json.dumps({'error':'invalid address'}))
			self.finish()
		else:
			ret = self.getResult('transfers', txHash)
			self.write(json.dumps(ret))
			self.finish()

class MessagesHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txId = int(self.get_argument('txid', default='0'), 10)
		txId = (2**63 - 1) if txId == 0 else txId
		spammersFrom =  ['NALICELGU3IVY4DPJKHYLSSVYFFWYS5QPLYEZDJJ','NBZMQO7ZPBYNBDUR7F75MAKA2S3DHDCIFG775N3D']
		spammersTo = ['NA5PXB3KOIEQR3XLS6QHPNXZZ5LMLCJTGZJMBKTQ','NBZMQO7ZPBYNBDUR7F75MAKA2S3DHDCIFG775N3D']
		spammers = [spammersFrom, spammersTo]
		ret = self.getMessages(spammers, txId, 25)
		ret['exceptFrom'] = spammersFrom
		ret['exceptTo'] = spammersTo
		self.write(json.dumps(ret))
		self.finish()


class TransfersHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txId = int(self.get_argument('txid', default='0'), 10)
		txId = (2**63 - 1) if txId == 0 else txId
		ret = self.getResults('transfers', txId)
		self.write(json.dumps(ret))
		self.finish()

class ImportanceHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txHash = verifyHash(self.get_argument('txhash'))
		if len(txHash) != 64:
			self.write(json.dumps({'error':'invalid address'}))
			self.finish()
		else:
			ret = self.getResult('delegates', txHash)
			self.write(json.dumps(ret))
			self.finish()


class ImportancesHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txId = int(self.get_argument('txid', default='0'), 10)
		txId = (2**63 - 1) if txId == 0 else txId
		ret = self.getResults('delegates', txId)
		self.write(json.dumps(ret))
		self.finish()

class AggregateHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txHash = verifyHash(self.get_argument('txhash'))
		if len(txHash) != 64:
			self.write(json.dumps({'error':'invalid address'}))
			self.finish()
		else:
			ret = self.getResult('modifications', txHash)
			self.write(json.dumps(ret))
			self.finish()


class AggregatesHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txId = int(self.get_argument('txid', default='0'), 10)
		txId = (2**63 - 1) if txId == 0 else txId
		ret = self.getResults('modifications', txId)
		self.write(json.dumps(ret))
		self.finish()

class MultisigHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txHash = verifyHash(self.get_argument('txhash'))
		if len(txHash) != 64:
			self.write(json.dumps({'error':'invalid address'}))
			self.finish()
		else:
			ret = self.getResult('multisigs', txHash)
			self.write(json.dumps(ret))
			self.finish()


class MultisigsHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txId = int(self.get_argument('txid', default='0'), 10)
		txId = (2**63 - 1) if txId == 0 else txId
		ret = self.getResults('multisigs', txId)
		self.write(json.dumps(ret))
		self.finish()

class NamespaceHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txHash = verifyHash(self.get_argument('txhash'))
		if len(txHash) != 64:
			self.write(json.dumps({'error':'invalid address'}))
			self.finish()
		else:
			ret = self.getResult('namespaces', txHash)
			self.write(json.dumps(ret))
			self.finish()


class NamespacesHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txId = int(self.get_argument('txid', default='0'), 10)
		txId = (2**63 - 1) if txId == 0 else txId
		ret = self.getResults('namespaces', txId)
		self.write(json.dumps(ret))
		self.finish()

class MosaicHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txHash = verifyHash(self.get_argument('txhash'))
		if len(txHash) != 64:
			self.write(json.dumps({'error':'invalid address'}))
			self.finish()
		else:
			ret = self.getResult('mosaics', txHash)
			self.write(json.dumps(ret))
			self.finish()


class BrowseHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		_ns = self.get_argument('name', default='')
		ns = _ns.split('.')
		pat = re.compile('^[0-9a-zA-z][0-9a-zA-z_-]*')
		if len(_ns) != 0 and ((len(ns) > 3) or any(map(lambda e: not pat.match(e), ns))):
			self.write(json.dumps({}))
			self.finish()
		else:
			if len(_ns) == 0:
				txes = self.db.getRootNamespaces()
				mses = None
			else:	
				root = self.db.getNamespace('namespace_name', _ns)
				txes = self.db.getNamespacesFrom(root['id'])
				mses = self.db.getMosaicsFrom(root['id'])
			ret = {'nses':txes, 'mses':mses}
			self.write(json.dumps(ret))
			self.finish()

class MosaicsHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		txId = int(self.get_argument('txid', default='0'), 10)
		txId = (2**63 - 1) if txId == 0 else txId
		ret = self.getResults('mosaics', txId)
		self.write(json.dumps(ret))
		self.finish()


class BlockTransactionsHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		height = int(self.get_argument('height'), 10)
		ret = {}
		ret['transfers'] = self.db.getBlockTransfers(height)
		ret['importances'] = self.db.getBlockDelegates(height)
		ret['aggregates'] = self.db.getBlockModifications(height)
		ret['multisigs'] = self.db.getBlockMultisigs(height)
		self.write(json.dumps(ret))
		self.finish()

class SearchHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		
		txHash = verifyHash(self.get_argument('hash'))
		if len(txHash) != 64:
			self.write(json.dumps({'error':'invalid address'}))
			self.finish()
		else:
			ret = {}
			inner = {'txid':None}
			def setRet(name, t):
				if t:
					ret[name] = [t]
					if t['signature'] is None:
						inner['txid'] = t['id']
				else:
					ret[name] = None
			
			temp = self.getResult('transfers', txHash)
			setRet('transfers', temp)

			temp = self.getResult('delegates', txHash)
			setRet('importances', temp)

			temp = self.getResult('modifications', txHash)
			setRet('aggregates', temp)

			temp = self.getResult('multisigs', txHash)
			setRet('multisigs', temp)

			if inner['txid']:
				temp = self.db.getMultisig('inner_id', inner['txid'])
				setRet('multisigs', temp)

			temp = self.db.getBlockByHash(txHash) 
			ret['block'] = temp
			self.write(json.dumps(ret))
			self.finish()
		
class AccountTransactionsHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		accId = int(self.get_argument('id'), 10)
		txId = int(self.get_argument('iid', default='0'), 10)
		txId = (2**63 - 1) if txId == 0 else txId
		ret = self.getInouts(accId, txId)
		self.write(json.dumps(ret))
		self.finish()

class AccountMosaicsHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		accId = int(self.get_argument('id'), 10)
		ret = self.getAccountMosaics(accId)
		self.write(json.dumps(ret))
		self.finish()
		

class NodesHandler(BaseHandler):
	@tornado.gen.coroutine
	def get(self):
		ret = None
		with open('async/nodes_dump-'+config.network+'.json', 'r') as inputData:
			ret = inputData.read()
		#self.write(json.dumps(ret))
		self.write(ret)
		self.finish()

