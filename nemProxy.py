from retrievers import synchronous

from config import config
from collections import Counter
import json

class NemProxy:
	def __init__(self):
		nodes = config.nodes
		if config.api == 'file':
			self.nodes = [synchronous.FileApi()]
		elif config.api == 'http':
			self.nodes = [synchronous.HttpApi(baseurl='http://' + n + ':7890/') for n in nodes]
	
	def getLastHeight(self):
		try:
			data = [n.getHeight() for n in self.nodes]
			res = filter(lambda r: r != None, data)
			res = min([n['height'] for n in res])
		except:
			res = 10
		print 'last height', res
		return res
		
	def getBlockAt(self, height):
		try:
			data = [n.getBlockAt(height) for n in self.nodes]
		except:
			print " [!] Exception OCCURED"
			return None
		res = filter(lambda r: r != None, data)
		r = res[0]
		for o in res[1:]:
			if r != o:
				raise RuntimeError('fork detected')
		return r


	def getAccount(self, address):
		try:
			data = [n.getAccount(address) for n in self.nodes]
			res = map(json.dumps, filter(lambda r: r != None, data))
			res = json.loads(Counter(res).most_common(1)[0][0])
		except Exception as e:
			print " [!] Exception", e
			return None
		return res

