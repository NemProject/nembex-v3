import json
import requests
import base64
import datetime
from binascii import hexlify, unhexlify
from math import ceil,log

class BadResult():
	def __init__(self):
		self.ok = False

class FileApi:
	def getBlockAt(self, height):
		data = None
		with open('../blocks/b' + str(height) + '.json') as data_file:
			data = json.load(data_file)
		return data

	def getHeight(self):
		return {'height':215190}

class HttpApi():
	def __init__(self, baseurl):
		self.baseUrl = baseurl
		self.nemEpoch = datetime.datetime(2015, 3, 29, 0, 6, 25, 0, None)

	def getTimeStamp(self):
		now = datetime.datetime.utcnow()
		return int((now - self.nemEpoch).total_seconds())

	def nodeInfo(self):
		r = self.sendGet('node/info', None)
		if r.ok:
			j = r.json()
			if j['metaData']['application'] == 'NIS':
				return j
		return None

	def getBlockAt(self, height):
		r = self.sendPost('local/block/at', {'height':height})
		if r.ok:
			j = r.json()
			return j
		return None
	
		
	def getAccount(self, id):
		r = self.sendGet('account/get?address=%s' % id, None)
		if r.ok:
			j = r.json()
			return j
		return None
		
	def getHeight(self):
		r = self.sendGet('chain/height', None)
		if r.ok:
			j = r.json()
			return j
		return None

	def sendGet(self, callName, data):
		headers = {'Content-type': 'application/json' }
		uri = self.baseUrl + callName
		try:
			ret = requests.get(uri, data=json.dumps(data), headers=headers, timeout=10)
		except (requests.exceptions.ConnectionError,requests.exceptions.ReadTimeout) as e:
			return BadResult()
		return ret

	def sendPost(self, callName, data):
		headers = {'Content-type': 'application/json' }
		uri = self.baseUrl + callName
		try:
			ret = requests.post(uri, data=json.dumps(data), headers=headers, timeout=10)
		except (requests.exceptions.ConnectionError,requests.exceptions.ReadTimeout) as e:
			print e
			return BadResult()
		return ret

