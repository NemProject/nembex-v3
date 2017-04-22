import asyncio
from concurrent.futures import ALL_COMPLETED
import aiohttp
from NodeEndpoint import NodeEndpoint
import networkx as nx
import logging

logging.basicConfig(level=logging.INFO)

TIMEOUT_VALUE=4

class NetworkCrawler:
	allNodes = {}
	allInfo = {}
	counter = {}

	def __init__(self, isTest):
		self.isTest = isTest

	def reset(self):
		self.counter = self.allInfo
		self.allNodes = {}
		self.allInfo = {}

	@asyncio.coroutine
	def getNodes(self, endpoint, attempt):
		endpointUrl = endpoint.url()
		if endpointUrl in self.allNodes:
			#if attempt == 1:
			return

		self.allNodes[endpointUrl] = True
		try:
			response = yield from asyncio.wait_for(aiohttp.request('GET', '{0}/node/peer-list/all'.format(endpoint.url())), 10 * attempt)
			peers = yield from response.json()

			futures = []
			for peer in peers['active']:
				peer_endpoint = NodeEndpoint.from_json(peer['endpoint'])
				futures.append(self.crawl(peer_endpoint))

			yield from asyncio.wait(futures, return_when=ALL_COMPLETED)

		except aiohttp.errors.ClientOSError as err:
			logging.warning('peerList, error detected: {0} {1}'.format(endpointUrl, str(err)))
		except aiohttp.errors.ServerDisconnectedError as err:
			logging.warning('peerList, error detected: {0} {1}'.format(endpointUrl, str(err)))
		except asyncio.TimeoutError as err:
			# retry two times and log if fail
			if attempt <= 0:
				yield from asyncio.wait([self.getNodes(endpoint, attempt + 1)], return_when=ALL_COMPLETED)
			else:
				logging.warning('peerList, timeout attempt {2}: {0} {1}'.format(endpointUrl, str(err), attempt))

		except Exception as err:
			logging.warning('peerList, exception: {0} {1}'.format(endpointUrl, str(err)))

	@asyncio.coroutine
	def getHeight(self, endpoint, attempt):
		endpointUrl = endpoint.url()
		try:
			response = yield from asyncio.wait_for(aiohttp.request('GET', '{0}/chain/height'.format(endpointUrl)), TIMEOUT_VALUE * attempt)
			info = yield from response.json()
			self.allInfo[endpointUrl]['node']['metaData']['height'] = info['height']
			yield from asyncio.wait([self.getNodes(endpoint, 1)], return_when=ALL_COMPLETED)
		
		except aiohttp.errors.ClientOSError as err:
			logging.warning('getHeight, error detected: {0} {1}'.format(endpointUrl, str(err)))
		except aiohttp.errors.ServerDisconnectedError as err:
			logging.warning('getHeight, error detected: {0} {1}'.format(endpointUrl, str(err)))
		except asyncio.TimeoutError as err:
			if attempt <= 0:
				yield from asyncio.wait([self.getHeight(endpoint, attempt + 1)], return_when=ALL_COMPLETED)
			else:
				logging.warning('getHeight, timeout attempt {2}: {0} {1}'.format(endpointUrl, str(err), attempt))

		except Exception as err:
			logging.warning('getHeight, exception: {0} {1}'.format(endpointUrl, str(err)))

	@asyncio.coroutine
	def getInfo(self, endpoint, attempt):
		endpointUrl = endpoint.url()
		if endpointUrl in self.allInfo:
			return

		self.allInfo[endpointUrl] = True
		try:
			response = yield from asyncio.wait_for(aiohttp.request('GET', '{0}/node/extended-info'.format(endpointUrl)), TIMEOUT_VALUE * attempt)
			info = yield from response.json()
			version = info['node']['metaData']['version']
			network = info['node']['metaData']['networkId']
			if (self.isTest and network == -104) or (not self.isTest and network != -104):
				self.allInfo[endpointUrl] = info
			yield from asyncio.wait([self.getHeight(endpoint, 1)], return_when=ALL_COMPLETED)

		except aiohttp.errors.ClientOSError as err:
			logging.warning('getInfo, error detected: {0} {1}'.format(endpointUrl, str(err)))
			pass
		except aiohttp.errors.ServerDisconnectedError as err:
			logging.warning('getInfo, error detected: {0} {1}'.format(endpointUrl, str(err)))
			pass
		except asyncio.TimeoutError as err:
			if attempt <= 0:
				yield from asyncio.wait([self.getInfo(endpoint, attempt + 1)], return_when=ALL_COMPLETED)
			else:
				logging.warning('getInfo, timeout attempt {2}: {0} {1}'.format(endpointUrl, str(err), attempt))
				pass
		except Exception as err:
			logging.warning('getInfo, exception: {0} {1}'.format(endpointUrl, str(err)))

	@asyncio.coroutine
	def crawl(self, endpoint):
		endpointUrl = endpoint.url()
		if endpointUrl in self.allNodes and endpointUrl in self.allInfo:
			return

		yield from asyncio.wait([self.getInfo(endpoint, 1)], return_when=ALL_COMPLETED)
