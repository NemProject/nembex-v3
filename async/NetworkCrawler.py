import asyncio
from concurrent.futures import ALL_COMPLETED
import aiohttp
from NodeEndpoint import NodeEndpoint
import networkx as nx


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
	def crawl(self, endpoint):
		endpointUrl = endpoint.url()

		if endpointUrl in self.allNodes and endpointUrl in self.allInfo:
			return

		if not endpointUrl in self.allInfo:
			#print('getting info {0}'.format(endpointUrl))
			self.allInfo[endpointUrl] = True
			try:
				response = yield from asyncio.wait_for(aiohttp.request('GET', '{0}/node/extended-info'.format(endpointUrl)), 2)
				info = yield from response.json()
				version = info['node']['metaData']['version']
				network = info['node']['metaData']['networkId']
				if (self.isTest and network == -104) or (not self.isTest and network != -104):
					self.allInfo[endpointUrl] = info
					
					response = yield from asyncio.wait_for(aiohttp.request('GET', '{0}/chain/height'.format(endpointUrl)), 2)
					info = yield from response.json()
					self.allInfo[endpointUrl]['node']['metaData']['height'] = info['height']
					yield from asyncio.wait([self.crawl(endpoint)], return_when=ALL_COMPLETED)

			except aiohttp.errors.ClientOSError as err:
				print('error detected:', str(err))

			except aiohttp.errors.ServerDisconnectedError as err:
				print('error detected:', str(err))

			except Exception as e:
				#print("Exceptionxxxx:", str(e))
				pass

		if not endpointUrl in self.allNodes:
			#print('processing {0}'.format(endpointUrl))
			self.allNodes[endpointUrl] = True
			try:
				response = yield from asyncio.wait_for(aiohttp.request('GET', '{0}/node/peer-list/active'.format(endpoint.url())), 2)
				peers = yield from response.json()

				futures = []
				for peer in peers['data']:
					peer_endpoint = NodeEndpoint.from_json(peer['endpoint'])
					futures.append(self.crawl(peer_endpoint))

				yield from asyncio.wait(futures, return_when=ALL_COMPLETED)
			except aiohttp.errors.ClientOSError as err:
				print('error detected:', str(err))

			except aiohttp.errors.ServerDisconnectedError as err:
				print('error detected:', str(err))

			except Exception as e:
				#print("Exceptionxxxx:", str(e))
				pass
