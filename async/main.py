import ujson as json
import asyncio
import networkx as nx
from NetworkCrawler import NetworkCrawler
from NodeEndpoint import NodeEndpoint
from datetime import datetime

import sys
sys.path.append('..')
from config import config

def isValidIp(address):
	try:
		host_bytes = address.split('.')
		valid = [int(b) for b in host_bytes]
		valid = [b for b in valid if b >= 0 and b<=255]
		return len(host_bytes) == 4 and len(valid) == 4
	except:
		return False

def getResults(nodes):
	result = []
	for v in nodes:
		if v == True:
			continue
		if 'node' not in v:
			print(v)
		else:
			d = v['node']
			#if isValidIp(d['endpoint']['host']):
			#	d['endpoint']['host'] = '.'.join(d['endpoint']['host'].split('.')[0:2] + ['xx', 'xx'])
			d['nisInfo'] = v['nisInfo']
			result.append(d)
	print(" [+] {0} nodes collected".format(len(result)))
	return result

@asyncio.coroutine
def runAsync():
	index = 0
	while index < len(config.nodes):
		crawlerSeed = config.nodes[index]
		sourceEndpoint = NodeEndpoint.from_parameters('http', crawlerSeed, 7890)
		crawler = NetworkCrawler(config.network == 'testnet')

		try:
			d = yield from crawler.crawl(sourceEndpoint)
		except Exception as e:
			print("MAIN Exception: {0}".format(e))

		crawler.reset()
		result = getResults(crawler.counter.values())
		end = datetime.utcnow()
		index += 1

		if not result:
			continue

		with open('nodes_dump-'+config.network+'.json', 'w') as output:
			output.write(json.dumps({'nodes_last_time':end, 'active_nodes':result}))
		break

def crawlNetwork():
	loop = asyncio.get_event_loop()
	loop.run_until_complete(runAsync())

crawlNetwork()

