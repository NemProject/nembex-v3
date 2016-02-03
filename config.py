class Config:
	def __init__(self):
		pass

config = Config
config.network = 'testnet'
if config.network == 'testnet':
	config.connection_string ="dbname=nembex2db user=nembexuser password=nembexpass"
	config.nodes = ['localhost']
	config.crawlerSeed = config.nodes[0]
	# http || file, file is for restoring db from FS backup, and should not be used
	config.api = 'http'
	config.nemesis = 'TBULEAUG2CZQISUR442HWA6UAKGWIXHDABJVIPS4'
else:
	config.connection_string="dbname=nembexdb user=nembexuser password=nembexpass"
	config.nodes = ['san.nem.ninja', 'go.nem.ninja', 'hachi.nem.ninja', 'jusan.nem.ninja', 'nijuichi.nem.ninja']
	config.crawlerSeed = config.nodes[0]
	# http || file, file is for restoring db from FS backup, and should not be used
	config.api = 'http'
	config.nemesis = 'NANEMOABLAGR72AZ2RV3V4ZHDCXW25XQ73O7OBT5'
