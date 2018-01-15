'''
Distributed under the MIT License, see accompanying file LICENSE.txt
'''

import tornado.web
import tornado.httpserver 
import tornado.ioloop 
import tornado.options
import os.path

import nemdb

from tornado.options import define, options

#api
from webapi.handlers import *

define("port", default=8888, help="run on the given port", type=int)

if __name__ == '__main__': 

    tornado.options.parse_command_line()

    settings = {
        "template_path": os.path.join(os.path.dirname(__file__), "templates"), 
        "static_path" : os.path.join(os.path.dirname(__file__), 'static'),
        "cookie_secret": "", #define your own here !
        "xsrf_cookies": True,
        "debug": False,
        "gzip":True,
    }

    #define the url endpoints
    app = tornado.web.Application(
        [
         #apis

         #blocks
         (r'/api/blocks', BlocksHandler)
         , (r'/api/blocks_stats', BlocksStatsHandler)

         , (r'/api/transfer', TransferHandler)
         , (r'/api/importance', ImportanceHandler)
         , (r'/api/aggregate', AggregateHandler)
         , (r'/api/multisig', MultisigHandler)
         , (r'/api/namespace', NamespaceHandler)
         , (r'/api/mosaic', MosaicHandler)

         , (r'/api/messages', MessagesHandler)
         , (r'/api/transfers', TransfersHandler)
         , (r'/api/importances', ImportancesHandler)
         , (r'/api/aggregates', AggregatesHandler)
         , (r'/api/multisigs', MultisigsHandler)
         , (r'/api/namespaces', NamespacesHandler)
         , (r'/api/mosaics', MosaicsHandler)
         , (r'/api/browse', BrowseHandler)

	 , (r'/api/account_mosaics', AccountMosaicsHandler)
         , (r'/api/account_transactions', AccountTransactionsHandler)
         , (r'/api/block_transactions', BlockTransactionsHandler)
         , (r'/api/block', BlockHandler)
         
	 , (r'/api/search', SearchHandler)

         , (r'/api/harvesters', HarvestersHandler)
         , (r'/api/account', AccountHandler)
         , (r'/api/account_net', AccountNetHandler)
         
	 , (r'/api/nodes', NodesHandler)
        ], 
        **settings
    )
    
    server = tornado.httpserver.HTTPServer(app, xheaders=True) 
    server.bind(options.port, '127.0.0.1')
    print "port: ", options.port
    server.start()
    
    tornado.ioloop.IOLoop.instance().start()

