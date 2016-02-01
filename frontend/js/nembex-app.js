(function($) {
	// NAVBAR related
	var 
		$window = $(window),
		$document = $(document),
		$popoverLink = $('[data-popover]'),
		$body = $('body'),
		$nav = $('.navbar'),
		navOffsetTop = $nav.offset().top;


	function resize() {
		$body.removeClass('has-docked-nav');
		navOffsetTop = $nav.offset().top;
		onScroll();
	}
	
	function onScroll() {
		if(navOffsetTop < $window.scrollTop() && !$body.hasClass('has-docked-nav')) {
			$body.addClass('has-docked-nav');
		}
		if(navOffsetTop > $window.scrollTop() && $body.hasClass('has-docked-nav')) {
			$body.removeClass('has-docked-nav');
		}
	}

	function openPopover(e) {
		e.preventDefault();
		closePopover();
		var popover = $($(this).data('popover'));
		popover.toggleClass('open');
		e.stopImmediatePropagation();
	}

	function closePopover(e) {
		if($('.popover.open').length > 0) {
			$('.popover').removeClass('open');
		}
	}

	function setActiveLink(name){
		var ul = $('ul');
		ul.children().removeClass('active');
		ul.find('a[href="#/'+name+'/"]').parent().addClass('active');
	}

	function init() {
		$window.on('scroll', onScroll);
		$window.on('resize', resize);
		$popoverLink.on('click', openPopover)
		$document.on('click', closePopover)

		$(document).keyup(function(evt) {
			if (evt.altKey) { return; }
			if (evt.ctrlKey) { return; }

			evt.preventDefault();
			switch(evt.which) {
			case 37:	// arrow left 
				$("a[rel=prev]").click();				
				break;
			case 39:	// arrow right
				$("a[rel=next]").click();
				break;
			}
		});
	}

	init();

	// Application start
	var app = $.sammy('#main', function() {
		this.use('Mustache', 'html');
		this.use(NemFormat);
		this.use(NemChart);

		// This is common for pages that display multiple txes
		// (/block/ page, /account/ page, /search/ page)
		var renderTxes = function(context, divName, txes, cb) {
			// c0392b
			var cbs = [];
			var skip = function(i) { return i['signature']; };
			if (txes['multisigs'])
				txes['multisigs'] = txes['multisigs'].filter(skip);
			if (txes['aggregates'])
				txes['aggregates'] = txes['aggregates'].filter(skip);
			if (txes['importances'])
				txes['importances'] = txes['importances'].filter(skip);
			if (txes['transfers'])
				txes['transfers'] = txes['transfers'].filter(skip);
			if (txes['namespaces'])
				txes['namespaces'] = txes['namespaces'].filter(skip);
			if (txes['mosaics'])
				txes['mosaics'] = txes['mosaics'].filter(skip);

			if (txes['mosaics'] && txes['mosaics'].length > 0) {
				$.each(txes['mosaics'], function(i,item){ context.formatTransaction(i,item); });
				cbs.push(function() {
					return context.render('t/mosaics.html')
						.appendTo(divName)
						.renderEach('t/mosaics.detail.html', txes['mosaics'])
						.appendTo('#mosaics');
				});
			}

			if (txes['namespaces'] && txes['namespaces'].length > 0) {
				$.each(txes['namespaces'], function(i,item){ context.formatTransaction(i,item); });
				cbs.push(function() {
					return context.render('t/namespaces.html')
						.appendTo(divName)
						.renderEach('t/namespaces.detail.html', txes['namespaces'])
						.appendTo('#namespaces');
				});
			}

			if (txes['multisigs'] && txes['multisigs'].length > 0) {
				$.each(txes['multisigs'], function(i,item){ context.formatTransaction(i,item); });
				cbs.push(function() {
					return context.render('t/multisigs.html')
						.appendTo(divName)
						.renderEach('t/multisigs.detail.html', txes['multisigs'])
						.appendTo('#multisigs');
				});
			}
			
			if (txes['aggregates'] && txes['aggregates'].length > 0) {
				$.each(txes['aggregates'], function(i,item){ context.formatTransaction(i,item); });
				cbs.push(function() {
					return context.render('t/aggregates.html')
						.appendTo(divName)
						.renderEach('t/aggregates.detail.html', txes['aggregates'])
						.appendTo('#aggregates');
				});
			}

			if (txes['importances'] && txes['importances'].length > 0) {
				$.each(txes['importances'], function(i,item){ context.formatTransaction(i,item); });
				cbs.push(function() {
					return context.render('t/importances.html')
						.appendTo(divName)
						.renderEach('t/importances.detail.html', txes['importances'])
						.appendTo('#importances');
				});
			}
		
			if (txes['transfers'] && txes['transfers'].length > 0) {
				$.each(txes['transfers'], function(i,item){ context.formatTransaction(i,item); });
				cbs.push(function() {
					return context.render('t/transfers.html')
						.appendTo(divName)
						.renderEach('t/transfers.detail.html', txes['transfers'])
						.appendTo('#transfers');
				});
			}

			cb(cbs);

			// call funcs in order
			(function foo() {
				if (cbs.length) {
					cbs.shift().call().then(foo);
				}
			})();
		};

		$("#searchForm").submit(function(event) {
			app.setLocation('#/search/' + $("input:first").val());
			event.preventDefault();
		});

		this.get('#/blocks/', function(context) { this.redirect('#/blocks/0'); });
		this.get('#/blocks', function(context) { this.redirect('#/blocks/0'); });
		this.get('#/transfers/', function(context) { this.redirect('#/transfers/0'); });
		this.get('#/transfers', function(context) { this.redirect('#/transfers/0'); });
		this.get('#/importances/', function(context) { this.redirect('#/importances/0'); });
		this.get('#/importances', function(context) { this.redirect('#/importances/0'); });
		this.get('#/aggregates/', function(context) { this.redirect('#/aggregates/0'); });
		this.get('#/aggregates', function(context) { this.redirect('#/aggregates/0'); });
		this.get('#/multisigs/', function(context) { this.redirect('#/multisigs/0'); });
		this.get('#/multisigs', function(context) { this.redirect('#/multisigs/0'); });
		this.get('#/namespaces/', function(context) { this.redirect('#/namespaces/0'); });
		this.get('#/namespaces', function(context) { this.redirect('#/namespaces/0'); });
		this.get('#/mosaics/', function(context) { this.redirect('#/mosaics/0'); });
		this.get('#/mosaics', function(context) { this.redirect('#/mosaics/0'); });

		this.get('#/browse', function(context) { this.redirect('#/browse/'); });

		this.get('#/account/:account', function(context) { this.redirect('#/account/'+this.params['account']+'/0'); });


		this.get('#/search/:data', function(context) {
			context.app.swap('');
			var fixedData = this.params['data'].replace(/[^a-zA-Z0-9]/g, "").toUpperCase();
			if (fixedData.length && ('MNT'.indexOf(fixedData[0]) !== -1)) {
				app.runRoute('get', '#/account/'+fixedData+'/0');
				return;
			}
			var hash = context.unfmtHash(fixedData);
			if (hash.length != 64) {
				context.render('t/search.html', {invalidSearch:true})
					.appendTo(context.$element());
				return;
			}

			context.render('t/search.html')
				.appendTo(context.$element());
				
			$.getJSON('/api3/search', {hash:hash}, function(items) {
				renderTxes(context, '#search_transactions', items, function(cbs){
					var item = items['block'];
					if (item) {
						context.fmtNemHeight('height', item);
						context.fmtNemValue('fees', item);
						context.fmtNemAddress('s_printablekey', item);
						cbs.push(function() {
							return context.render('t/blocks.html', {})
								.appendTo('#search_transactions')
								.render('t/blocks.detail.html', item)
								.appendTo('#blocks')
						});
					}
				});
			});
		});

		this.get('#/nodes/', function(context) {
			context.app.swap('');
			setActiveLink('nodes');

			$.getJSON('/api3/nodes', {}, function(items) {

				context.fmtTime('nodes_last_time', items);
				$.each(items['active_nodes'], function(i, item){
					context.fmtUptime(item);
					context.fmtNodeName(item);
					context.fmtNodeVersion(item);
					context.fmtNodeOs(item);
				});

				// nodes sorting
				function sortVersions(v1, v2) {
					var presuf1 = v1.split(/-/);
					var presuf2 = v2.split(/-/);
					var sv1 = presuf1[0].split(/\./).map(function(x){return parseInt(x);});
					var sv2 = presuf2[0].split(/\./).map(function(x){return parseInt(x);});

					if (presuf1[1] == 'D' || presuf2[1] == 'D') {
						if (presuf1[1] == 'D' && presuf2[1] == 'D') {
							return 0;
						} else if (presuf1[1] == 'D') {
							return -1;
						} else {
							return 1;
						}
					}
					var majorDiff = sv2[0] - sv1[0];
					if (majorDiff != 0) {
						return majorDiff;
					}
					var minorDiff = sv2[1] - sv1[1];
					if (minorDiff != 0) {
						return minorDiff;
					}
					return sv2[2] - sv1[2];
				}

				sortbya = "metaData";
				sortbyb = "height";
				$("#tbl thead th:eq(4)").addClass("sortable");

				var sortFunc = function(o1, o2){
					var k1 = o1[sortbya][sortbyb];
					var k2 = o2[sortbya][sortbyb];
					var v1 = o1['metaData']['version'];
					var v2 = o2['metaData']['version'];
					
					var sv = sortVersions(v1,v2);
					
					var ret = sv == 0 ? (k1 - k2) : sv;
					return ret;
					//k1 < k2 ? 1 : (k2 < k1 ? -1 : sortVersions(v1,v2));
				};
				items['active_nodes'].sort(sortFunc);

				$.each(items['active_nodes'], function(i, item){ item['seq'] = i; });

				return context.partial('t/nodes.html', items)
					.renderEach('t/nodes.detail.html', items['active_nodes'])
					.appendTo('#nodes')
			});
		});

		// gets the block up to the specified height
		this.get('#/blocks/:height', function(context) {
			context.app.swap('');
			setActiveLink('blocks');
			
			var t = parseInt(this.params['height'], 10);
			if (isNaN(t)) {
				return;
			}
			var params = t == 0 ? {} : {height:t};

			$.getJSON('/api3/blocks', params, function(items) {
				$.each(items, function(i, item){
					context.fmtNemHeight('height', item);
					context.fmtNemValue('fees', item);
					context.fmtNemAddress('s_printablekey', item);
				});
				var h = items[items.length - 1]['height'];
				var data = {};
				if (items.length == 25) { data['next'] = h + 25; }
				if (h>25) { data['prev'] = h - 25; }

				data['showNav']=true;
				context.partial('t/blocks.html', data)
					.renderEach('t/blocks.detail.html', items)
					.appendTo('#blocks');
			});
		});

		this.get('#/transfer/:txhash', function(context) {
			context.app.swap('');
			setActiveLink('transactions');
			var hash = context.unfmtHash(this.params['txhash']);
			if (hash.length != 64) {
				return;
			}
			$.getJSON('/api3/transfer', {txhash:hash}, function(items) {
				// need to do first to have properties_map
				context.formatTransaction(0,items);
				$.each(items['mosaics'], function(i, item){
					context.formatTransaction(i,item);
				});
				$.each(items['attachments'], function(j, at){
					var mosaic = items['mosaics'][at['mosaic_id']];
					var transfer = items;
					context.formatAttachment(j, at, transfer, mosaic);
					at['mosaic'] = mosaic;
				});

				context.render('t/s.transfer.html',items)
					.appendTo(context.$element());
			});
		});

		this.get('#/importance/:txhash', function(context) {
			context.app.swap('');
			setActiveLink('transactions');
			var hash = context.unfmtHash(this.params['txhash']);
			if (hash.length != 64) {
				return;
			}
			$.getJSON('/api3/importance', {txhash:hash}, function(items) {
				context.formatTransaction(0,items);
				context.render('t/s.importance.html',items)
					.appendTo(context.$element());
			});
		});

		this.get('#/aggregate/:txhash', function(context) {
			context.app.swap('');
			setActiveLink('transactions');
			var hash = context.unfmtHash(this.params['txhash']);
			if (hash.length != 64) {
				return;
			}
			$.getJSON('/api3/aggregate', {txhash:hash}, function(items) {
				context.formatTransaction(0,items);
				context.render('t/s.aggregate.html',items)
					.appendTo(context.$element());
			});
		});


		this.get('#/multisig/:txhash', function(context) {
			context.app.swap('');
			setActiveLink('transactions');
			var hash = context.unfmtHash(this.params['txhash']);
			if (hash.length != 64) {
				return;
			}
			$.getJSON('/api3/multisig', {txhash:hash}, function(items) {
				context.formatTransaction(0,items);
				context.formatTransaction(0,items['inner']);

				$.each(items['signatures'], function(i,item){ context.formatTransaction(i, item); });
				var innerTemplate = {
					257: 'transfer'
					, 2049: 'importance'
					, 4097: 'aggregate'
					, 8193: 'namespace'
					, 16385: 'mosaic'
				};
				var templateName = 't/s.' + innerTemplate[items['inner_type']] + '.html';
				context.render('t/s.multisig.html',items)
					.appendTo(context.$element())
					.render(templateName, items['inner'])
					.appendTo("#innerTx")
			});
		});

		this.get('#/namespace/:txhash', function(context) {
			context.app.swap('');
			setActiveLink('transactions');
			var hash = context.unfmtHash(this.params['txhash']);
			if (hash.length != 64) {
				return;
			}
			$.getJSON('/api3/namespace', {txhash:hash}, function(items) {
				context.formatTransaction(0,items);
				context.render('t/s.namespace.html',items)
					.appendTo(context.$element());
			});
		});

		this.get('#/mosaic/:txhash/:txid', function(context) {
			context.app.swap('');
			setActiveLink('transactions');
			var hash = context.unfmtHash(this.params['txhash']);
			if (hash.length != 64) {
				return;
			}
			var t = parseInt(this.params['txid'], 10);
			if (isNaN(t)) {
				return;
			}
			$.getJSON('/api3/mosaic', {txhash:hash,txid:t}, function(items) {
				console.log(items);
				context.formatTransaction(0,items);
				$.each(items['txes'], function(i,item){ context.formatTransaction(i, item); });
				$.each(items['txes'], function(i,item){
					$.each(item['attachments'], function(j, at){
						var mosaic = items;
						var transfer = item;
						context.formatAttachment(j, at, transfer, mosaic);
						if (at['mosaic_id'] === items['id']) {
							at['current'] = true;
						}
					});
				});

				items['showNav']=true;
				context.partial('t/s.mosaic.html',items)
					.renderEach('t/s.mosaic.detail.html', items['txes'])
					.appendTo('#transfers');
			});
		});

		this.get('#/messages/:txid', function(context) {
			var t = parseInt(this.params['txid'], 10);
			if (isNaN(t)) {
				return;
			}
			context.app.swap('');
			setActiveLink('transactions');

			$.getJSON('/api3/messages', {txid:t}, function(items) {
				$.each(items['txes'], function(i,item){ context.formatTransaction(i,item); });
				$.each(items['exceptFrom'], function(i,item){ items['exceptFrom'][i] = item.match(/.{1,6}/g).join('-'); });
				$.each(items['exceptTo'], function(i,item){ items['exceptTo'][i] = item.match(/.{1,6}/g).join('-'); });
				items['showNav']=true;
				context.partial('t/messages.html', items)
					.renderEach('t/messages.detail.html', items['txes'])
					.appendTo('#messages');
			});
		});


		this.get('#/transfers/:txid', function(context) {
			var t = parseInt(this.params['txid'], 10);
			if (isNaN(t)) {
				return;
			}
			context.app.swap('');
			setActiveLink('transactions');

			$.getJSON('/api3/transfers', {txid:t}, function(items) {
				$.each(items['txes'], function(i,item){ context.formatTransaction(i,item); });
				items['showNav']=true;
				context.partial('t/transfers.html', items)
					.renderEach('t/transfers.detail.html', items['txes'])
					.appendTo('#transfers');
			});
		});


		this.get('#/importances/:txid', function(context) {
			var t = parseInt(this.params['txid'], 10);
			if (isNaN(t)) {
				return;
			}
			context.app.swap('');
			setActiveLink('transactions');

			$.getJSON('/api3/importances', {txid:t}, function(items) {
				$.each(items['txes'], function(i,item){ context.formatTransaction(i,item); });
				items['showNav']=true;
				context.partial('t/importances.html', items)
					.renderEach('t/importances.detail.html', items['txes'])
					.appendTo('#importances');
			});
		});


		this.get('#/aggregates/:txid', function(context) {
			var t = parseInt(this.params['txid'], 10);
			if (isNaN(t)) {
				return;
			}
			context.app.swap('');
			setActiveLink('transactions');

			$.getJSON('/api3/aggregates', {txid:t}, function(items) {
				$.each(items['txes'], function(i,item){ context.formatTransaction(i,item); });
				items['showNav']=true;
				context.partial('t/aggregates.html', items)
					.renderEach('t/aggregates.detail.html', items['txes'])
					.appendTo('#aggregates');
			});
		});

		this.get('#/multisigs/:txid', function(context) {
			var t = parseInt(this.params['txid'], 10);
			if (isNaN(t)) {
				return;
			}
			context.app.swap('');
			setActiveLink('transactions');

			$.getJSON('/api3/multisigs', {txid:t}, function(items) {
				$.each(items['txes'], function(i,item){ context.formatTransaction(i,item); });
				items['showNav']=true;
				context.partial('t/multisigs.html', items)
					.renderEach('t/multisigs.detail.html', items['txes'])
					.appendTo('#multisigs');
			});
		});

		this.get('#/namespaces/:txid', function(context) {
			var t = parseInt(this.params['txid'], 10);
			if (isNaN(t)) {
				return;
			}
			context.app.swap('');
			setActiveLink('transactions');

			$.getJSON('/api3/namespaces', {txid:t}, function(items) {
				$.each(items['txes'], function(i,item){ context.formatTransaction(i,item); });
				items['showNav']=true;
				context.partial('t/namespaces.html', items)
					.renderEach('t/namespaces.detail.html', items['txes'])
					.appendTo('#namespaces');
			});
		});

		this.get('#/mosaics/:txid', function(context) {
			var t = parseInt(this.params['txid'], 10);
			if (isNaN(t)) {
				return;
			}
			context.app.swap('');
			setActiveLink('transactions');

			$.getJSON('/api3/mosaics', {txid:t}, function(items) {
				$.each(items['txes'], function(i,item){ context.formatTransaction(i,item); });
				items['showNav']=true;
				context.partial('t/mosaics.html', items)
					.renderEach('t/mosaics.detail.html', items['txes'])
					.appendTo('#mosaics');
			});
		});

		this.get('#/browse/', function(context) {
			context.app.swap('');
			setActiveLink('browse');
			$.getJSON('/api3/browse', {}, function(items) {
				context.partial('t/browse.html',items)
					.renderEach('t/browse.ns.detail.html', items['nses'])
					.appendTo('#browse-ns');
			});
		});

		this.get('#/browse/:tld', function(context) {
			context.app.swap('');
			setActiveLink('browse');
			var _tld = this.params['tld'];
			var tld = _tld.split('.');
			var result = (tld.length <= 3) && tld.reduce(function(p,c){
				return p && c.match("^[a-zA-Z0-9][a-zA-Z0-9_-]*");
			}, true);
			if (! result) {
				context.render('t/browse.html')
					.appendTo(context.$element());
			} else {
				$.getJSON('/api3/browse', {name:_tld}, function(items) {
					context.partial('t/browse.html',items)
						.renderEach('t/browse.ns.detail.html', items['nses'])
						.appendTo('#browse-ns')
						.renderEach('t/browse.ms.detail.html', items['mses'])
						.appendTo('#browse-ms');
				});
			}
		});

		// shows specified account
		this.get('#/account/:id/:page', function(context) {
			context.app.swap('');
			setActiveLink('');
			var addr = context.unfmtNemAddress(this.params['id']);
			if (addr.length != 40) {
				return;
			}

			var inoutsId = parseInt(this.params['page'], 10);
			if (isNaN(inoutsId)) {
				inoutsId = 0;
			}

			$.getJSON('/api3/account', {address:addr}, function(item) {
				context.fmtNemAddress('printablekey', item['raw']);
				context.fmtNemValue('balance', item['account']);
				context.fmtNemValue('vestedBalance', item['account']);
				context.fmtNemImportanceScore('importance', item['account']);
				$.each(item['meta']['cosignatoryOf'], function(i, it){
					context.fmtNemAddress('address', it);
					context.fmtNemValue('balance', it);
				});
				$.each(item['meta']['cosignatories'], function(i, it){
					context.fmtNemAddress('address', it);
				});

				var dest = {};
				var ref = item['raw']['balance'];
				for (var t in ref) {
					var i = ref[t]['type'];
					dest[i] = ref[t]['sum'];
				}
				for (var i=1; i<6; ++i) {
					if (!(i in dest)) dest[i] = 0;
				}

				dest[6] = dest[1] + dest[4] + dest[3] - dest[2] - dest[5];
				item['raw']['balance'] = dest;
				$.each(item['raw']['balance'], function(i, it) {
					context.fmtNemValue(i, item['raw']['balance']);
				});
				if ('other' in item['raw']) {
					if (item['meta']['remoteStatus'] == 'ACTIVE') {
						item['raw']['other']['text'] = 'ACTIVE using';
					}
					else if (item['meta']['remoteStatus'] == 'REMOTE') {
						item['raw']['other']['text'] = 'REMOTE for';
					}
					context.fmtNemAddress('printablekey', item['raw']['other']);
				}

				context.render('t/account.html',item)
					.appendTo(context.$element());

				$.getJSON('/api3/account_transactions', {id:item['raw']['id'],iid:inoutsId}, function(txes) {

					renderTxes(context, '#account_transactions', txes, function(cbs){
						// make paging avail for the account data
						txes['addr'] = addr;
						txes['showNav'] = true;

						cbs.push(function() {
							return context.render('t/account.detail.html', txes)
								.appendTo('#account_transactions')
						});
					});
				});
				$.getJSON('/api3/account_mosaics', {id:item['raw']['id']}, function(data){
					$.each(data, function(i, item){
						context.formatTransaction(i,item['mosaic']);
					});
					$.each(data, function(i, item) {
						context.fmtQuantity('amount', item, item['mosaic']);
					});
					return context.render('t/account.mosaics.html')
						.appendTo('#account_mosaics')
						.renderEach('t/account.mosaics.detail.html', data)
						.appendTo('#account_mosaics_list');

				});
			});
		});

		this.get('#/block/:blockHeight', function(context) {
			var t = parseInt(this.params['blockHeight'], 10);
			if (isNaN(t)) {
				return;
			}
			context.app.swap('');
			setActiveLink('blocks');
			$.getJSON('/api3/block', {height:t}, function(block) {
				context.fmtNemAddress('s_printablekey', block);
				context.fmtNemValue('fees', block);
				context.fmtNemDifficulty('difficulty', block);
				context.render('t/block.details.html',block)
					.appendTo(context.$element());

				$.getJSON('/api3/block_transactions', {height:t}, function(txes) {
					renderTxes(context, '#block_transactions', txes, function(cbs) {
					});
				});
			});
		});

		this.get('#/statistics/', function(context) {
			context.app.swap('');
			setActiveLink('statistics');

			context.render('t/statistics.html')
				.appendTo(context.$element());
			$.getJSON('/api3/blocks_stats', function(items) {
				items.reverse();
				var prev = items[0]['timestamp_nem'];
				$.each(items, function(i, item){
					var p = item['timestamp_nem'];
					item['tsd'] = item['timestamp_nem'] - prev;
					prev = p;
				});
				var idxes = items.map(function(x){ return x['height']; });
				var diffs = items.map(function(x){ return context.calculateDiff(x['difficulty']); });
				var fees = items.map(function(x){ return Math.floor(x['fees'] / 10000) / 100; });
				var ts = items.map(function(x){ return x['tsd']; });

				$("#blocktimes_range").change(function(evt) {
					var id = this.id;
					$("label[for='" + id + "']").html(this.value);
					$('#blocktimes').empty();
					context.renderChart(idxes, diffs, fees, ts, $("#blocktimes_range").val());

				}); 
				context.renderChart(idxes, diffs, fees, ts, $("#blocktimes_range").val());
			});
		});

		this.get('#/harvesters/', function(context) {
			context.app.swap('');
			setActiveLink('harvesters');

			$.getJSON('/api3/harvesters', function(items) {
				$.each(items, function(i, item){
					context.fmtNemAddress('printablekey', item);
				});
				context.partial('t/harvesters.html')
					.renderEach('t/harvester.html', items)
					.appendTo('#harvesters');
			});
		});
	});

	$(function() {
		app.run('#/blocks/0');
	});
})(jQuery);
