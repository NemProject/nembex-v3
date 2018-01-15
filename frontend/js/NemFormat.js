var NemFormat = function(app) {
	//app.use(XBBCODE);

	this.helpers({
	hex2a: function (hexx) {
		var hex = hexx.toString();
		var str = '';
		for (var i = 0; i < hex.length; i += 2)
			str += String.fromCharCode(parseInt(hex.substr(i, 2), 16));
		return str;
	},
	fmtNemHeight: function(key, data) {
		if (!(key in data)) { return; }
		var o = data[key].toString();
		var padded = Array(12 + 1 - o.length).join('&nbsp;') + o + '&nbsp;&nbsp;';
		data[key + '_fmt'] = padded;
	},
	fmtNemValue: function(key,data) {
		if (data===null || !(key in data)) { return; }
		var o = data[key];
		if (! o) {
			o = "0.<span class='dim'>000000</span>";
		} else {
			o = o / 1000000;
			var b = o.toFixed(6).split('.');
			var r = "<span class='sep'>" +b[0].split(/(?=(?:...)*$)/).join("</span><span class='sep'>") + "</span>";
			o = r + ".<span class='dim'>" + b[1] + "</span>";
		}
		data[key + '_fmt'] = o;
	},
	fmtQuantity: function(key, data, mosaic) {
		if (data===null || !(key in data)) { return; }
		var decimals = mosaic['properties_map']['divisibility'];
		var o = data[key];
		if (! o) {
			o = "0.<span class='dim'>" + "000000".substr(0, decimals) + "</span>";
		} else {
			o = o / Math.pow(10, decimals);;
			var b = o.toFixed(decimals).split('.');
			var r = "<span class='sep'>" +b[0].split(/(?=(?:...)*$)/).join("</span><span class='sep'>") + "</span>";
			o = r + "<span class='dim'>." + (b[1] || '') + "</span>";

		}
		data[key + '_fmt'] = o;
	},
	fmtType: function(key, data) {
		if (data===null || !(key in data)) { return; }
		var o = data[key];
		if (o === 12) {
			o = 'levy';
		} else {
			o = '';
		}
		data[key + '_fmt'] = o;
	},
	fmtNemImportanceScore: function(key,data) {
		if (!(key in data)) { return; }
		var o = data[key];
		if (o) {
			o *= 10000;
			o = o.toFixed(4).split('.');
			o = o[0] + ".<span class='dim'>" + o[1] + "</span>";
		}
		data[key + '_fmt'] = o;
	},
	calculateDiff: function(diff) {
		return Math.floor(diff / 10000000000) / 100;
	},
	fmtNemDifficulty: function(key, data) {
		data[key + '_fmt'] = (data[key] / 1000000000000).toFixed(2);
	},
	fmtNemAddress: function(key, data) {
		if (data===null || !(key in data)) { return; }
		data[key + '_fmt'] = data[key].match(/.{1,6}/g).join('-');
	},
	fmtNemImportanceMode: function(key, data) {
		if (!(key in data)) { return; }
		var o = data[key];
		if (o == 1) { data[key +'_fmt'] = 'ACTIVATION'; }
		if (o == 2) { data[key +'_fmt'] = 'DEACTIVATION'; }
	},
	fmtNemModificationType: function(key, data) {
		if (!(key in data)) { return; }
		var o = data[key];
		if (o == 1) { data[key +'_fmt'] = 'ADD'; }
		if (o == 2) { data[key +'_fmt'] = 'DEL'; }
	},
	fmtNemSupplyType: function(key, data) {
		if (!(key in data)) { return; }
		var o = data[key];
		if (o == 1) { data[key +'_fmt'] = 'Create'; }
		if (o == 2) { data[key +'_fmt'] = 'Destroy'; }
	},
	unfmtNemAddress: function(data) {
		return data.replace(/[^a-zA-Z2-7]/g, "").toUpperCase();
	},
	unfmtHash: function(data) {
		return data.replace(/[^a-fA-F0-9]/g, "").toUpperCase();
	},
	fmtTransactionType: function(key, data) {
		if (!(key in data)) { return; }
		var translation = {
			257: 'transfer'
			, 2049: 'importance transfer'
			, 4097: 'aggregate modification'
		}
		data[key + '_fmt'] = translation[data[key]];
	},
	fmtMsg: function(data) {
		if (!('message_data' in data) || data['message_data']===null) { return; }
		if (data['message_data'].length > 0) {
			data['hasMsg'] = true;
		}
		if (data['message_type'] == 1) {
			data['plain'] = true;
			if (data['message_data'].substring(0,2) == 'fe') {
				data['message_data_fmt'] = data['message_data'].substring(2);
				data['message_hex'] = true;
			} else {
				data['message_data_fmt'] = this.hex2a(data['message_data']);
			}
		}
		if (data['message_type'] == 2) { data['enc'] = true; }
	},
	formatAttachment: function(i, item, transfer, mosaic) {
		this.fmtQuantity('quantity', item, mosaic);
		this.fmtType('type', item);
	},
	formatTransaction: function(i, item) {
		// need properties first as they might be needed
		if ('properties' in item) {
			var m  = {};
			$.each(item['properties'], function(i, it){ m[it['name']] = it['value'] });
			m['initialSupply'] = parseInt(m['initialSupply']);
			m['divisibility'] = parseInt(m['divisibility']);
			item['properties_map'] = m;
		}

		this.fmtNemValue('amount', item);
		this.fmtNemValue('fee', item);
		this.fmtNemValue('rental_fee', item);
		this.fmtNemValue('creation_fee', item);
		this.fmtNemAddress('s_printablekey', item);
		this.fmtNemAddress('r_printablekey', item);
		this.fmtNemImportanceMode('mode', item);
		this.fmtNemValue('total_fees', item);
		this.fmtTransactionType('inner_type', item);
		this.fmtNemSupplyType('supply_type', item);
		this.fmtMsg(item);
		if ('modifications' in item) {
			var _s = this;
			$.each(item['modifications'], function(i,it){ 
				_s.fmtNemModificationType('type', it);
				_s.fmtNemAddress('printablekey', it);
			});
		}
		if ('properties' in item) {
			var p = item['properties_map'];
			item['total_supply'] = item['supply']['quantity'];
		}
		if ('levy' in item && item['levy']) {
			this.fmtNemAddress('r_printablekey', item['levy']);
			this.formatTransaction(0, item['levy']['fee_mosaic']);
		}
	},
	fmtTime: function(key, data) {
		if (!(key in data)) { return; }
		var o = data[key];
		var t = (new Date(o*1000));
		var now = (new Date).getTime();
		data[key + '_fmt'] = t.toUTCString();
		data[key + '_sec'] = ((now/1000) - o).toFixed(0);
	},
	fmtNodeOs: function(item) {
		var alices = {
			'cd94cdcfde6878e093bc70e35b575dbe68095c69f73112e67559f71c1fb64c6e':'alice2',
			'47ff934888ed8ea66433c889630bf67a46d47717e99e2f29db5b3866e7cc4c89':'alice3',
			'd11ada95014685240ae69b0e44d60f931e61d444604a6a1b74ea085360dca396':'alice4',
			'6ecd181da287c9ccb0075336de36427f25cbc216dc6b1f0e87e35e41a39f63fe':'alice5',
			'00a30788dc1f042da959309639a884d8f6a87086cda10300d2a7c3a0e0891a4d':'alice6',
			'64f0c867c52e8d3f7fe478854dbb197646d06041cc16b56595c03a217af6564b':'alice7',
			'35ee63ac6c4bbf1cb56eeaf5ce4edc591ab0c27e1598672d7915af2333224ece':'bigalice3'
		};
		function platformToOs(d) {
			if (d == null) return "unk";
			if (d.match(/on linux/gi)) return "lin";
			if (d.match(/on mac/gi)) return "mac";
			if (d.match(/on windows/gi)) return "win";
			
			return "unk";
		}
		item['os'] = platformToOs(item['metaData']['platform']);
		if (item['identity']['public-key'] in alices) {
			item['os'] = 'alice';
			item['metaData']['version'] = '0.6.0-D';
		}
	},
	fmtNodeVersion: function(item) {
		var latest = [0, 6, 95];
		var v = item['metaData']['version'];
		item['metaData']['version_fmt'] = v;
		var v2 = v.split(/-/);
		if (v2[1] == 'DEVELOPER BUILD' || v2[1] == 'D') {
			v2[1] = 'dev';
			item['metaData']['version_fmt'] = v2.join('-');
			return;
		}
		var v3 = v2[0].split(/\./).map(function(x){return parseInt(x);});
	
		var h = latest[2] - v3[2];

		if (v3[1] !== latest[1] || h > 4) {
			item['metaData']['version_fmt'] = '<span style="color:red">' + item['metaData']['version_fmt'] + '</span>';
			return;
		}
	
		if (0 == h) {
			item['metaData']['version_fmt'] = '<span style="color:#41ce7b">' + item['metaData']['version_fmt'] + '</span>';
		} else if (1 == h) {
			item['metaData']['version_fmt'] = '<span style="color:#dfa82f">' + item['metaData']['version_fmt'] + '</span>';
		} else if (h > 0) {
			item['metaData']['version_fmt'] = '<span style="color:#c60">' + item['metaData']['version'] + '</span>';
		}
	},
	fmtNodeName: function(item) {
		var n = item['identity']['name'];
		item['identity']['name_fmt'] = n || '[i]>empty<[/i]';
		item['identity']['name_fmt'] = XBBCODE.process({
			text: item['identity']['name_fmt'],
			removeMisalignedTags: true,
			addInLineBreaks: false
		}).html;
	},
	fmtUptime: function(item) {
		if ('nisInfo' in item) {
			var start = item['nisInfo']['startTime'];
			var end = item['nisInfo']['currentTime'];
			var hours = Math.floor((end - start) / 3600);
			var days = Math.floor(hours / 24);
			hours = hours -  24 * days;
			item['uptime'] = days + 'd, ' + hours + 'h ';
		} else {
			item['uptime'] = 'n/a';
		}
	}
	//var nemEpoch = Date.UTC(2015, 2, 29, 0, 6, 25, 0)
	});
};
