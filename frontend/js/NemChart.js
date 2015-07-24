var NemChart = function(app) {
	this.helpers({
	renderTxes: function(items) {
		var balances = new Array();
		for (var i = 0; i < items.length; ++i) {
			var t = items[i]['type'];
			var elem = new Array(
				items[i]['block_height']
				, ((t == 2 || t == 5) ? -1 : 1)*(items[i]['amount'] / 1000000)
			);
			balances.push(elem);
			console.log(elem);
		}

	
		g_chart = new Dygraph(
			document.getElementById("balances"),
			balances,
			{
				labels: [ 'Height', 'Amount'],
				labelsDiv: 'balances_labels',
				colors: ['#8e8e8e','#dfa82f', '#41ce7b'],
				ylabel: 'Block time (seconds)',
				legend: 'always',
				series: {
				},
				axes: {
					y: {
						// set axis-related properties here
						drawGrid: false,
						independentTicks: true
					},
				},
				axisLabelColor: '#000',
				strokeWidth: 1.75,
				highlightCircleSize: 4,
			}
		);

	},
	renderChart: function (idxes, difficulties, fees, ts, avg) {
		var prepareData = function (idxes, diffs, fees, ts, avgBlocks) {
			var blocktimes = new Array();
			var difficulties = new Array();
			
			// add first element twice (on purpose)
			var sumStack = ts[0];
			for (var i = 0; i < avgBlocks - 1; ++i) {
				sumStack += ts[i];
			}
			for (var i = (avgBlocks - 1); i < ts.length; ++i) {
				sumStack -= ts[i - (avgBlocks - 1)];
				sumStack += ts[i];

				var block = new Array(
					idxes[i]
					, ts[i]
					, (sumStack / avgBlocks)
				);
				blocktimes.push(block);
			}

			for (var i = 0; i < ts.length; ++i) {
				var block = new Array(
					idxes[i]
					, diffs[i]
				);
				difficulties.push(block);
			}

			return [blocktimes, difficulties];
		};

		var all = prepareData(idxes, difficulties, fees, ts, avg);
		var blocktimes = all[0];
		var difficulties = all[1];
		
		var nblocks = blocktimes.length;
		var initialSelector = Array();
		initialSelector.push(blocktimes[nblocks-1][0]);
		initialSelector.push(initialSelector[0]-720);
		initialSelector = initialSelector.reverse();
		
		g_chart = new Dygraph(
			document.getElementById("blocktimes"),
			blocktimes,
			{
				labels: [ 'Height', 'Time', 'Average'],
				labelsDiv: 'blocktimes_labels',
				colors: ['#8e8e8e','#dfa82f', '#41ce7b'],
				ylabel: 'Block time (seconds)',
				y2label: 'Average block time (seconds)',
				legend: 'always',
				showRangeSelector: true,
				dateWindow: initialSelector,
				series: {
					'Average': {
						axis: 'y2',
						stepPlot: false,
					},
					'#TXs': {
						axis: 'y2',
						stepPlot: false,
					}
				},
				axes: {
					y: {
						// set axis-related properties here
						drawGrid: false,
						independentTicks: true
					},
					y2: {
						// set axis-related properties here
						valueRange: [40,80],
						labelsKMB: false,
						drawGrid: true,
						independentTicks: true,
					}
				},

				axisLabelColor: '#000',
				strokeWidth: 1.75,
				highlightCircleSize: 4,
			}
		);

		g_chart_2 = new Dygraph(
			document.getElementById("difficulties"),
			difficulties,
			{
				labels: [ 'Height', 'Difficulty'],
				labelsDiv: 'difficulties_labels',
				colors: ['#41ce7b'],
				ylabel: 'Difficulty',
				legend: 'always',
				showRangeSelector: true,
				dateWindow: initialSelector,
				axes: {
					y: {
						// set axis-related properties here
						drawGrid: true,
						independentTicks: true
					},
				},

				axisLabelColor: '#000',
				strokeWidth: 1.75,
				highlightCircleSize: 4,
			}
		);
		//*/

	}
	});
};


