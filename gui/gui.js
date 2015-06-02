window.mediaWiki = window.mediaWiki || {};
window.EDITOR = {};

(function($, mw) {
	var SERVICE = '/bigdata/namespace/wdq/sparql',
		SHORTURL = 'http://tinyurl.com/create.php?url=',
		NAMESPACE_SHORTCUTS = {
			'Wikidata' : {
				'wikibase' : 'http://wikiba.se/ontology#',
				'wd' : 'http://www.wikidata.org/entity/',
				'wdt' : 'http://www.wikidata.org/prop/direct/',
				'wds' : 'http://www.wikidata.org/entity/statement/',
				'p' : 'http://www.wikidata.org/prop/',
				'wdref' : 'http://www.wikidata.org/reference/',
				'wdv' : 'http://www.wikidata.org/value/',
				'ps' : 'http://www.wikidata.org/prop/statement/',
				'psv' : 'http://www.wikidata.org/prop/statement/value/',
				'pq' : 'http://www.wikidata.org/prop/qualifier/',
				'pqv' : 'http://www.wikidata.org/prop/qualifier/value/',
				'pr' : 'http://www.wikidata.org/prop/reference/',
				'prv' : 'http://www.wikidata.org/prop/reference/value/',
				'wdno' : 'http://www.wikidata.org/prop/novalue/',
				'wdata' : 'http://www.wikidata.org/wiki/Special:EntityData/'
			},
			'W3C' : {
				'rdf' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
				'rdfs' : 'http://www.w3.org/2000/01/rdf-schema#',
				'owl' : 'http://www.w3.org/2002/07/owl#',
				'skos' : 'http://www.w3.org/2004/02/skos/core#',
				'xsd' : 'http://www.w3.org/2001/XMLSchema#',
				'prov' : 'http://www.w3.org/ns/prov#'
			},
			'Social/Other' : {
				'schema' : 'http://schema.org/'
			},
			'Blazegraph' : {
				'bd' : 'http://www.bigdata.com/rdf#',
				'bds' : 'http://www.bigdata.com/rdf/search#',
				'gas' : 'http://www.bigdata.com/rdf/gas#',
				'hint' : 'http://www.bigdata.com/queryHints#'
			}
		},
		STANDARD_PREFIXES =[
				'PREFIX wd: <http://www.wikidata.org/entity/>',
				'PREFIX wdt: <http://www.wikidata.org/prop/direct/>',
				'PREFIX wikibase: <http://wikiba.se/ontology#>',
				'PREFIX p: <http://www.wikidata.org/prop/>',
				'PREFIX v: <http://www.wikidata.org/prop/statement/>',
				'PREFIX q: <http://www.wikidata.org/prop/qualifier/>',
				'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>',
		].join( "\n" ),
		QUERY_START = 0,
		CODEMIRROR_DEFAULTS = {
				lineNumbers : true,
				matchBrackets : true,
				mode : "sparql",
		},
		ERROR_LINE_MARKER = null,
		ERROR_CHARACTER_MARKER = null;

	function submitQuery(e) {
		e.preventDefault();
		EDITOR.save();

		var query = $('#query-form').serialize(),
			hash = encodeURIComponent(EDITOR.getValue()),
			url = SERVICE + "?" + query,
			settings = {
				headers : {
					'Accept' : 'application/sparql-results+json'
				},
				success : showQueryResults,
				error : queryResultsError
			};
		$('#query-result').empty('');
		$('#query-result').hide();
		$('#total').hide();
		$('#query-error').show();
		$('#query-error').text('Running query...');
		if (window.location.hash !== hash) {
			window.location.hash = hash;
		}
		QUERY_START = Date.now();
		$.ajax(url, settings);
	}

	function queryResultsError(jqXHR, textStatus, errorThrown) {
		var response,
			message = 'ERROR: ';

		if (jqXHR.status === 0) {
			message += 'Could not contact server';
		} else {
			response = $('<div>').append(jqXHR.responseText);
			message += response.text();
			highlightError(jqXHR.responseText);
			if(jqXHR.responseText.match(/Query deadline is expired/)) {
				message = "QUERY TIMEOUT\n"+message;
			}
		}
		$('#query-error').html($('<pre>').text(message)).show();
	}

	function showQueryResults(data) {
		var results, thead, i, tr, td, linkText, j, binding, title,
			table = $('<table>')
				.attr('class','table')
				.appendTo($('#query-result'));
		$('#query-error').hide();
		$('#query-result').show();

		if(typeof data.boolean != 'undefined') {
			// ASK query
			table.append('<tr><td>' + data.boolean + '</td></tr>').addClass('boolean');
			return;
		}

		results = data.results.bindings.length;
		$('#total-results').text(results);
		$('#query-time').text(Date.now() - QUERY_START);
		$('#total').show();
		$('#shorturl').attr("href", SHORTURL+encodeURIComponent(window.location));

		thead = $('<thead>').appendTo(table);
		tr = $('<tr>');
		for (i = 0; i < data.head.vars.length; i++) {
			tr.append('<th>' + data.head.vars[i] + '</th>');
		}
		thead.append(tr);
		table.append(thead);

		for (i = 0; i < results; i++) {
			tr = $('<tr>');
			for (j = 0; j < data.head.vars.length; j++) {
				td = $('<td>');
				if (data.head.vars[j] in data.results.bindings[i]) {
					binding = data.results.bindings[i][data.head.vars[j]];
					text = binding.value;
					if (binding.type == 'uri') {
						text = abbreviate(text);
					}
					linkText = $('<pre>').text(text.trim());
					if (binding.type == 'typed-literal') {
						td.attr({
							"class": "literal",
							"data-datatype": binding.datatype
						}).append(linkText);
					} else {
						td.attr('class', binding.type);
						if (binding.type == 'uri') {
							td.append($('<a>')
								.attr("href", binding.value)
								.append(linkText)
							);
							if(binding.value.match(/http:\/\/www.wikidata.org\/entity\//)) {
								td.append($('<a>')
									.attr("href", '#')
									.bind('click', exploreURL.bind(undefined, binding.value))
									.text('*')
								);
							}
						} else {
							td.append(linkText);
						}

						if (binding['xml:lang']) {
							td.attr({
								"data-lang": binding["xml:lang"],
								title: binding.value + '@' + binding["xml:lang"]
							});
						}
					}
				} else {
					// no binding
					td.attr("class", "unbound");
				}
				tr.append(td);
			}
			table.append(tr);
		}
	}

	function abbreviate(uri) {
		var nsGroup, ns;

		for ( nsGroup in NAMESPACE_SHORTCUTS) {
			for ( ns in NAMESPACE_SHORTCUTS[nsGroup]) {
				if (uri.indexOf(NAMESPACE_SHORTCUTS[nsGroup][ns]) === 0) {
					return uri.replace(NAMESPACE_SHORTCUTS[nsGroup][ns], ns + ':');
				}
			}
		}
		return '<' + uri + '>';
	}

	function addPrefixes() {
		var current = EDITOR.getValue();
		EDITOR.setValue(STANDARD_PREFIXES + current);
	}

	function populateNamespaceShortcuts() {
		var category, select, ns,
			container = $('.namespace-shortcuts');
		// add namespaces to dropdowns
		container.text('Namespace prefixes: ');
		for ( category in NAMESPACE_SHORTCUTS) {
			select = $('<select>')
				.attr('class', 'form-control')
				.append($('<option>').text(category))
				.appendTo(container);
			for ( ns in NAMESPACE_SHORTCUTS[category]) {
				select.append($('<option>').text(ns).attr({
					value: NAMESPACE_SHORTCUTS[category][ns]
				}));
			}
		}
	}

	function selectNamespace() {
		var ns,
			uri = this.value,
			current = EDITOR.getValue();

		if (current.indexOf(uri) == -1) {
			ns = $(this).find(':selected').text();
			EDITOR.setValue('prefix ' + ns + ': <' + uri + '>\n' + current);
		}

		// reselect group label
		this.selectedIndex = 0;
	}

	function showHideHelp(e) {
		e.preventDefault();
		$('#seealso').toggle();
		if($('#seealso').is(':visible')) {
			$('#showhide').text("hide");
		} else {
			$('#showhide').text("show");
		}
	}


	function initQuery() {
		if(window.location.hash !== "") {
			EDITOR.setValue(decodeURIComponent(window.location.hash.substr(1)));
			EDITOR.refresh();
		}
	}

	function setupEditor() {
		EDITOR = CodeMirror.fromTextArea($('#query')[0], CODEMIRROR_DEFAULTS);
		EDITOR.on('change', function() {
				if(ERROR_LINE_MARKER) {
						ERROR_LINE_MARKER.clear();
						ERROR_CHARACTER_MARKER.clear();
				}
		});
		EDITOR.addKeyMap({'Ctrl-Enter': submitQuery});
		EDITOR.focus();
	}

	function highlightError(description) {
		var line, character,
		    match = description.match(/line (\d+), column (\d+)/);
		if(match) {
			// highlight character at error position
			line = match[1] - 1;
			character = match[2] - 1;
			ERROR_LINE_MARKER = EDITOR.doc.markText({line: line, ch: 0}, {line: line}, {className: 'error-line'});
			ERROR_CHARACTER_MARKER = EDITOR.doc.markText({line: line, ch: character}, {line: line, ch: character + 1}, {className: 'error-character'});
		}
	}

	function exploreURL(url) {
		var id,
			match = url.match(/http:\/\/www.wikidata.org\/entity\/(.+)/);
		if(!match) {
			return;
		}
		$('#hide-explorer').show();
		$('#show-explorer').hide();
		id = match[1];
		mw.config = { get: function() {
			return id;
		}};
		$('html, body').animate({ scrollTop: $("#explore").offset().top }, 500);
		EXPLORER($, mw, $("#explore"));
	}

	function hideExlorer(e) {
		e.preventDefault();
		$('#explore').empty('');
		$('#hide-explorer').hide();
		$('#show-explorer').show();
	}

	function setupExamples() {
		var exampleQueries = document.getElementById('exampleQueries');
		exampleQueries.add(new Option('US presidents and wives',
			'PREFIX wd: <http://www.wikidata.org/entity/> \n' +
			'PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n' +
			'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n' +
			'PREFIX p: <http://www.wikidata.org/prop/>\n' +
			'PREFIX v: <http://www.wikidata.org/prop/statement/>\n' +
			'SELECT ?p ?w ?l ?wl WHERE {\n' +
			'   wd:Q30 p:P6/v:P6 ?p .\n' +
			'   ?p wdt:P26 ?w .\n' +
			'   OPTIONAL  {  \n' +
			'     ?p rdfs:label ?l FILTER (LANG(?l) = "en") . \n' +
			'   }\n' +
			'   OPTIONAL {\n' +
			'     ?w rdfs:label ?wl FILTER (LANG(?wl) = "en"). \n' +
			'   }\n' +
			' }'
		));
	}

	function pasteExample() {
		var text = this.value;
		this.selectedIndex = 0;
		if(!text || !text.trim()) {
			return;
		}
		EDITOR.setValue(text);
    }

	function setupHandlers() {
		$('#query-form').submit(submitQuery);
		$('.namespace-shortcuts').on('change', 'select', selectNamespace);
		$('.exampleQueries').on('change', pasteExample);
		$('#prefixes-button').click(addPrefixes);
		$('#showhide').click(showHideHelp);
		$('#hide-explorer').click(hideExlorer);
		$('#clear-button').click(function () { EDITOR.setValue("") });
	}

	function startGUI() {
		setupEditor();
		setupExamples();
		populateNamespaceShortcuts();
		setupHandlers();
		initQuery();
	}


	$(document).ready(function() {
		startGUI();
	});
	$(window).on('popstate', initQuery);
})(jQuery, mediaWiki);

