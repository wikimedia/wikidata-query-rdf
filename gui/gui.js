var jQuery = $;
var mediaWiki = {};

$(function() {
	var SERVICE = '/bigdata/namespace/wdq/sparql';

	var SHORTURL = 'http://tinyurl.com/create.php?url=';

	var NAMESPACE_SHORTCUTS = {
		'Wikidata' : {
			'wikibase' : 'http://wikiba.se/ontology#',
			'wdata' : 'http://wikidata.org/wiki/Special:EntityData/',
			'wd' : 'http://wikidata.org/entity/',
			'wdt' : 'http://wikidata.org/prop/direct/',
			'wds' : 'http://wikidata.org/entity/statement/',
			'p' : 'http://wikidata.org/prop/',
			'wdref' : 'http://wikidata.org/reference/',
			'wdv' : 'http://wikidata.org/value/',
			'ps' : 'http://wikidata.org/prop/statement/',
			'psv' : 'http://wikidata.org/prop/statement/value/',
			'pq' : 'http://wikidata.org/prop/qualifier/',
			'pqv' : 'http://wikidata.org/prop/qualifier/value/',
			'pr' : 'http://wikidata.org/prop/reference/',
			'prv' : 'http://wikidata.org/prop/reference/value/',
			'wdno' : 'http://wikidata.org/prop/novalue/'
		},
		'W3C' : {
			'rdf' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
			'rdfs' : 'http://www.w3.org/2000/01/rdf-schema#',
			'owl' : 'http://www.w3.org/2002/07/owl#',
			'skos' : 'http://www.w3.org/2004/02/skos/core#',
			'xsd' : 'http://www.w3.org/2001/XMLSchema#'
		},
		'Social/Other' : {
			'schema' : 'http://schema.org/',
		},
		'Blazegraph' : {
			'bd' : 'http://www.bigdata.com/rdf#',
			'bds' : 'http://www.bigdata.com/rdf/search#',
			'gas' : 'http://www.bigdata.com/rdf/gas#',
			'hint' : 'http://www.bigdata.com/queryHints#'
		}
	};

	var STANDARD_PREFIXES = 'PREFIX wd: <http://www.wikidata.org/entity/>\n\
PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n\
PREFIX wikibase: <http://wikiba.se/ontology#>\n\
PREFIX p: <http://www.wikidata.org/prop/>\n\
PREFIX v: <http://www.wikidata.org/prop/statement/>\n\
PREFIX q: <http://www.wikidata.org/prop/qualifier/>\n\
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\
';

	var QUERY_START = 0;

	var CODEMIRROR_DEFAULTS = {
		lineNumbers : true,
		matchBrackets : true,
		mode : "sparql",
	};

	var EDITOR = {};
	var ERROR_LINE_MARKER = null;
	var ERROR_CHARACTER_MARKER = null;

	function submitQuery(e) {
		e.preventDefault();
		EDITOR.save();
		var query = $('#query-form').serialize();
		var url = SERVICE + "?" + query;
		var settings = {
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
		window.location.hash = encodeURIComponent(EDITOR.getValue());
		QUERY_START = Date.now();
		$.ajax(url, settings);
	}

	function queryResultsError(jqXHR, textStatus, errorThrown) {
		$('#query-error').show();
		var message = 'ERROR: ';
		if (jqXHR.status === 0) {
			message += 'Could not contact server';
		} else {
			var response = $('<div>').append(jqXHR.responseText);
			message += response.text();
			highlightError(jqXHR.responseText);
		}
		$('#query-error').text(message);
	}

	function showQueryResults(data) {
		$('#query-error').hide();
		$('#query-result').show();

		var table = $('<table>').appendTo($('#query-result'));

		if(typeof data.boolean != 'undefined') {
			// ASK query
	        table.append('<tr><td>' + data.boolean + '</td></tr>').addClass('boolean');
	        return;
		}

		var results = data.results.bindings.length;
		$('#total-results').html(results);
		$('#query-time').html(Date.now() - QUERY_START);
		$('#total').show();
		$('#shorturl').attr("href", SHORTURL+encodeURIComponent(window.location));

	    var thead = $('<thead>').appendTo(table);
		tr = $('<tr>');
		for (i = 0; i < data.head.vars.length; i++) {
			tr.append('<th>' + data.head.vars[i] + '</th>');
		}
		thead.append(tr);
		table.append(thead);

		for (var i = 0; i < results; i++) {
			var tr = $('<tr>');
			for (var j = 0; j < data.head.vars.length; j++) {
				if (data.head.vars[j] in data.results.bindings[i]) {
					var binding = data.results.bindings[i][data.head.vars[j]];
					text = binding.value;
					if (binding.type == 'uri') {
						text = abbreviate(text);
					}
					linkText = escapeHTML(text).replace(/\n/g, '<br>');
					if (binding.type == 'typed-literal') {
						tdData = ' class="literal" data-datatype="'
								+ binding.datatype + '"';
					} else {
						if (binding.type == 'uri') {
							text = '<a href="' + binding.value + '">'
									+ linkText + '</a>';
							if(binding.value.match(/http:\/\/www.wikidata.org\/entity\//)) {
								text += '<a href="javascript:exploreURL(\'' + binding.value + '\')">*</a>';
							}
						}
						tdData = ' class="' + binding.type + '"';
						if (binding['xml:lang']) {
							var title = text + "@" + binding['xml:lang'];
							tdData += ' data-lang="' + binding['xml:lang']
									+ '" title="' + title + '"';
						}
					}
					tr.append('<td' + tdData + '>' + text + '</td>');
				} else {
					// no binding
					tr.append('<td class="unbound">');
				}
			}
			table.append(tr);
		}
	}

	function escapeHTML(text) {
		return $('<div/>').text(text).html();
	}

	function abbreviate(uri) {
		for ( var nsGroup in NAMESPACE_SHORTCUTS) {
			for ( var ns in NAMESPACE_SHORTCUTS[nsGroup]) {
				if (uri.indexOf(NAMESPACE_SHORTCUTS[nsGroup][ns]) === 0) {
					return uri.replace(NAMESPACE_SHORTCUTS[nsGroup][ns], ns
							+ ':');
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
		// add namespaces to dropdowns
		$('.namespace-shortcuts').html('Namespace prefixes: ');
		for ( var category in NAMESPACE_SHORTCUTS) {
			var select = $('<select><option>' + category + '</option></select>')
					.appendTo($('.namespace-shortcuts'));
			for ( var ns in NAMESPACE_SHORTCUTS[category]) {
				select.append('<option value="'
						+ NAMESPACE_SHORTCUTS[category][ns] + '">' + ns
						+ '</option>');
			}
		}
	}

	function selectNamespace() {
		var uri = this.value;
		var current = EDITOR.getValue();

		if (current.indexOf(uri) == -1) {
			var ns = $(this).find(':selected').text();
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
		if(window.location.hash != "") {
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
		var match = description.match(/line (\d+), column (\d+)/);
		if(match) {
			// highlight character at error position
			var line = match[1] - 1;
			var character = match[2] - 1;
			ERROR_LINE_MARKER = EDITOR.doc.markText({line: line, ch: 0}, {line: line}, {className: 'error-line'});
			ERROR_CHARACTER_MARKER = EDITOR.doc.markText({line: line, ch: character}, {line: line, ch: character + 1}, {className: 'error-character'});
		}
	}

	window.exploreURL = function(url) {
		var match = url.match(/http:\/\/www.wikidata.org\/entity\/(.+)/)
		if(!match) {
			return;
		}
		$('#hide-explorer').show();
		$('#show-explorer').hide();
		var id = match[1];
		mediaWiki.config = { get: function() {
			return id;
		}};
		$('html, body').animate({ scrollTop: $("#explore").offset().top }, 500);
		EXPLORER($, mediaWiki, $("#explore"));
	}

	function hideExlorer(e) {
		e.preventDefault();
		$('#explore').empty('');
		$('#hide-explorer').hide();
		$('#show-explorer').show();
	}

	function setupHandlers() {
		$('#query-form').submit(submitQuery);
		$('.namespace-shortcuts').on('change', 'select', selectNamespace);
		$('#prefixes-button').click(addPrefixes);
		$('#showhide').click(showHideHelp);
		$('#hide-explorer').click(hideExlorer);

	}

	function startGUI() {
		setupHandlers();
		setupEditor();
		populateNamespaceShortcuts();
		initQuery();
	}


	startGUI();
});

