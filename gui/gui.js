window.mediaWiki = window.mediaWiki || {};
window.EDITOR = {};

( function ( $, mw ) {
	var SERVICE = '/bigdata/namespace/wdq/sparql',
		SHORTURL = 'http://tinyurl.com/create.php?url=',
		EXPLORE_URL = 'http://www.wikidata.org/entity/',
		NAMESPACE_SHORTCUTS = {
			'Wikidata': {
				'wikibase': 'http://wikiba.se/ontology#',
				'wd': 'http://www.wikidata.org/entity/',
				'wdt': 'http://www.wikidata.org/prop/direct/',
				'wds': 'http://www.wikidata.org/entity/statement/',
				'p': 'http://www.wikidata.org/prop/',
				'wdref': 'http://www.wikidata.org/reference/',
				'wdv': 'http://www.wikidata.org/value/',
				'ps': 'http://www.wikidata.org/prop/statement/',
				'psv': 'http://www.wikidata.org/prop/statement/value/',
				'pq': 'http://www.wikidata.org/prop/qualifier/',
				'pqv': 'http://www.wikidata.org/prop/qualifier/value/',
				'pr': 'http://www.wikidata.org/prop/reference/',
				'prv': 'http://www.wikidata.org/prop/reference/value/',
				'wdno': 'http://www.wikidata.org/prop/novalue/',
				'wdata': 'http://www.wikidata.org/wiki/Special:EntityData/'
			},
			'W3C': {
				'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
				'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
				'owl': 'http://www.w3.org/2002/07/owl#',
				'skos': 'http://www.w3.org/2004/02/skos/core#',
				'xsd': 'http://www.w3.org/2001/XMLSchema#',
				'prov': 'http://www.w3.org/ns/prov#'
			},
			'Social/Other': {
				'schema': 'http://schema.org/'
			},
			'Blazegraph': {
				'bd': 'http://www.bigdata.com/rdf#',
				'bds': 'http://www.bigdata.com/rdf/search#',
				'gas': 'http://www.bigdata.com/rdf/gas#',
				'hint': 'http://www.bigdata.com/queryHints#'
			}
		},
		STANDARD_PREFIXES =[
			'PREFIX wd: <http://www.wikidata.org/entity/>',
			'PREFIX wdt: <http://www.wikidata.org/prop/direct/>',
			'PREFIX wikibase: <http://wikiba.se/ontology#>',
			'PREFIX p: <http://www.wikidata.org/prop/>',
			'PREFIX v: <http://www.wikidata.org/prop/statement/>',
			'PREFIX q: <http://www.wikidata.org/prop/qualifier/>',
			'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>'
		].join( '\n' ),
		QUERY_START = 0,
		CODEMIRROR_DEFAULTS = {
			lineNumbers: true,
			matchBrackets: true,
			mode: 'sparql',
			extraKeys: { 'Ctrl-Space': 'autocomplete' }
		},
		ERROR_LINE_MARKER = null,
		ERROR_CHARACTER_MARKER = null,
		LAST_RESULT = null,
		DOWNLOAD_FORMATS = {
			'CSV': {
				handler: getCSVData,
				mimetype: 'text/csv;charset=utf-8'
			},
			'JSON': {
				handler: getJSONData,
				mimetype: 'application/json;charset=utf-8'
			},
			'TSV': {
				handler: getSparqlTSVData,
				mimetype: 'text/tab-separated-values;charset=utf-8'
			},
			'Simple TSV': {
				handler: getSimpleTSVData,
				mimetype: 'text/tab-separated-values;charset=utf-8',
				ext: 'tsv'
			},
			'Full JSON': {
				handler: getAllJSONData,
				mimetype: 'application/json;charset=utf-8',
				ext: 'json'
			}
		};

	/**
	 * Submit SPARQL query.
	 */
	function submitQuery( e ) {
		e.preventDefault();
		EDITOR.save();

		LAST_RESULT = null;

		var query = $( '#query-form' ).serialize(),
			hash = encodeURIComponent( EDITOR.getValue() ),
			url = SERVICE + '?' + query,
			settings = {
				headers: {
					'Accept': 'application/sparql-results+json'
				},
				success: showQueryResults,
				error: queryResultsError
			};
		$( '#query-result' ).empty( '' );
		$( '#query-result' ).hide();
		$( '#total' ).hide();
		$( '#query-error' ).show();
		$( '#query-error' ).text( 'Running query...' );
		if ( window.location.hash !== hash ) {
			window.location.hash = hash;
		}
		QUERY_START = Date.now();
		$.ajax( url, settings );
	}

	/**
	 * Handle SPARQL error.
	 */
	function queryResultsError( jqXHR, textStatus, errorThrown ) {
		var response,
			message = 'ERROR: ';

		if ( jqXHR.status === 0 ) {
			message += 'Could not contact server';
		} else {
			response = $( '<div>' ).append( jqXHR.responseText );
			message += response.text();
			highlightError( jqXHR.responseText );
			if ( jqXHR.responseText.match( /Query deadline is expired/ ) ) {
				message = 'QUERY TIMEOUT\n' + message;
			}
		}
		$( '#query-error' ).html( $( '<pre>' ).text( message ) ).show();
	}

	/**
	 * Show results of the query.
	 */
	function showQueryResults( data ) {
		var results, thead, i, tr, td, linkText, j, binding,
			table = $( '<table>' )
				.attr( 'class', 'table' )
				.appendTo( $( '#query-result' ) );
		$( '#query-error' ).hide();
		$( '#query-result' ).show();

		if ( typeof data.boolean !== 'undefined' ) {
			// ASK query
			table.append( '<tr><td>' + data.boolean + '</td></tr>' ).addClass( 'boolean' );
			return;
		}

		LAST_RESULT = data;
		results = data.results.bindings.length;
		$( '#total-results' ).text( results );
		$( '#query-time' ).text( Date.now() - QUERY_START );
		$( '#total' ).show();
		$( '#shorturl' ).attr( 'href', SHORTURL + encodeURIComponent( window.location ) );

		thead = $( '<thead>' ).appendTo( table );
		tr = $( '<tr>' );
		for ( i = 0; i < data.head.vars.length; i++ ) {
			tr.append( '<th>' + data.head.vars[i] + '</th>' );
		}
		thead.append( tr );
		table.append( thead );

		for ( i = 0; i < results; i++ ) {
			tr = $( '<tr>' ) ;
			for ( j = 0; j < data.head.vars.length; j++ ) {
				td = $( '<td>' ) ;
				if ( data.head.vars[j] in data.results.bindings[i] ) {
					binding = data.results.bindings[i][data.head.vars[j]];
					text = binding.value;
					if ( binding.type === 'uri' ) {
						text = abbreviate( text );
					}
					linkText = $( '<pre>' ).text( text.trim() );
					if ( binding.type === 'typed-literal' ) {
						td.attr( {
							'class': 'literal',
							'data-datatype': binding.datatype
						} ).append( linkText );
					} else {
						td.attr( 'class', binding.type );
						if ( binding.type === 'uri' ) {
							td.append( $( '<a>' )
								.attr( 'href', binding.value )
								.append( linkText )
							);
							if ( binding.value.match( EXPLORE_URL ) ) {
								td.append( $( '<a>' )
									.attr( 'href', '#' )
									.bind( 'click', exploreURL.bind( undefined, binding.value ) )
									.text( '*' )
								);
							}
						} else {
							td.append( linkText );
						}

						if ( binding['xml:lang'] ) {
							td.attr( {
								'data-lang': binding['xml:lang'],
								title: binding.value + '@' + binding['xml:lang']
							} );
						}
					}
				} else {
					// no binding
					td.attr( 'class', 'unbound' );
				}
				tr.append( td );
			}
			table.append( tr );
		}
	}

	/**
	 * Produce abbreviation of the URI.
	 */
	function abbreviate( uri ) {
		var nsGroup, ns;

		for ( nsGroup in NAMESPACE_SHORTCUTS ) {
			for ( ns in NAMESPACE_SHORTCUTS[nsGroup] ) {
				if ( uri.indexOf( NAMESPACE_SHORTCUTS[nsGroup][ns] ) === 0 ) {
					return uri.replace( NAMESPACE_SHORTCUTS[nsGroup][ns], ns + ':' );
				}
			}
		}
		return '<' + uri + '>';
	}

	/**
	 * Add standard prefixes to editor window.
	 */
	function addPrefixes() {
		var current = EDITOR.getValue();
		EDITOR.setValue( STANDARD_PREFIXES + '\n\n' + current );
	}

	/**
	 * Populate namespace shortcut selector.
	 */
	function populateNamespaceShortcuts() {
		var category, select, ns,
			container = $( '.namespace-shortcuts' );

		container.click( function ( e ) {
			e.stopPropagation();
		} );

		// add namespaces to dropdowns
		for ( category in NAMESPACE_SHORTCUTS ) {
			select = $( '<select>' )
				.attr( 'class', 'form-control' )
				.append( $( '<option>' ).text( category ) )
				.appendTo( container );
			for ( ns in NAMESPACE_SHORTCUTS[category] ) {
				select.append( $( '<option>' ).text( ns ).attr( {
					value: NAMESPACE_SHORTCUTS[category][ns]
				} ) );
			}
		}
	}

	/**
	 * Add selected namespace's prefix to editor.
	 */
	function selectNamespace() {
		var ns,
			uri = this.value,
			current = EDITOR.getValue();

		if ( current.indexOf( uri ) === -1 ) {
			ns = $( this ).find( ':selected' ).text();
			EDITOR.setValue( 'prefix ' + ns + ': <' + uri + '>\n' + current );
		}

		// reselect group label
		this.selectedIndex = 0;
	}

	/**
	 * Show/hide help text.
	 */
	function showHideHelp( e ) {
		var $seeAlso = $( '#seealso' );

		e.preventDefault();
		$seeAlso.toggle();
		if ( $seeAlso.is( ':visible' ) ) {
			$( '#showhide' ).text( 'hide' );
		} else {
			$( '#showhide' ).text( 'show' );
		}
	}

	/**
	 * Initialize query editor window.
	 */
	function initQuery() {
		if ( window.location.hash !== '' ) {
			EDITOR.setValue( decodeURIComponent( window.location.hash.substr( 1 ) ) );
			EDITOR.refresh();
		}
	}

	/**
	 * Setup editor window.
	 */
	function setupEditor() {
		EDITOR = CodeMirror.fromTextArea( $( '#query' )[0], CODEMIRROR_DEFAULTS );
		EDITOR.on( 'change', function () {
			if ( ERROR_LINE_MARKER ) {
				ERROR_LINE_MARKER.clear();
				ERROR_CHARACTER_MARKER.clear();
			}
		} );
		EDITOR.addKeyMap( { 'Ctrl-Enter': submitQuery } );
		EDITOR.focus();

		new WikibaseRDFTooltip(EDITOR);
	}

	/**
	 * Highlight SPARQL error in editor window.
	 */
	function highlightError( description ) {
		var line, character,
			match = description.match( /line (\d+), column (\d+)/ );
		if ( match ) {
			// highlight character at error position
			line = match[1] - 1;
			character = match[2] - 1;
			ERROR_LINE_MARKER = EDITOR.doc.markText(
				{ line: line, ch: 0 },
				{ line: line },
				{ className: 'error-line' }
			);
			ERROR_CHARACTER_MARKER = EDITOR.doc.markText(
				{ line: line, ch: character },
				{ line: line, ch: character + 1 },
				{ className: 'error-character' }
			);
		}
	}

	/**
	 * Show explorer window for given URL.
	 */
	function exploreURL( url ) {
		var id,
			match = url.match( EXPLORE_URL + '(.+)' );
		if ( !match ) {
			return;
		}
		if ( $( '#hide-explorer' ).is( ':visible' ) ) {
			$( '#explore' ).empty( '' );
		} else {
			$( '#hide-explorer' ).show();
			$( '#show-explorer' ).hide();
		}
		id = match[1];
		mw.config = { get: function () {
			return id;
		} };
		$( 'html, body' ).animate( { scrollTop: $( '#explore' ).offset().top }, 500 );
		EXPLORER( $, mw, $( '#explore' ) );
	}

	/**
	 * Hide explorer window.
	 */
	function hideExlorer( e ) {
		e.preventDefault();
		$( '#explore' ).empty( '' );
		$( '#hide-explorer' ).hide();
		$( '#show-explorer' ).show();
	}

	/**
	 * Setup query examples.
	 */
	function setupExamples() {
		var exampleQueries = document.getElementById( 'exampleQueries' );

		$.ajax( {
			url: 'https://www.mediawiki.org/w/api.php?action=query&prop=revisions&titles=Wikibase/'
				+ 'Indexing/SPARQL_Query_Examples&rvprop=content',
			data: {
				format: 'json'
			},
			dataType: 'jsonp'
		} ).done( function ( data ) {
			var wikitext = data.query.pages[Object.keys( data.query.pages )].revisions[0]['*'];
			var paragraphs = wikitext.split( '==' );

			$.each( paragraphs, function ( key, paragraph ) {
				if ( paragraph.match( /SPARQL\|.*query\=/ ) ) {
					var query = paragraph.substring(
						paragraph.indexOf( '|query=' ) + 7,
						paragraph.lastIndexOf( '}}' )
					).trim();
					var title = paragraphs[key - 1] || '';
					title = title.replace( '=', '' ).trim();

					if ( paragraph.match( 'extraprefix=' ) ) {
						var prefix = paragraph.substring( paragraph.indexOf( '|extraprefix=' ) + 13, paragraph.indexOf( '|query=' ) ).trim();
						query = prefix + '\n\n' + query;
					}

					if ( title ) {
						exampleQueries.add( new Option( title, query ) );
					}
				}
			} )
		} );
	}

	/**
	 * Add query example to editor window.
	 */
	function pasteExample() {
		var text = this.value;
		this.selectedIndex = 0;
		if ( !text || !text.trim() ) {
			return;
		}
		EDITOR.setValue( text );
		addPrefixes();
	}

	/**
	 * Setup event handlers.
	 */
	function setupHandlers() {
		$( '#query-form' ).submit( submitQuery );
		$( '.namespace-shortcuts' ).on( 'change', 'select', selectNamespace );
		$( '.exampleQueries' ).on( 'change', pasteExample );
		$( '.addPrefixes' ).click( addPrefixes );
		$( '#showhide' ).click( showHideHelp );
		$( '#hide-explorer' ).click( hideExlorer );
		$( '#clear-button' ).click( function () {
			EDITOR.setValue( '' );
		} );
		for ( format in DOWNLOAD_FORMATS ) {
			var extension = DOWNLOAD_FORMATS[format].ext || format.toLowerCase();
			var formatName = format.replace( /\s/g, '-' );
			$( '#download' + formatName ).click( downloadHandler( 'query.' + extension,
				DOWNLOAD_FORMATS[format].handler, DOWNLOAD_FORMATS[format].mimetype
			) );
		}
	}

	/**
	 * Create download handler function.
	 */
	function downloadHandler( filename, handler, mimetype ) {
		return function ( e ) {
			e.preventDefault();
			if ( !LAST_RESULT ) {
				return '';
			}
			download( filename, handler( LAST_RESULT ), mimetype );
		}
	}

	/**
	 * Fetch last DB update time.
	 */
	function getDbUpdated() {
		var query = encodeURI( 'prefix schema: <http://schema.org/> '
			+ 'SELECT * WHERE {<http://www.wikidata.org> schema:dateModified ?y}' );
		var url = SERVICE + '?query=' + query,
			settings = {
				headers: {
					'Accept': 'application/sparql-results+json'
				},
				success: showDbQueryResults,
				error: DbQueryResultsError
			};
		$.ajax( url, settings );
	}

	/**
	 * Show results for last DB update time.
	 */
	function showDbQueryResults( data ) {
		try {
			var updateDate = new Date( data.results.bindings[0][data.head.vars[0]].value );
			$( '#dbUpdated' ).text( updateDate.toLocaleTimeString(
				navigator.language,
				{ timeZoneName: 'short' }
			) + ', ' + updateDate.toLocaleDateString(
				navigator.language,
				{ month: 'short', day: 'numeric', year: 'numeric' }
			) );
		} catch ( err ) {
			$( '#dbUpdated' ).text( '[unable to connect]' );
		}
	}

	/**
	 * Show error for last DB update time.
	 */
	function DbQueryResultsError( jqXHR, textStatus, errorThrown ) {
		$( '#dbUpdated' ).text( '[unable to connect]' );
	}

	/**
	 * Initialize GUI
	 */
	function startGUI() {
		setupEditor();
		setupExamples();
		populateNamespaceShortcuts();
		setupHandlers();
		initQuery();
		getDbUpdated();
	}

	/**
	 * Process SPARQL query result.
	 */
	function processData( data, rowHandler, context ) {
		results = data.results.bindings.length;
		for ( i = 0; i < results; i++ ) {
			rowBindings = {};
			for ( j = 0; j < data.head.vars.length; j++ ) {
				if ( data.head.vars[j] in data.results.bindings[i] ) {
					rowBindings[data.head.vars[j]] = data.results.bindings[i][data.head.vars[j]];
				}
			}
			context = rowHandler( rowBindings, context );
		}
		return context;
	}

	/**
	 * Encode string as CSV.
	 */
	function encodeCSV( string ) {
		var result = string.replace( /"/g, '""' );
		if ( result.search( /("|,|\n)/g ) >= 0 ) {
			result = '"' + result + '"';
		}
		return result;
	}

	/**
	 * Get CSV rendering of the result data.
	 */
	function getCSVData( data ) {
		out = data.head.vars.map( encodeCSV ).join( ',' ) + '\n';
		out = processData( data, function ( row, out ) {
			rowOut = '';
			for ( rowVar in row ) {
				var rowCSV = encodeCSV( row[rowVar].value );
				if ( rowOut.length > 0 ) {
					rowOut += ',';
				}
				rowOut += rowCSV;
			}
			if ( rowOut.length > 0 ) {
				rowOut += '\n';
			}
			return out + rowOut;
		}, out );
		return out;
	}

	/**
	 * Get TSV rendering of the result data.
	 */
	function getSimpleTSVData( data ) {
		out = data.head.vars.join( '\t' ) + '\n';
		out = processData( data, function ( row, out ) {
			rowOut = '';
			for ( rowVar in row ) {
				var rowTSV = row[rowVar].value.replace( /\t/g, '' );
				if ( rowOut.length > 0 ) {
					rowOut += '\t';
				}
				rowOut += rowTSV;
			}
			if ( rowOut.length > 0 ) {
				rowOut += '\n';
			}
			return out + rowOut;
		}, out );
		return out;
	}

	/**
	 * Render value as per http://www.w3.org/TR/sparql11-results-csv-tsv/#tsv
	 */
	function renderValueTSV( binding ) {
		var value = binding.value.replace( /\t/g, '' );
		switch ( binding.type ) {
			case 'uri':
				return '<' + value + '>';
			case 'bnode':
				return '_:' + value;
			case 'literal':
				var lvalue = JSON.stringify( value );
				if ( binding['xml:lang'] ) {
					return lvalue + '@' + binding['xml:lang'];
				}
				if ( binding.datatype ) {
					if ( binding.datatype === 'http://www.w3.org/2001/XMLSchema#integer' ||
						binding.datatype === 'http://www.w3.org/2001/XMLSchema#decimal' ||
						binding.datatype === 'http://www.w3.org/2001/XMLSchema#double'
					) {
						return value;
					}
					return lvalue + '^^<' + binding.datatype + '>';
				}
				return lvalue;
		}
		return value;
	}

	/**
	 * Get TSV rendering of the result data according to SPARQL standard.
	 * See: http://www.w3.org/TR/sparql11-results-csv-tsv/#tsv
	 */
	function getSparqlTSVData( data ) {
		out = data.head.vars.map( function ( vname ) {
			return '?' + vname;
		} ).join( '\t' ) + '\n';
		out = processData( data, function ( row, out ) {
			rowOut = '';
			for ( rowVar in row ) {
				var rowTSV = renderValueTSV( row[rowVar] );
				if ( rowOut.length > 0 ) {
					rowOut += '\t';
				}
				rowOut += rowTSV;
			}
			if ( rowOut.length > 0 ) {
				rowOut += '\n';
			}
			return out + rowOut;
		}, out );
		return out;
	}
	/**
	 * Get JSON rendering of the result data.
	 */
	function getJSONData( data ) {
		out = [];
		out = processData( data, function ( row, out ) {
			extractRow = {};
			for ( rowVar in row ) {
				extractRow[rowVar] = row[rowVar].value;
			}
			out.push( extractRow );
			return out;
		}, out );
		return JSON.stringify( out );
	}

	function getAllJSONData( data ) {
		return JSON.stringify( data );
	}

	/**
	 * Produce file download.
	 */
	function download( filename, text, contentType ) {
		if ( !text ) {
			return;
		}
		var element = document.createElement( 'a' );
		element.setAttribute( 'href', 'data:' + contentType + ',' + encodeURIComponent( text ) );
		element.setAttribute( 'download', filename );

		element.style.display = 'none';
		document.body.appendChild( element );
		element.click();
		document.body.removeChild( element );
	}

	$( document ).ready( function () {
		startGUI();
	} );
	$( window ).on( 'popstate', initQuery );
} )( jQuery, mediaWiki );
