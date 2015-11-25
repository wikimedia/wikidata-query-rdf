var wikibase = wikibase || {};
wikibase.queryService = wikibase.queryService || {};
wikibase.queryService.api = wikibase.queryService.api || {};

wikibase.queryService.api.Sparql = (function($) {
	"use strict";

	var SERVICE = '/bigdata/namespace/wdq/sparql';

	/**
	 * SPARQL API for the Wikibase query service
	 *
	 * @class wikibase.queryService.api.Sparql
	 * @licence GNU GPL v2+
	 *
	 * @author Stanislav Malyshev
	 * @author Jonas Kress
	 * @constructor
	 */
	function SELF() {
	}

	/**
	 * @property {int}
	 * @private
	 **/
	SELF.prototype._executionTime = null;

	/**
	 * @property {string}
	 * @private
	 **/
	SELF.prototype._errorMessage = null;

	/**
	 * @property {int}
	 * @private
	 **/
	SELF.prototype._resultLength = null;

	/**
	 * @property {Object}
	 * @private
	 **/
	SELF.prototype._rawData = null;

	/**
	 * @property {string}
	 * @private
	 **/
	SELF.prototype._queryUri = null;


	/**
	 * Submit a query to the API
	 *
	 * @return {jQuery.Promise}
	 */
	SELF.prototype.queryDataUpdatedTime = function() {

		var deferred = $.Deferred(),
		query = encodeURI('prefix schema: <http://schema.org/> ' +
				'SELECT * WHERE {<http://www.wikidata.org> schema:dateModified ?y}'),
		url = SERVICE + '?query=' + query, settings = {
			'headers' : {
				'Accept' : 'application/sparql-results+json'
			},
		};

		$.ajax( url, settings ).done(function( data ){
			var updateDate = new Date( data.results.bindings[0][data.head.vars[0]].value ),
			dateText = updateDate.toLocaleTimeString(
				navigator.language,
				{ timeZoneName: 'short' }
			) + ', ' + updateDate.toLocaleDateString(
				navigator.language,
				{ month: 'short', day: 'numeric', year: 'numeric' }
			);

			deferred.resolve( dateText );
		}).fail(function(){
			deferred.reject();
		});

		return deferred;
	};

	/**
	 * Submit a query to the API
	 *
	 * @param {string[]}
	 *            query
	 * @return {jQuery.Promise}
	 */
	SELF.prototype.query = function(query) {
		var deferred = $.Deferred(), self = this, settings = {
			'headers' : {
				'Accept' : 'application/sparql-results+json'
			},
		};

		this._queryUri = SERVICE + '?' + query;

		this._executionTime = Date.now();
		$.ajax( this._queryUri, settings ).done(function( data, textStatus, jqXHR ) {
			self._executionTime = Date.now() - self._executionTime;
			self._resultLength = data.results.bindings.length || 0;
			self._rawData = data;

			deferred.resolve();
		}).fail(function( jqXHR, textStatus, errorThrown ) {
			self._executionTime = null;
			self._rawData = null;
			self._resultLength = null;
			self._generateErrorMessage(jqXHR);

			deferred.reject();
		});

		return deferred;
	};

	/**
	 * Get execution time in ms of the submitted query
	 *
	 * @return {int}
	 */
	SELF.prototype._generateErrorMessage = function( jqXHR ) {
		var message = 'ERROR: ';

		if ( jqXHR.status === 0 ) {
			message += 'Could not contact server';
		} else {
			message += jqXHR.responseText;
			if ( jqXHR.responseText.match( /Query deadline is expired/ ) ) {
				message = 'QUERY TIMEOUT\n' + message;
			}
		}
		this._errorMessage = message;
	};

	/**
	 * Produce abbreviation of the URI.
	 *
	 * @param {string} uri
	 * @returns {string}
	 */
	SELF.prototype.abbreviateUri = function( uri ) {
		var nsGroup, ns, NAMESPACE_SHORTCUTS = wikibase.queryService.RdfNamespaces.NAMESPACE_SHORTCUTS;

		for ( nsGroup in NAMESPACE_SHORTCUTS ) {
			for ( ns in NAMESPACE_SHORTCUTS[nsGroup] ) {
				if ( uri.indexOf( NAMESPACE_SHORTCUTS[nsGroup][ns] ) === 0 ) {
					return uri.replace( NAMESPACE_SHORTCUTS[nsGroup][ns], ns + ':' );
				}
			}
		}
		return '<' + uri + '>';
	};

	/**
	 * Get execution time in seconds of the submitted query
	 *
	 * @return {int}
	 */
	SELF.prototype.getExecutionTime = function() {
		return this._executionTime;
	};

	/**
	 * Get error message of the submitted query if it has failed
	 *
	 * @return {int}
	 */
	SELF.prototype.getErrorMessage = function() {
		return this._errorMessage;
	};

	/**
	 * Get result length of the submitted query if it has failed
	 *
	 * @return {int}
	 */
	SELF.prototype.getResultLength = function() {
		return this._resultLength;
	};

	/**
	 * Get query URI
	 *
	 * @return {string}
	 */
	SELF.prototype.getQueryUri = function() {
		return this._queryUri;
	};

	/**
	 * Get the result as table
	 *
	 * @return {jQuery}
	 */
	SELF.prototype.getResultAsTable = function() {
		var data = this._rawData, thead, tr, td, binding,
		table = $( '<table>' ).attr( 'class', 'table' );

		if ( typeof data.boolean !== 'undefined' ) {
			// ASK query
			table.append( '<tr><td>' + data.boolean + '</td></tr>' ).addClass( 'boolean' );
			return;
		}

		thead = $( '<thead>' ).appendTo( table );
		tr = $( '<tr>' );
		for ( i = 0; i < data.head.vars.length; i++ ) {
			tr.append( '<th>' + data.head.vars[i] + '</th>' );
		}
		thead.append( tr );
		table.append( thead );

		for (var i = 0; i < this._resultLength; i++ ) {
			tr = $( '<tr>' ) ;
			for ( var j = 0; j < data.head.vars.length; j++ ) {
				td = $( '<td>' ) ;
				if ( data.head.vars[j] in data.results.bindings[i] ) {
					binding = data.results.bindings[i][data.head.vars[j]];
					var text = binding.value;
					if ( binding.type === 'uri' ) {
						text = this.abbreviateUri( text );
					}
					var linkText = $( '<pre>' ).text( text.trim() );
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

		return table;
	};

	/**
	 * Process SPARQL query result.
	 *
	 * @param {Object} data
	 * @param {Function} rowHandler
	 * @param {*} context
	 * @private
	 * @return {*} The provided context, modified by the rowHandler.
	 */
	SELF.prototype._processData = function( data, rowHandler, context ) {
		var results = data.results.bindings.length;
		for ( var i = 0; i < results; i++ ) {
			var rowBindings = {};
			for ( var j = 0; j < data.head.vars.length; j++ ) {
				if ( data.head.vars[j] in data.results.bindings[i] ) {
					rowBindings[data.head.vars[j]] = data.results.bindings[i][data.head.vars[j]];
				}
			}
			context = rowHandler( rowBindings, context );
		}
		return context;
	};

	/**
	 * Encode string as CSV.
	 *
	 * @param {string} string
	 * @return {string}
	 */
	SELF.prototype._encodeCsv = function( string ) {
		var result = string.replace( /"/g, '""' );
		if ( result.search( /("|,|\n)/g ) >= 0 ) {
			result = '"' + result + '"';
		}
		return result;
	};

	/**
	 * Get the result of the submitted query as CSV
	 *
	 * @return {string} csv
	 */
	SELF.prototype.getResultAsCsv = function() {
		var self = this, data = self._rawData;
		var out = data.head.vars.map( this._encodeCsv ).join( ',' ) + '\n';
		out = this._processData( data, function ( row, out ) {
			var rowOut = '';
			for ( var rowVar in row ) {
				var rowCSV = self._encodeCsv( row[rowVar].value );
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
	};

	/**
	 * Get the result of the submitted query as JSON
	 *
	 * @return {string}
	 */
	SELF.prototype.getResultAsJson = function() {
		var out = [], data = this._rawData;
		out = this._processData( data, function ( row, out ) {
			var extractRow = {};
			for ( var rowVar in row ) {
				extractRow[rowVar] = row[rowVar].value;
			}
			out.push( extractRow );
			return out;
		}, out );
		return JSON.stringify( out );
	};

	/**
	 * Get the result of the submitted query as JSON
	 *
	 * @return {string}
	 */
	SELF.prototype.getResultAsAllJson = function() {
		return JSON.stringify( this._rawData );
	};

	/**
	 * Render value as per http://www.w3.org/TR/sparql11-results-csv-tsv/#tsv
	 *
	 * @param {Object} binding
	 * @return {string}
	 */
	SELF.prototype._renderValueTSV = function( binding ) {
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
	};

	/**
	 * Get the result of the submitted query as TSV
	 *
	 * @return {string}
	 */
	SELF.prototype.getSparqlTsv = function() {
		var data = this._rawData, self = this,
			out = data.head.vars.map( function ( vname ) {
			return '?' + vname;
		} ).join( '\t' ) + '\n';
		out = this._processData( data, function ( row, out ) {
			var rowOut = '';
			for ( var rowVar in row ) {
				var rowTSV = self._renderValueTSV( row[rowVar] );
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
	};

	/**
	 * Get the result of the submitted query as TSV
	 *
	 * @return {string}
	 */
	SELF.prototype.getSimpleTsv = function() {
		var data = this._rawData,
			out = data.head.vars.join( '\t' ) + '\n';
		out = this._processData( data, function ( row, out ) {
			var rowOut = '';
			for ( var rowVar in row ) {
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
	};

	return SELF;

}(jQuery));
