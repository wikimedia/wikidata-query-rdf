/**
 * Code completion for Wikibase entities RDF prefixes in SPARQL
 *
 * Determines entity type from list of prefixes and completes input
 * based on search results from wikidata.org entities search API
 *
 * licence GNU GPL v2+
 *
 * @author Jens Ohlig <jens.ohlig@wikimedia.de>
 * @author Jan Zerebecki
 * @author Jonas Kress
 */

( function ( mod ) {
	if ( typeof exports == 'object' && typeof module == 'object' ) {// CommonJS
		mod( require( '../../lib/codemirror' ) );
	} else if ( typeof define == 'function' && define.amd ) {// AMD
		define( [ '../../lib/codemirror' ], mod );
	} else {// Plain browser env
		mod( CodeMirror );
	}
} )( function ( CodeMirror ) {
	'use strict';

	var ENTITY_TYPES = { 'http://www.wikidata.org/prop/direct/': 'property',
						'http://www.wikidata.org/prop/': 'property',
						'http://www.wikidata.org/prop/novalue/': 'property',
						'http://www.wikidata.org/prop/statement/': 'property',
						'http://www.wikidata.org/prop/statement/value/': 'property',
						'http://www.wikidata.org/prop/qualifier/': 'property',
						'http://www.wikidata.org/prop/qualifier/value/': 'property',
						'http://www.wikidata.org/prop/reference/': 'property',
						'http://www.wikidata.org/prop/reference/value/': 'property',
						'http://www.wikidata.org/wiki/Special:EntityData/': 'item',
						'http://www.wikidata.org/entity/': 'item' },
		ENTITY_SEARCH_API_ENDPOINT = 'https://www.wikidata.org/w/api.php?action=wbsearchentities&search={term}&format=json&language=en&uselang=en&type={entityType}&continue=0';

	CodeMirror.registerHelper( 'hint', 'sparql', function ( editor, callback, options ) {

		if( wikibase_sparqlhint ){
			wikibase_sparqlhint( editor, callback, options );
		}

		var currentWord = getCurrentWord( getCurrentLine( editor ), getCurrentCurserPosition( editor ) ),
			prefix,
			term,
			entityPrefixes;

		if ( !currentWord.word.match( /\S+:\S*/ ) ) {
			return;
		}

		prefix = getPrefixFromWord( currentWord.word );
		term = getTermFromWord( currentWord.word );
		entityPrefixes = extractPrefixes( editor.doc.getValue() );


		if (! entityPrefixes[ prefix ] ) {//unknown prefix
			var list = [{text: term, displayText:'Unknown prefix \''+prefix+':\''}];
			return callback( getHintCompletion( editor, currentWord, prefix, list ) );
		}

		if(term.length === 0){//empty search term
			var list = [{text: term, displayText:'Type to search for an entity'}];
			return callback( getHintCompletion( editor, currentWord, prefix, list ) );
		}

		if ( entityPrefixes[ prefix ] ) {//search entity
			searchEntities( term, entityPrefixes[ prefix ] ).done( function ( list ) {
				callback( getHintCompletion( editor, currentWord, prefix, list ) );
			} );
		}

	} );

	CodeMirror.hint.sparql.async = true;
	CodeMirror.defaults.hintOptions = {};
	CodeMirror.defaults.hintOptions.closeCharacters = /[]/;
	CodeMirror.defaults.hintOptions.completeSingle = false;



	function getPrefixFromWord( word ) {
		return word.split( ':' ).shift();
	}

	function getTermFromWord( word ) {
		return word.split( ':' ).pop();
	}

	function getCurrentLine( editor ) {
		return editor.getLine( editor.getCursor().line );
	}

	function getCurrentCurserPosition( editor ) {
		return editor.getCursor().ch;
	}

	function getHintCompletion( editor, currentWord, prefix , list) {

		var completion = { list: [] };
		completion.from = CodeMirror.Pos( editor.getCursor().line, currentWord.start + prefix.length + 1 );
		completion.to = CodeMirror.Pos( editor.getCursor().line, currentWord.end );
		completion.list = list;

		return completion;
	}

	function searchEntities( term, type ) {
		var entityList = [],
			deferred = $.Deferred();

		$.ajax( {
			url: ENTITY_SEARCH_API_ENDPOINT.replace( '{term}', term ).replace( '{entityType}', type ),
			dataType: 'jsonp'
		} ).done( function ( data ) {

			$.each( data.search, function ( key, value ) {
				entityList.push( { className: 'wikibase-rdf-hint', text: value.id, displayText: value.label + ' (' + value.id + ') ' + value.description + '\n' } );
			} );

			deferred.resolve( entityList );
		} );

		return deferred.promise();
	}

	function getCurrentWord( line, position ) {
		// TODO This will not work for terms containing a slash, for example 'TCP/IP'
		var leftColon = line.substring( 0, position ).lastIndexOf( ':' ),
			left = line.substring( 0, leftColon ).lastIndexOf( ' ' ),
			rightSlash = line.indexOf( '/', position ),
			rightSpace = line.indexOf( ' ', position ),
			right,
			word;

		if ( rightSlash === -1 || rightSlash > rightSpace ) {
			right = rightSpace;
		} else {
			right = rightSlash;
		}

		if ( left === -1 ) {
			left = 0;
		} else {
			left += 1;
		}
		if ( right === -1 ) {
			right = line.length;
		}
		word = line.substring( left, right );
		return { word: word, start: left, end: right };
	}

	function extractPrefixes( text ) {
		var prefixes = {},
			lines = text.split( '\n' ),
			matches;

		$.each( lines, function ( index, line ) {
			// PREFIX wd: <http://www.wikidata.org/entity/>
			if ( matches = line.match( /(PREFIX) (\S+): <([^>]+)>/ ) ) {
				if ( ENTITY_TYPES[ matches[ 3 ] ] ) {
					prefixes[ matches[ 2 ] ] = ENTITY_TYPES[ matches[ 3 ] ];
				}
			}
		} )

		return prefixes;
	}

} );
