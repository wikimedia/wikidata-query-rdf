/**
 * Code completion for Wikibase entities RDF prefixes in SPARQL
 * completes SPARQL keywords and ?variables
 *
 * licence GNU GPL v2+
 *
 * @author Jonas Kress
 */

var wikibase_sparqlhint = null;

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

	var SPARQL_KEYWORDS = [ 'SELECT', 'OPTIONAL', 'WHERE', 'ORDER', 'ORDER BY', 'DISTINCT', 'WHERE {\n\n}', 'SERVICE', 'SERVICE wikibase:label {\n bd:serviceParam wikibase:language "en" .\n}',
	                        'BASE', 'PREFIX', 'REDUCED', 'FROM', 'LIMIT', 'OFFSET', 'HAVING', 'UNION'];

	wikibase_sparqlhint = function ( editor, callback, options ) {
		var currentWord = getCurrentWord( getCurrentLine( editor ), getCurrentCurserPosition( editor ) ),
			hintList = [];


		if ( currentWord.word.indexOf('?') === 0 ) {
			hintList = hintList.concat( getVariableHints( currentWord.word,
										getDefinedVariables( editor.doc.getValue() ) ) );
		}

		hintList = hintList.concat( getSPARQLHints( currentWord.word ) );

		if(hintList.length > 0){
			callback( getHintCompletion( editor, currentWord, hintList ) );
		}

	}

	CodeMirror.hint.sparql.async = true;
	CodeMirror.defaults.hintOptions = {};
	CodeMirror.defaults.hintOptions.closeCharacters = /[]/;
	CodeMirror.defaults.hintOptions.completeSingle = false;

	function getSPARQLHints( term ){
		var list = [];

		$.each(SPARQL_KEYWORDS, function( key, keyword ){
			if( keyword.toLowerCase().indexOf( term.toLowerCase() ) === 0){
				list.push(keyword);
			}
		});

		return list;
	}

	function getDefinedVariables( text ) {
		var variables = [];

		$.each(text.split(' '), function( key, word ){
			if( word.indexOf('?') === 0){
				variables.push(word);
			}
		});

		return $.unique( variables );
	}

	function getVariableHints( term, variables ) {

		var list = [];

		if(!term || term === '?'){
			return variables;
		}

		$.each(variables, function( key, variable ){
			if( variable.toLowerCase().indexOf( term.toLowerCase() ) === 0){
				list.push(variable);
			}
		});

		return list;
	}

	function getHintCompletion( editor, currentWord , list) {

		var completion = { list: [] };
		completion.from = CodeMirror.Pos( editor.getCursor().line, currentWord.start );
		completion.to = CodeMirror.Pos( editor.getCursor().line, currentWord.end );
		completion.list = list;

		return completion;
	}


	function getCurrentWord(line, position) {
		var words = line.split(' '), matchedWord = "", scannedPostion = 0;

		$.each(words, function(key, word) {

			scannedPostion += word.length;

			if (key > 0) {// add spaces to position
				scannedPostion++;
			}

			if (scannedPostion >= position) {
				matchedWord = word;
				return;
			}
		});

		return {
			word : matchedWord,
			start : scannedPostion - matchedWord.length,
			end : scannedPostion
		};
	}

	function getCurrentLine(editor) {
		return editor.getLine(editor.getCursor().line);
	}

	function getCurrentCurserPosition(editor) {
		return editor.getCursor().ch;
	}

} );
