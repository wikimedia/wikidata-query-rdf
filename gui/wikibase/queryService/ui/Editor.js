var wikibase = wikibase || {};
wikibase.queryService = wikibase.queryService || {};
wikibase.queryService.ui = wikibase.queryService.ui || {};

wikibase.queryService.ui.Editor = ( function( $ ) {
	"use strict";

	var CODEMIRROR_DEFAULTS = {
			"lineNumbers": true,
			"matchBrackets": true,
			"mode": 'sparql',
			"extraKeys": { 'Ctrl-Space': 'autocomplete' },
			"viewportMargin": Infinity
		},
		ERROR_LINE_MARKER = null,
		ERROR_CHARACTER_MARKER = null;

	/**
	 * An ui this._editor for the Wikibase query service
	 *
	 * @class wikibase.queryService.ui.this._editor
	 * @licence GNU GPL v2+
	 *
	 * @author Stanislav Malyshev
	 * @author Jonas Kress
	 * @constructor
	 */
	function SELF() {
	}

	/**
	 * @property {CodeMirror}
	 * @type CodeMirror
	 * @private
	 **/
	SELF.prototype._editor = null;

	/**
	 * Construct an this._editor on the given textarea DOM element
	 *
	 * @param {Element} element
	 **/
	SELF.prototype.fromTextArea = function( element ) {
		var self = this;

		this._editor = CodeMirror.fromTextArea( element, CODEMIRROR_DEFAULTS );
		this._editor.on( 'change', function () {
			self.clearError();
		} );
		this._editor.focus();

		new WikibaseRDFTooltip(this._editor);
	};

	/**
	 * Construct an this._editor on the given textarea DOM element
	 *
	 * @param {string} keyMap
	 * @throws {function} callback
	 **/
	SELF.prototype.addKeyMap = function( keyMap, callback ) {
		this._editor.addKeyMap( { keyMap : callback } );
	};

	/**
	 * @param {string} value
	 **/
	SELF.prototype.setValue = function( value ) {
		this._editor.setValue( value );
	};

	/**
	 * @return {string}
	 **/
	SELF.prototype.getValue = function() {
		return this._editor.getValue();
	};

	SELF.prototype.save = function() {
		this._editor.save();
	};

	/**
	 * @param {string} value
	 **/
	SELF.prototype.prepandValue = function( value ) {
		this._editor.setValue( value + this._editor.getValue() );
	};

	SELF.prototype.refresh = function() {
		this._editor.refresh();
	};

	/**
	 * Highlight SPARQL error in editor window.
	 *
	 * @param {string} description
	 */
	SELF.prototype.highlightError = function( description ) {
		var line, character,
			match = description.match( /line (\d+), column (\d+)/ );
		if ( match ) {
			// highlight character at error position
			line = match[1] - 1;
			character = match[2] - 1;
			ERROR_LINE_MARKER = this._editor.doc.markText(
				{ 'line': line, 'ch': 0 },
				{ 'line': line },
				{ 'className': 'error-line' }
			);
			ERROR_CHARACTER_MARKER = this._editor.doc.markText(
				{ 'line': line, 'ch': character },
				{ 'line': line, 'ch': character + 1 },
				{ 'className': 'error-character' }
			);
		}
	};

	/**
	 * Clear SPARQL error in editor window.
	 *
	 * @param {string} description
	 */
	SELF.prototype.clearError = function() {
		if ( ERROR_LINE_MARKER ) {
			ERROR_LINE_MARKER.clear();
			ERROR_CHARACTER_MARKER.clear();
		}
	};

	return SELF;

}( jQuery ) );
