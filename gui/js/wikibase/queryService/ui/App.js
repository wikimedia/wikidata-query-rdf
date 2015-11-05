var wikibase = wikibase || { queryService: { ui: {}} };

wikibase.queryService.ui.App = ( function( $ ) {
	"use strict";

	/**
	 * A ui application for the Wikibase query service
	 *
	 * @class wikibase.queryService.ui.App
	 * @licence GNU GPL v2+
	 * @author Jonas Kress
	 * @constructor
	 *
	 * @param {jQuery} $element
	 * @param {wikibase.queryService.ui.Editor}
	 * @param {wikibase.queryService.API}
	 */
	function SELF( $element, editor, api ) {
		this.$element = $element;
		this.editor = editor;
		this.api = api;

		if( !this.api ){
			this.api = new wikibase.queryService.Api();
		}

		if( !this.editor ){
			this.editor = new wikibase.queryService.ui.Editor();
		}
	}

	/**
	 * @property {string}
	 * @private
	 **/
	this.$element = null;

	/**
	 * @property {wikibase.queryService.API}
	 * @private
	 **/
	this.api = null;

	/**
	 * @property {wikibase.queryService.ui.Editor}
	 * @type wikibase.queryService.ui.Editor
	 * @private
	 **/
	this.editor = null;


	return SELF;
}( jQuery ) );
