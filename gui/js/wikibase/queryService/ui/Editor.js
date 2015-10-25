var wikibase = wikibase || { queryService: { ui: {}} };

wikibase.queryService.ui.Editor = ( function( $ ) {
	"use strict";

	/**
	 * An ui editor for the Wikibase query service
	 *
	 * @class wikibase.queryService.ui.Editor
	 * @licence GNU GPL v2+
	 * @author Jonas Kress
	 * @constructor
	 */
	function SELF() {
	}

	/**
	 * Construct an editor on the given textarea DOM element
	 *
	 * @param {jQuery} $element
	 * @throws {Error} If given element is not a valid jQuery textarea
	 **/
	SELF.prototype.fromTextArea = function( $element ){
	};


	return SELF;

}( jQuery ) );
