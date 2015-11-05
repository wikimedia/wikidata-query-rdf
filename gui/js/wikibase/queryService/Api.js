var wikibase = wikibase || { queryService: { ui: {}} };

wikibase.queryService.Api = ( function( $ ) {
	"use strict";

	/**
	 * API for the Wikibase query service
	 *
	 * @class wikibase.queryService.Api
	 * @licence GNU GPL v2+
	 * @author Jonas Kress
	 * @constructor
	 */
	function SELF() {
	}

	/**
	 * Submit a query to the API
	 *
	 * @param {string[]} data
	 * @return {jQuery.Promise}
	 **/
	SELF.prototype.submitQuery = function( data ){
	};

	/**
	 * Get the result of the submitted query
	 *
	 * @return {string}
	 **/
	SELF.prototype.getResult = function(){
	};

	/**
	 * Get the result of the submitted query as CSV
	 *
	 * @return {string}
	 **/
	SELF.prototype.getResultAsCSV = function(){
	};

	return SELF;

}( jQuery ) );
