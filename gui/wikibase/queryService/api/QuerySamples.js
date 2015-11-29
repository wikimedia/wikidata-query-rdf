var wikibase = wikibase || {};
wikibase.queryService = wikibase.queryService || {};
wikibase.queryService.api = wikibase.queryService.api || {};

wikibase.queryService.api.QuerySamples = ( function( $ ) {
	"use strict";

	/**
	 * QuerySamples API for the Wikibase query service
	 *
	 * @class wikibase.queryService.api.QuerySamples
	 * @licence GNU GPL v2+
	 *
	 * @author Stanislav Malyshev
	 * @author Jonas Kress
	 * @constructor
	 */
	function SELF() {
	}

	/**
	 * @return {jQuery.Deferred} Object taking list of example queries  { title:, query: }
	 **/
	SELF.prototype.getExamples = function() {

		var examples = {}, deferred = $.Deferred();

		$.ajax( {
			url: 'https://www.mediawiki.org/w/api.php?action=query&prop=revisions&titles=Wikibase/'	+
			'Indexing/SPARQL_Query_Examples&rvprop=content',
			data: {
				format: 'json'
			},
			dataType: 'jsonp'
		} ).done( function ( data ) {
			var wikitext = data.query.pages[Object.keys( data.query.pages )].revisions[0]['*'];
			var paragraphs = wikitext.split( '==' );

			//TODO extract and make more robust against wrong formatting on the wiki page
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
						examples[title] = query;
					}
				}
			} );

			deferred.resolve( examples );
		} );

		return deferred;
	};

	return SELF;

}( jQuery ) );
