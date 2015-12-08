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

		var examples = [], deferred = $.Deferred();

		$.ajax( {
			url: 'https://www.mediawiki.org/w/api.php?action=query&prop=revisions&titles=Wikibase/'	+
			'Indexing/SPARQL_Query_Examples&rvprop=content',
			data: {
				format: 'json'
			},
			dataType: 'jsonp'
		} ).done( function ( data ) {

			var wikitext = data.query.pages[Object.keys( data.query.pages )].revisions[0]['*'];
				wikitext = wikitext.replace( /\{\{!\}\}/g, '|' );

			var re = /(?:[\=]+)([^\=]*)(?:[\=]+)\n(?:[]*?)(?:[^=]*?)({{SPARQL\|[\s\S]*?}}\n){1}/g, m,
				regexQuery = /query\s*\=([^]+)(?:}}|\|)/,
				regexExtraPrefix = /extraprefix\s*\=([^]+?)(?:\||}})+/,
				regexTags = /{{Q\|([^]+?)\|([^]+?)}}+/g;

			while ( m = re.exec( wikitext ) ) {
				var paragraph = m[0],
					title = m[1].trim(),
					tags = [], tag,
					href = 'https://www.mediawiki.org/wiki/Wikibase/Indexing/SPARQL_Query_Examples#' + encodeURIComponent(title.replace( / /g, "_" )).replace( /%/g, "." ) ,
					sparqlTemplate = m[2],
					query = sparqlTemplate.match( regexQuery )[1].trim();

					if(sparqlTemplate.match( regexExtraPrefix )){
						query = sparqlTemplate.match(regexExtraPrefix)[1] + '\n\n' + query;
					}
					if( paragraph.match( regexTags ) ) {
						while(tag = regexTags.exec( paragraph ) ) {
							tags.push(tag[2].trim() + ' (' + tag[1].trim() + ')');
						}
					}

				examples.push( {title: title, query: query, href: href, tags: tags} );
			}
			deferred.resolve( examples );
		} );

		return deferred;
	};

	return SELF;

}( jQuery ) );
