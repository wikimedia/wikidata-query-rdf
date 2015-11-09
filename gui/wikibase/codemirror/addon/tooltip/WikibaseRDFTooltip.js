var WikibaseRDFTooltip = ( function ( CodeMirror, $ ) {
	'use strict';

	/**
	 * Wikibase RDF tooltip for codemirror editor
	 *
	 * @class WikibaseRdfTooltip
	 * @licence GNU GPL v2+
	 * @author Jonas Kress
	 * @constructor
	 */
	function SELF( editor ) {
		this.editor = editor;
		this._registerHandler();
	}

	SELF.prototype.editor = null;
	SELF.prototype.tooltipTimeoutHandler = null;

	var ENTITY_TYPES = {
		'http://www.wikidata.org/prop/direct/': 'property',
		'http://www.wikidata.org/prop/': 'property',
		'http://www.wikidata.org/prop/novalue/': 'property',
		'http://www.wikidata.org/prop/statement/': 'property',
		'http://www.wikidata.org/prop/statement/value/': 'property',
		'http://www.wikidata.org/prop/qualifier/': 'property',
		'http://www.wikidata.org/prop/qualifier/value/': 'property',
		'http://www.wikidata.org/prop/reference/': 'property',
		'http://www.wikidata.org/prop/reference/value/': 'property',
		'http://www.wikidata.org/wiki/Special:EntityData/': 'item',
		'http://www.wikidata.org/entity/': 'item'
	};

	var ENTITY_SEARCH_API_ENDPOINT = 'https://www.wikidata.org/w/api.php?action=wbsearchentities&search={term}&format=json&language=en&uselang=en&type={entityType}&continue=0';

	SELF.prototype._registerHandler = function () {
		CodeMirror.on( this.editor.getWrapperElement(), 'mouseover', $.proxy( this._triggerTooltip, this ) );
	};

	SELF.prototype._triggerTooltip = function ( e ) {
		clearTimeout( this.tooltipTimeoutHandler );
		this._removeToolTip();

		var self = this;
		this.tooltipTimeoutHandler = setTimeout( function () {
			self._createTooltip( e );
		}, 500 );
	};

	SELF.prototype._createTooltip = function ( e ) {
		var posX = e.clientX,
			posY = e.clientY + $( window ).scrollTop(),
			token = this.editor.getTokenAt( this.editor.coordsChar( { left: posX, top: posY } ) ).string;

		if ( !token.match( /.+\:(Q|P)[0-9]*/ ) ) {
			return;
		}

		var prefixes = this._extractPrefixes( this.editor.doc.getValue() );
		var prefix = token.split( ':' ).shift();
		var entityId = token.split( ':' ).pop();

		if ( !prefixes[prefix] ) {
			return;
		}

		var self = this;
		this._searchEntities( entityId, prefixes[prefix] ).done( function ( list ) {
			self._showToolTip( list.shift(), { x: posX, y: posY } );
		} );
	};

	SELF.prototype._removeToolTip = function () {
		$( '.wikibaseRDFtoolTip' ).remove();
	};

	SELF.prototype._showToolTip = function ( text, pos ) {
		if ( !text || !pos ) {
			return;
		}
		$( '<div/>' )
		.text( text )
		.css( 'position', 'absolute' )
		.css( 'background-color', 'white' )
		.css( 'z-index', '100' )
		.css( 'border', '1px solid grey' )
		.css( 'max-width', '200px' )
		.css( 'padding', '5px' )
		.css( { top: pos.y + 2, left: pos.x + 2 } )
		.addClass( 'wikibaseRDFtoolTip' )
		.appendTo( 'body' )
		.fadeIn( 'slow' );
	};

	SELF.prototype._extractPrefixes = function ( text ) {
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
		} );

		return prefixes;
	};

	SELF.prototype._searchEntities = function ( term, type ) {
		var entityList = [],
			deferred = $.Deferred();

		$.ajax( {
			url: ENTITY_SEARCH_API_ENDPOINT.replace( '{term}', term ).replace( '{entityType}', type ),
			dataType: 'jsonp'
		} ).done( function ( data ) {

			$.each( data.search, function ( key, value ) {
				entityList.push( value.label + ' (' + value.id + ')\n' + value.description );
			} );

			deferred.resolve( entityList );
		} );

		return deferred.promise();
	};

	return SELF;

}( CodeMirror, jQuery ) );
