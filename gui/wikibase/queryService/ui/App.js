var wikibase = wikibase || {};
wikibase.queryService = wikibase.queryService || {};
wikibase.queryService.ui = wikibase.queryService.ui || {};
window.mediaWiki = window.mediaWiki || {};

wikibase.queryService.ui.App = ( function( $, mw ) {
	"use strict";

	var SHORTURL = '//tinyurl.com/create.php?url=';
	var SHORTURL_API = '//tinyurl.com/api-create.php?url=';
	var EXPLORE_URL = 'http://www.wikidata.org/entity/Q';

	/**
	 * A ui application for the Wikibase query service
	 *
	 * @class wikibase.queryService.ui.App
	 * @licence GNU GPL v2+
	 *
	 * @author Stanislav Malyshev
	 * @author Jonas Kress
	 * @constructor
	 *
	 * @param {jQuery} $element
	 * @param {wikibase.queryService.ui.Editor}
	 * @param {wikibase.queryService.api.Sparql}
	 */
	function SELF( $element, editor, sparqlApi, querySamplesApi ) {

		this._$element = $element;
		this._editor = editor;
		this._sparqlApi = sparqlApi;
		this._querySamplesApi = querySamplesApi;

		this._init();
	}

	/**
	 * @property {string}
	 * @private
	 **/
	SELF.prototype._$element = null;

	/**
	 * @property {wikibase.queryService.api.Sparql}
	 * @private
	 **/
	SELF.prototype._sparqlApi = null;

	/**
	 * @property {wikibase.queryService.api.QuerySamplesApi}
	 * @private
	 **/
	SELF.prototype._querySamplesApi = null;

	/**
	 * @property {wikibase.queryService.ui.Editor}
	 * @type wikibase.queryService.ui.Editor
	 * @private
	 **/
	SELF.prototype._editor = null;

	/**
	 * Initialize private members and call delegate to specific init methods
	 * @private
	 **/
	SELF.prototype._init = function() {

		if( !this._sparqlApi ){
			this._sparqlApi = new wikibase.queryService.api.Sparql();
		}

		if( !this._querySamplesApi ){
			this._querySamplesApi = new wikibase.queryService.api.QuerySamples();
		}

		if( !this._editor ){
			this._editor = new wikibase.queryService.ui.Editor();
		}

		this._initEditor();
		this._initExamples();
		this._initDataUpdated();
		this._initQuery();
		this._initRdfNamespaces();
		this._initHandlers();

	};

	/**
	 * @private
	 **/
	SELF.prototype._initEditor = function() {

		this._editor.fromTextArea( this._$element.find( '.queryEditor' )[0] );
		this._editor.addKeyMap( 'Ctrl-Enter',  $.proxy( this._handleQuerySubmit, this ) );
	};

	/**
	 * @private
	 **/
	SELF.prototype._initExamples = function() {
		var self = this;

		new wikibase.queryService.ui.QueryExampleDialog( $( '#QueryExamples' ), this._querySamplesApi, function( query ){
			if ( !query || !query.trim() ) {
				return;
			}

			self._editor.setValue( query );
			var prefixes = wikibase.queryService.RdfNamespaces.STANDARD_PREFIXES.join( '\n' );
			self._editor.prepandValue( prefixes + '\n\n' );
		} );
	};

	/**
	 * @private
	 */
	SELF.prototype._initRdfNamespaces = function() {
		var category, select, ns, container = $( '.namespace-shortcuts' ),
			namespaces = wikibase.queryService.RdfNamespaces.NAMESPACE_SHORTCUTS;

		container.click( function( e ) {
			e.stopPropagation();
		} );

		// add namespaces to dropdowns
		for ( category in namespaces ) {
			select = $( '<select>' )
				.attr( 'class', 'form-control' )
				.append( $( '<option>' ).text( category ) )
				.appendTo( container );
			for ( ns in namespaces[category] ) {
				select.append( $( '<option>' ).text( ns ).attr( {
					value: namespaces[category][ns]
				} ) );
			}
		}
	};

	/**
	 * @private
	 **/
	SELF.prototype._initQuery = function() {
		if ( window.location.hash !== '' ) {
			this._editor.setValue( decodeURIComponent( window.location.hash.substr( 1 ) ) );
			this._editor.refresh();
		}
	};



	/**
	 * @private
	 **/
	SELF.prototype._initDataUpdated = function() {
		this._sparqlApi.queryDataUpdatedTime().done( function( time ){
			$( '#dbUpdated' ).text( time );
		} ).fail(function(){
			$( '#dbUpdated' ).text( '[unable to connect]' );
		});
	};


	/**
	 * @private
	 **/
	SELF.prototype._initHandlers = function() {
		var self = this;

		$( '#query-form' ).submit(  $.proxy( this._handleQuerySubmit, this ) );
		$( '.namespace-shortcuts' ).on( 'change', 'select', $.proxy( this._handleNamespaceSelected, this ) );

		$( '.addPrefixes' ).click( function() {
			var prefixes = wikibase.queryService.RdfNamespaces.STANDARD_PREFIXES.join( '\n' );
			self._editor.prepandValue( prefixes + '\n\n' );
		} );

		$( '#clear-button' ).click( function () {
			self._editor.setValue( '' );
		} );

		$( '.explorer-close' ).click( function( e ){
			e.preventDefault();
			$( '.explorer-panel' ).hide();
		} );

		$( window ).on( 'popstate', $.proxy( this._initQuery, this ) );

		$('body').on('click', function (e) {
		    if ($(e.target).data('toggle') !== 'popover'
		        && $(e.target).parents('.popover.in').length === 0) {
		        $('[data-toggle="popover"]').popover('hide');
		    }
		});

		this._initHandlersDownloads();
	};

	/**
	 * @private
	 **/
	SELF.prototype._initHandlersDownloads = function() {

		var api = this._sparqlApi;
		var DOWNLOAD_FORMATS = {
				'CSV': {
					handler: $.proxy( api.getResultAsCsv, api ),
					mimetype: 'text/csv;charset=utf-8'
				},
				'JSON': {
					handler: $.proxy( api.getResultAsJson, api ),
					mimetype: 'application/json;charset=utf-8'
				},
				'TSV': {
					handler: $.proxy( api.getSparqlTsv, api ),
					mimetype: 'text/tab-separated-values;charset=utf-8'
				},
				'Simple TSV': {
					handler: $.proxy( api.getSimpleTsv, api ),
					mimetype: 'text/tab-separated-values;charset=utf-8',
					ext: 'tsv'
				},
				'Full JSON': {
					handler: $.proxy( api.getResultAsAllJson, api ),
					mimetype: 'application/json;charset=utf-8',
					ext: 'json'
				}
			};

		var downloadHandler = function( filename, handler, mimetype ) {

			return function ( e ) {
				e.preventDefault();

				if ( api.getResultLength() === null ) {
					return '';
				}

				// see: http://danml.com/download.html
				download(handler(), filename, mimetype);
			};
		};

		for ( var format in DOWNLOAD_FORMATS ) {
			var extension = DOWNLOAD_FORMATS[format].ext || format.toLowerCase();
			var formatName = format.replace( /\s/g, '-' );
			$( '#download' + formatName ).click(
					downloadHandler( 'query.' + extension,	DOWNLOAD_FORMATS[format].handler,
							DOWNLOAD_FORMATS[format].mimetype )
			 );
		}
	};

	/**
	 * @private
	 **/
	SELF.prototype._handleQuerySubmit = function( e ) {
		var self = this;

		e.preventDefault();
		this._editor.save();

		var hash = encodeURIComponent( this._editor.getValue() );
		if ( window.location.hash !== hash ) {
			window.location.hash = hash;
		}
		$( '#shorturl' ).attr( 'href', SHORTURL + encodeURIComponent( window.location ) );

		$( '#shorturl' ).click( function(){

	    	$( '#shorturl' ).popover({
	    		placement : 'left',
	    		'html':true,
	    		'content':function(){
	    			return '<iframe class="shortUrl" src="' + SHORTURL_API + encodeURIComponent( window.location )  +   '">';
	    		}
	    	});
	    	$( '#shorturl' ).popover('show');
			return false;
		} );

		$( '#query-result' ).empty( '' );
		$( '#query-result' ).hide();
		$( '.query-total' ).hide();
		$( '.actionMessage' ).show();
		$( '.actionMessage' ).text( 'Running query...' );

		var query = $( '#query-form' ).serialize();
		this._sparqlApi.query(query)
		.done( $.proxy( this._handleQueryResult, this ) )
		.fail(function(){
			$( '.actionMessage' ).hide();
			$( '#query-error' ).html( $( '<pre>' ).text( self._sparqlApi.getErrorMessage() ) ).show();
			self._editor.highlightError( self._sparqlApi.getErrorMessage() );
		} );

		$( '.queryUri' ).attr( 'href',self._sparqlApi.getQueryUri() );
	};

	/**
	 * @private
	 */
	SELF.prototype._handleQueryResult = function() {

		var api = this._sparqlApi;
		$( '#total-results' ).text( api.getResultLength() );
		$( '#query-time' ).text( api.getExecutionTime() );
		$( '.query-total' ).show();
		$( '#query-result' ).append( api.getResultAsTable() ).show();
		$( '.actionMessage' ).hide();
		$( '#query-error' ).hide();

		var $linkableItems = $( '#query-result' ).find('a').filter( function() {
			return this.href.match( EXPLORE_URL + '(.+)' );
		} );

		var $exploreLink = $( '<a href="#" title="Explore item" class="glyphicon glyphicon-search" aria-hidden="true"></a>' );
		$exploreLink.click( $.proxy( this._handleExploreItem, this ) );
		$linkableItems.after( $exploreLink );

		//$linkableItems.attr( 'href', '#');
	};

	/**
	 * @private
	 */
	SELF.prototype._handleNamespaceSelected = function( e ) {
		var ns, uri = e.target.value, current = this._editor.getValue();

		if ( current.indexOf( '<' + uri + '>' ) === -1 ) {
			ns = $( e.target ).find(':selected').text();
			this._editor.setValue('PREFIX ' + ns + ': <' + uri + '>\n' + current);
		}

		// reselect group label
		e.target.selectedIndex = 0;
	};

	/**
	 * @private
	 */
	SELF.prototype._handleExploreItem = function( e ) {
		var id, url = $(e.target).prev().attr( 'href' ) || '', match;

		match = url.match( EXPLORE_URL + '(.+)' );
		if ( !match ) {
			return;
		}

		$( '.explorer' ).empty( '' );
		$( '.explorer-panel' ).show();

		id = match[1];
		mw.config = { get: function () {
			return 'Q'+id;
		} };
		EXPLORER( $, mw, $( '.explorer' ) );

		return false;
	};

	return SELF;
}( jQuery, mediaWiki ) );
