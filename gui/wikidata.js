'use strict';

(function ($, mw) {

  var id = function (x) {
    return x;
  };

  var future = function(f) {
    return {
      apply: function (k) {
        if (k === undefined) {
          return f(id);
        } else {
          return f(k);
        }
      },
      map: function (g) {
        return future (function (k) {
          return f(function (x) {
            return k(g(x));
          });
        });
      },
      flatMap: function (g) {
        return future (function (k) {
          return f(function (x) {
            return g(x).apply(k);
          });
        });
      }
    };
  };

  var entitiesCache = {};

  mw.wdGetEntities = function (ids) {
    var uncachedIds = [];
    for (var i = 0; i < ids.length; i++) {
      if (entitiesCache[ids[i]] === undefined) {
        uncachedIds.push(ids[i]);
      }
    }
    return future(function (k) {
      $.ajax({
        url: '//www.wikidata.org/w/api.php',
        data: { action: 'wbgetentities', ids: uncachedIds.join('|'), format: 'json' },
        dataType: 'jsonp',
        success: function (x) { return k(x.entities); }
      });
    }).map(function (uncachedEntities) {
      for (var id in uncachedEntities) {
        if (uncachedEntities.hasOwnProperty(id)) {
          entitiesCache[id] = uncachedEntities[id];
        }
      }
      var entities = {};
      for (var i = 0; i < ids.length; i++) {
        entities[ids[i]] = entitiesCache[ids[i]];
      }
      return entities;
    });
  };

  mw.wdGetEntity = function (id) {
    if (entitiesCache[id]) {
      return future(function (k) {
        k(entitiesCache[id]);
      });
    } else {
      return mw.wdGetEntities([id]).map(function (entities) {
        return entities[id];
      });
    }
  };

  mw.wdEntityLabel = function (entity, lang) {
    lang = lang || 'en';
    return entity.labels[lang].value;
  };

  mw.properties = {};

})(jQuery, mediaWiki);

