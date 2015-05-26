'use strict';

var EXPLORER = function ($, mw, attachContent) {

  var initGraph = function (rootId) {

    var graphContainer = document.createElement('div');
    graphContainer.style.height = '100%';
    graphContainer.style.width = '64em';
    graphContainer.style.float = 'left';

    var panelContainer = document.createElement('div');
    panelContainer.style.height = '100%';
    panelContainer.style.width = '24em';
    panelContainer.style.overflow = 'auto';
    panelContainer.style.float = 'left';

    var clear = document.createElement('div');
    clear.style.clear = 'left';

    var container = document.createElement('div');
    container.style.height = '48em';
    container.style.width = '90em';
    container.appendChild(graphContainer);
    container.appendChild(panelContainer);
    container.appendChild(clear);
    attachContent.append(container);

    var nodes = new mw.vis.DataSet();
    var edges = new mw.vis.DataSet();

    var network = new mw.vis.Network(
      graphContainer,
      { nodes: nodes, edges: edges },
      { edges: { style: 'arrow' } }
    );

    network.on('doubleClick', function (properties) {
      if (properties.nodes.length === 1) {
        var label = nodes.get(properties.nodes[0]).label;
        window.open('//en.wikipedia.org/wiki/' + label, '_blank');
      }
    });

    network.on('click', function (properties) {
      while (panelContainer.hasChildNodes()) {
        panelContainer.removeChild(panelContainer.childNodes[0]);
      }
      if (properties.nodes.length === 1) {

        var loading = document.createElement('em');
        loading.innerHTML = 'Loading links...';
        panelContainer.appendChild(loading);

        getBindings(properties.nodes[0]);

      }
    });

    var showPanel = function (fromId, allBindings) {
      while (panelContainer.hasChildNodes()) {
        panelContainer.removeChild(panelContainer.childNodes[0]);
      }

      var histogram = {};
      var tally = function(bindings) {
        for (var i = 0; i < bindings.length; i++) {
          var binding = bindings[i];
          var linkLabel = value(binding.pl);
	  histogram[linkLabel] = histogram[linkLabel] || 0;
          histogram[linkLabel] = histogram[linkLabel] + 1;
        }
      };
      tally(allBindings.outgoing);
      tally(allBindings.incoming);

      var list = [];
      for (var x in histogram) {
        if (histogram.hasOwnProperty(x)) {
          list.push(createListItem(fromId, allBindings, x, histogram[x]));
        }
      }
      list.sort(function (a, b) { return b.count - a.count; });

      var heading = document.createElement('h2');
      heading.innerHTML = 'Links';
      panelContainer.appendChild(heading);

      for (var i = 0; i < list.length; i++) {
        showListItem(list[i]);
      }

    };

    var createListItem = function (fromId, allBindings, label, item) {
      return {
        label: label,
        count: item,
        show: function () {
          showBindings(fromId, allBindings.outgoing, label);
          showBindings(fromId, allBindings.incoming, label, true);
         }
      };
    };

    var showListItem = function (item) {
      var button = document.createElement('button');
      button.innerHTML = 'show';
      button.style.margin = '0.5em';
      button.onclick = item.show;

      var count = document.createElement('em');
      count.style.margin = '0.25em';
      count.innerHTML = '(' + item.count + ')';

      var label = document.createElement('em');
      label.style.margin = '0.25em';
      label.innerHTML = item.label;

      var buttonContainer = document.createElement('div');
      buttonContainer.appendChild(button);
      buttonContainer.appendChild(count);
      buttonContainer.appendChild(label);

      panelContainer.appendChild(buttonContainer);
    };
 
    var getBindings = function (fromId) {
      mw.wdqsGetOutgoingLinksById(fromId).sequence(
        mw.wdqsGetIncomingLinksById(fromId)).apply(
        function (bindingses) {
          showPanel(fromId, { outgoing: bindingses[0], incoming: bindingses[1] });
        }
      );
    };

    var value = function (bindingVar) {
      if (bindingVar && bindingVar.value) {
        return bindingVar.value;
      } else {
        return '(unknown)';
      }
    };

    var entityId = function (bindingVar) {
      if (bindingVar && bindingVar.value) {
        return bindingVar.value.substr('http://www.wikidata.org/entity/'.length);
      } else {
        return '(unknown)';
      }
    };

    var showBindings = function (fromId, bindings, linkType, incoming) {
      for (var i = 0; i < bindings.length; i++) {
        var binding = bindings[i];
        var linkLabel = value(binding.pl);
        if (linkLabel === linkType) {
          var toId = entityId(binding.o);
          var toLabel = value(binding.ol);
          addNode(toId, toLabel);
          if (incoming) {
            addLink(toId, linkLabel, fromId);
          } else {
            addLink(fromId, linkLabel, toId);
          }
        }
      }
    };

    var addNode = function(id, label) {
      if (!nodes.get(id)) {
        nodes.add([ { id: id, label: label } ]);
      }
    };

    var addLink = function(fromId, linkLabel, toId) {
      var id = fromId + '-' + linkLabel + '-' + toId;
      if (!edges.get(id)) {
        edges.add([ { id: id, from: fromId, to: toId, label: linkLabel } ]);
      }
    };

    mw.wdqsGetLabelById(rootId).apply(
      function (label) {
        addNode(rootId, label);
      }
    );

  };

  initGraph(mw.config.get('wgWikibaseItemId'));

};
