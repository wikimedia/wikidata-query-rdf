# Categories

This document describes how to set up and maintain Mediawiki categories graph on top of Wikidata Query Service.

# Setup

In order to create categories namespace, run `createNamespace.sh categories`.

# Data loading

To load the data, the dumps should be in https://dumps.wikimedia.org/other/categoriesrdf or another place with analogous structure pointed to by SOURCE variable.

Run script loadCategoryDump.sh for each wiki, e.g.: `loadCategoryDump.sh testwiki`