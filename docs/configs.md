This help file describes configurations available for Wikidata Query Service.

Configurable Java properties are described in the [User Manual](https://www.mediawiki.org/wiki/Wikidata_query_service/User_Manual#Configurable_properties).

The following configuration files exist (usually located in the root directory of the package):

# RWStore.properties
This file configures the functionality of Blazegraph as the backend engine for Wikidata Query Service. You usually won't need to edit it.
If you want to change the location of the data file, edit this line:

    com.bigdata.journal.AbstractJournal.file=wikidata.jnl

# allowlist.txt
This file contains the list of external (federated) SPARQL endpoints that the service is allowed to call. It is a simple list of URLs, one URL per line.

# ldf-config.json
This file configures Blazegraph [LDF service](https://www.mediawiki.org/wiki/Wikidata_query_service/User_Manual#Linked_Data_Fragments_endpoint).

The format is JSON. The file does not contain user-serviceable parts, except for the prefix list - add there more prefixes if you want them to be recognized in LDF queries. It is not required to add prefixes if you use full URLs.

# prefixes.conf
This file contains the list of prefixes that will be recognized by the service. Note that some prefixes are hardcoded in the code and won't be in this file, but you can add more prefixes to this config.

The format is the same as prefix declaration in the RDF, e.g.:

    PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>

