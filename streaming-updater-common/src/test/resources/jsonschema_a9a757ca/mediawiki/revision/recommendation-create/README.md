The recommendation-create event signals to to consumers (primarily, to the
search infrastructure) that a new task recommendation has been created.
This mechanism is used to keep the MediaWiki database storing the
recommendations, and the search index making articles with recommendations
findable, in sync. Other updates (such as when a recommendation becomes
obsolete due to an edit) happen via the normal MediaWiki index update
mechanism.

For more information about link recommendations, see
https://wikitech.wikimedia.org/wiki/Add_Link

For more information about task recommendations in general, see
https://www.mediawiki.org/wiki/Growth/Personalized_first_day/Structured_tasks
