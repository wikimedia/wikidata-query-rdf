As part of https://phabricator.wikimedia.org/T308017,
MediaWiki state change events are being remodeled as entities.

We have learned much since when we first modeled
MediaWiki event data in ~2016.  Notably, we've learned that it
is better to model state changes as 'change' events
for specific entities in a changelog.  The intention
is to represent all of an entitiy's state changes in the same
stream, and to do that, we need a full representation
of the current state of that entity in each event in that stream.

We also need reusable fragments of actual state change
schemas too, not just the entity models.

- The state/entity directory contains data models that
  represent an MediaWiki entity's state at a specific time.

- The state/change directory has fragments that represent
  a state change of an entity (or entities).  These mostly exist
  so that they can also be used as reusable fragments,
  e.g. mediawiki/page/change and
  possibly mediawiki/page/change_with_rendered_content_body.

The mediawiki/common/, mediawiki/revision/, and mediawiki/user/ directories here are
still used by active event streams, but may one (distant)
day be deprecated.  If you are creating a new MediaWiki
state change stream, please consider modeling the events as changes to
entities.
