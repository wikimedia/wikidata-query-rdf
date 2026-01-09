### Version 1.3.0
- Bumped to user entity schema refs to 1.1.0 to
  pick up user_central_id field.
  https://phabricator.wikimedia.org/T403664

### Version 1.2.0
- performer is now optional.  We don't set it for revision visibility changes where the change is admin suppressed.
  https://phabricator.wikimedia.org/T342487
  NOTE: While removing required-ness is technically a breaking change
  that requires a major version bump,
  it should not break any consumers in this case.
  We've added an exception in .jsonschema-tools.yaml to skip the
  backwards compatibility check for version 1.2.0.

### Version 1.1.0
- Create a local `definitions` to DRY up referencing
  repeated fields, e.g. `revision_count`.

- Declare `revision_count` field in extended page entities directly in this schema.
  `revision_count` has been removed from fragment/mediawiki/state/entity/page
  to make that entity schema more reusable.

- Add new `page.redirect_page_link` field using fragment/mediawiki/state/entity/page_link
  to represent the page to which a redirect page links to.
  https://phabricator.wikimedia.org/T325315


