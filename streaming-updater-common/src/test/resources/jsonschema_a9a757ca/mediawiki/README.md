# Mediawiki Specific Event Schemas

Mediawiki entities are represented in directories here.  Individual
change event types are in subdirectories of each entity.  E.g.

```
page/
  create/
  delete/
  restore/
# ...
```

The schema form of each entity should be fairly similiar, with individual
variations needed in order to represent the properties that change during an
event.  Top level fields such as `page_id` or `user_id` should refer to
the entity that the event represents.  This means that a `user_id`
field on a user event represents the affected user entity, NOT the user
that performed the change.  Mediawiki code refers to such user's as
'performers', and we keep that convention here as well.

For update (change) type events, the prior values of the entity's
changed properties should be contained in a `prior_state` subobject, with
field names the same as the entity's top level field names.

## Versioning
New schema versions should be backwards compatible. They should
only add new optional fields.  They should never remove or rename fields, and
they should never add new required fields.  This allows producer and consumer
code to continue to work with older versions of schemas, so deployments that
alter use of schema versions do not have to be coordinated.

Ideally, each new schema version would be represented by a bump in file version
number.  A new version of `mediawiki/revision/create/1.0.0.yaml` would go in
`mediawiki/revision/create/1.1.0.yaml`.
