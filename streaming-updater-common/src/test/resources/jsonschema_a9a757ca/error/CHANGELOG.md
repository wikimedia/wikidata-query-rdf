# 2.1.0

Add error_type.

# 2.0.0
Technically 'backwards imcompatible', but existent
downstream systems should not care.
We are adding a new field, and removing requiredness.

- Use fragment/common/2.0.0 to get dt field.
- raw_event should not be required

# 1.0.0
- Add errored_schema_uri and errored_stream_name

# 0.0.3
- switch to semver versioning 0.0.3 is after 2.yaml
- switch to JSONSchema Draft 7
- use $schema instead of meta.schema, meta.schema has been removed.
- use meta.stream instead of meta.topic, meta.topic has been removed.
- set $id to the relative schema URI
- meta.uri is no longer required, as it wasn't always meaningful
- meta.uri format is now uri-reference instead of uri to support relative URIs.

# Version 2
- Added `minLength: 1` for required string properties.
