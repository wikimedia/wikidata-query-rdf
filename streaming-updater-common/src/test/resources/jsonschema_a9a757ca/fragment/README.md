This directory contains schemas that are meant to be included in other
final event schemas via JSON $ref pointers.

This can be done in a couple of different ways.

### $ref the full schema file
This is most useful when used with jsonschema-tools allOf merging capabilities.
Since even fragment schemas should have `title` and `$id` fields, you often
want to keep your enclosing schema's top level fields, merging them over the
included schemas fields.

```lang=yaml
title: my/event/schema
description: A specific event schema
$id: /my/event/schema/1.0.0
$schema: http://json-schema.org/draft-07/schema#
type: object
allOf:
  # Include the common schema fields
  - $ref: /fragment/common/1.0.0#
  # Include the http field.  This will include and merge all of the
  # http schema, including the http field.  This will end up
   # adding an `http` field to /my/event/schema/1.0.0.
  - $ref: /fragment/http/1.0.0#
  # As well as all of the fields needed for /my/event/schema/1.0.0.
  - properties:
      my_event_schema_field:
        type: string
    # ...
```

### $ref a specific field inside a schema

If you wanted to use the `http` schema's `http` field, but rename the
field in my/event/schema differently, you could do the following:

```lang=yaml
# ...
allOf:
  - $ref: /fragment/common/1.0.0#
  - properties:
      my_event_schema_field:
        type: string
    # Include the http schema's http field under a new field name
      http_info:
        $ref: /fragment/http/1.0.0#/properties/http
    # ...
```
