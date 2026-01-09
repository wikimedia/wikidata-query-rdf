### Version 1.2.0
- Remove `client_ip` property from the schema.
  eventgate-wikimedia will set this to the value of the X-Client-IP header
  if this property exists in the schema. We don't want to always do this by
  default, so we remove it from this `http` schema field fragment.
  `http.client_ip can be manually added a schema if this behavior is desired.
  This is technically backwards incompatible change, but we can manage this
  by manually including it in concrete schemas that already exist.  At least
  no new schemas that $ref this http schema fragment will automatically include it
  A new fragment/http/client_ip schema has been added.  This can be $ref-ed
  in a concrete schema directly to continue using http.client_ip.

### Version 1.1.0
- Added `protocol` property to the schema
