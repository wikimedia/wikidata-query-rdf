### Version 1.1.0
- Use fragment/http/1.2.0, which removes client_ip (and adds protocol).
  In https://phabricator.wikimedia.org/T262626, we want to stop
  collecting client_ip by default.  eventgate-wikimedia currently uses
  the presence of the http.client_ip property in the schema to decide if
  it should set it to the value of X-Client-IP.
  This is technically a backwards incompatible change, but mediawiki/client/error
  data currently only is ingested into LogStash, which does not care.
### Version 2.0.0
- Rename tags field to error_context. "tags" is a reserved word in our
  Logstash setup which made sending tag data impossible.
  Normally backwards-incompatible schema changes can be problematic, but
  because this schema only writes to Logstash, and because it wasn't possible
  to send data in the tag field, it should be fine in this case.
