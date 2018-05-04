Wikibase RDF Query
==================

Tools for Querying Wikibase instances with RDF.  The modules:
* blazegraph - Blazegraph extension to make querying Wikibase instances more efficient
  * GPLv2 Licensed
* war - Configurations for Blazegraph and the service
  * GPLv2 Licensed
* tools - Tools for syncing a Wikibase instance with an SPARQL 1.1 compliant triple store
  * Apache Licensed
* common - Code shared between tools and blazegraph
  * Apache Licensed
* testTools - Helpers for testing
  * Apache Licensed
* gui - UI for running queries and displaying results
  * Apache Licensed
* dist - scripts for running the service
  * Apache Licensed

See more in the [User Manual](https://www.mediawiki.org/wiki/Wikidata_query_service/User_Manual).

Logging
-------
Mapped Diagnostic Context contains some information depending on the context:

* remote-query: a query sent to mediawiki
* a few request related context, see `ch.qos.logback.classic.helpers.MDCInsertingServletFilter` for details)

See also `org.wikidata.query.rdf.common.LoggingNames`.

Development Notes
-----------------
### Eclipse
Works well with m2e.

### Randomized Testing
Some tests use RandomizedRunner.  If they fail you'll get a stack trace containing a "seed" that looks like this:
```
	at __randomizedtesting.SeedInfo.seed([A4D62887A701F9F1:1BF047C091E0A9C2]:0)
```
You can reuse that see by adding @Seed to the test class like this:
```java
	@RunWith(RandomizedRunner.class)
	@Seed("A4D62887A701F9F1:1BF047C091E0A9C2")
	public class MungerUnitTest extends RandomizedTest {
```
Just remember to remove the @Seed annotation before committing the code.

We use RandomizedRunner because its a good way to cover a ton of testing ground with relatively little code.  Its how Lucene consistently finds bugs in the JVM before they're hit in production.

### Unit and Integration Testing
All tests either end in "UnitTest" or "IntegrationTest".  "UnitTest"s are so named because they don't need any external services.  "IntegrationTest"s either need to spin up some service like Blazegraph or they need an Internet connection to wikidata.org or test.wikidata.org.

### Blazegraph
We use Blazegraph for testing SPARQL.  You can start it from the command line by running
```bash
	cd tools && runBlazegraph.sh
```
It is started automatically during integration testing.

### Maven
pom.xml files are sorted according to the usual code convention. The
[sortpom-maven-plugin](https://github.com/Ekryd/sortpom/) is used to fail the
build if this order is not respected. The pom.xml can be automatically sorted
with:
```bash
mvn sortpom:sort
```

The application can be started by running the following command in the war submodule:
```bash
mvn -pl war jetty:run
```

The `-pl war` argument tells maven to run inside the `war` submodule, this is equivalent to running:
```bash
cd war && mvn jetty:run
```

The same target can be used directly from your IDE to run in debug mode and use all the nice IDE integration (automatic class reloading, ...). Check your IDE documentation for details.

Note: `jetty:run` will not automatically detect changes to other modules, but if you run `mvn install` in the root of the project, the changes should be compiled and jetty should auto reload the application.

Current central released version: [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.wikidata.query.rdf/service/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.wikidata.query.rdf/service)