Wikibase RDF Query
==================

Tools for Querying Wikibase instances with RDF.  The modules:
* tools - Tools for syncing a Wikibase instance with an SPARQL 1.1 compliant triple store
 * Apache Licensed
* blazegraph - Blazegraph extension to make querying Wikibase instances more efficient
 * GPLv2 Licensed
* common - Code shared between tools and blazegraph
 * Apache Licensed

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
