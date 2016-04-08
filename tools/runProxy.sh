mvn exec:java -Dexec.classpathScope=test -Dexec.mainClass=org.wikidata.query.rdf.tool.Proxy -Dexec.args="--port 8812 --error 503 --errorMod 2 -v"

