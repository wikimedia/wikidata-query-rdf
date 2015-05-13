# Exploring linked data

In the following examples, we'll see how we can discover information by exploring linked data by walking through a few SPARQL analogs of "casually clicking around the Web".

## George Washington's linked data

George Washington has the identifier [`Q23`](http://www.wikidata.org/wiki/Q23) on Wikidata.  Let's see what we can learn about the national founder through RDF.

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>

SELECT ?predicate ?object WHERE {
  entity:Q23 ?predicate ?object .
} LIMIT 10
```

This query asks for the *predicate* and *object* of every *subject/predicate/object* triple which has the subject *Q23*.  It yields the following:

| predicate                                   | object                                                                                |
| ------------------------------------------- | ------------------------------------------------------------------------------------- |
| schema:dateModified                         | 2015-04-23T01:45:55Z                                                                  |
| schema:description                          | American politician, 1st president of the United States (in office from 1789 to 1797) |
| schema:version                              | 212398883                                                                             |
| <http://www.wikidata.org/prop/direct/P1005> | 64839                                                                                 |
| <http://www.wikidata.org/prop/direct/P1006> | 069038171                                                                             |
| <http://www.wikidata.org/prop/direct/P1017> | ADV10077200                                                                           |
| <http://www.wikidata.org/prop/direct/P106>  | <http://www.wikidata.org/entity/Q131512>                                              |
| <http://www.wikidata.org/prop/direct/P106>  | <http://www.wikidata.org/entity/Q1734662>                                             |
| <http://www.wikidata.org/prop/direct/P106>  | <http://www.wikidata.org/entity/Q189290>                                              |

Let's explore the statement in last row:

`<http://www.wikidata.org/entity/Q23> <http://www.wikidata.org/prop/direct/P106> <http://www.wikidata.org/entity/Q189290> .`

On Wikidata, [`P106`](http://www.wikidata.org/wiki/Property:P106) is the identifier for the *occupation* property, and [`Q189290`](http://www.wikidata.org/wiki/Q189290) is the identifier for the *(military) officer* entity.  In other words, this statement says that George Washington held the occupation of *officer*.  This is interesting, but it doesn't tell us a whole lot about where or where Washington was an officer, or what rank he held during that time.

Let's find out everything we know about this item.

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX p:      <http://www.wikidata.org/prop/>
PREFIX ps:     <http://www.wikidata.org/prop/statement/>

SELECT ?predicate ?object WHERE {
  entity:Q23 p:P106 ?subject .
  ?subject ps:P106 entity:Q189290 .
  ?subject ?predicate ?object .
} LIMIT 10
```

| predicate                                     | object                                        |
| --------------------------------------------- | --------------------------------------------- |
| <http://www.wikidata.org/prop/statement/P106> | <http://www.wikidata.org/entity/Q189290>      |
| <http://www.wikiba.se/ontology#rank>          | <http://www.wikiba.se/ontology#BestRank>      |
| <http://www.wikiba.se/ontology#rank>          | <http://www.wikiba.se/ontology#NormalRank>    |

There's nothing very interesting here; it looks like a dead end.  Let's explore a different path, substituting *position held* ([`P39`](http://www.wikidata.org/wiki/Property:P39)) for *occupation*.  First, we'll check whether Washington held any positions:

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX a:      <http://www.wikidata.org/prop/direct/>

SELECT ?object WHERE {
  entity:Q23 a:P39 ?object .
} LIMIT 10
```

| object                                    |
| ----------------------------------------- |
| <http://www.wikidata.org/entity/Q11696>   |
| <http://www.wikidata.org/entity/Q1115127> |

It looks like Washington held the position with the Wikidata identifier [`Q11696`](http://www.wikidata.org/wiki/Q11696), or *President of the United States of America*.  Let's find out more about it.

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX p:      <http://www.wikidata.org/prop/>
PREFIX ps:     <http://www.wikidata.org/prop/statement/>

SELECT ?predicate ?object WHERE {
  entity:Q23 p:P39 ?subject .
  ?subject ps:P39 entity:Q11696 .
  ?subject ?predicate ?object .
} LIMIT 10
```

| predicate                                           | object                                                                       |
| --------------------------------------------------- | ---------------------------------------------------------------------------- |
| <http://www.w3.org/ns/prov#wasDerivedFrom>          | <http://www.wikidata.org/reference/7eb64cf9621d34c54fd4bd040ed4b61a88c4a1a0> |
| <http://www.wikidata.org/prop/qualifier/P1365>      | <http://www.wikiba.se/ontology#Novalue>                                      |
| <http://www.wikidata.org/prop/qualifier/P1366>      | <http://www.wikidata.org/entity/Q11806>                                      |
| <http://www.wikidata.org/prop/qualifier/P580>       | 1789-04-30T00:00:00Z                                                         |
| <http://www.wikidata.org/prop/qualifier/value/P580  | <http://www.wikidata.org/value/75cbf81427a9b5854184f36402952925>             |
| <http://www.wikidata.org/prop/qualifier/P582>       | 1797-03-04T00:00:00Z                                                         |
| <http://www.wikidata.org/prop/qualifier/value/P582> | <http://www.wikidata.org/value/2b259e264018fbb2123f2667d6912f0e>             |
| <http://www.wikidata.org/prop/statement/P39>        | <http://www.wikidata.org/entity/Q11696>                                      |
| <http://www.wikiba.se/ontology#rank>                | <http://www.wikiba.se/ontology#BestRank>                                     |
| <http://www.wikiba.se/ontology#rank>                | <http://www.wikiba.se/ontology#NormalRank>                                   |

There's lots of information about Washington's presidency.  We can see a couple of dates with the years 1789 and 1797, which seem likely to be the beginning and ending of his presidency.  Checking Wikidata, we can see that the corresponding property identifiers, [`P580`](http://www.wikidata.org/wiki/Property:P580) and [`P582`](http://www.wikidata.org/wiki/Property:P582), are indeed *start time* and *end time*, respectively.

## Data linked to George Washington

Let's flip things around and see what data are associated with George Washington.

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>

SELECT ?subject ?predicate WHERE {
  ?subject ?predicate entity:Q23 .
} LIMIT 10
```

| subject                                                                                 | predicate                                     |
| --------------------------------------------------------------------------------------- | --------------------------------------------- |
| <http://www.wikidata.org/entity/Q191789>                                                | <http://www.wikidata.org/prop/direct/P26>     |
| <http://www.wikidata.org/entity/Q458119>                                                | <http://www.wikidata.org/prop/direct/P40>     |
| <http://www.wikidata.org/entity/Q5083373>                                               | <http://www.wikidata.org/prop/direct/P7>      |
| <http://www.wikidata.org/entity/Q7412891>                                               | <http://www.wikidata.org/prop/direct/P7>      |
| <http://www.wikidata.org/entity/Q8488276>                                               | <http://www.wikidata.org/prop/direct/P301>    |
| <http://www.wikidata.org/entity/Q850421>                                                | <http://www.wikidata.org/prop/direct/P7>      |
| <http://www.wikidata.org/entity/Q61>                                                    | <http://www.wikidata.org/prop/direct/P138>    |
| <http://www.wikidata.org/entity/Q1223>                                                  | <http://www.wikidata.org/prop/direct/P138>    |
| <http://www.wikidata.org/entity/Q511164>                                                | <http://www.wikidata.org/prop/direct/P138>    |
| <http://www.wikidata.org/entity/statement/q511164-cd0598a2-4b84-3acf-79c0-5c044cb6a054> | <http://www.wikidata.org/prop/statement/P138> |

A bunch of entities are linked to Washington via a variety of properties.  Let's find out about the first one:

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>

SELECT ?subject WHERE {
  entity:Q191789 rdfs:label ?subject .
} LIMIT 10
```

| subject           |
| ----------------- |
| Martha Washington |

We found Washington's wife.  She is associated with him via [`P26`](http://www.wikidata.org/wiki/Property:P26), the Wikidata identifier for *spouse*.

To discover this without manually looking it on the Wikidata wiki, we can inspect the label of the entity that connects Washington's wife to him:

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX wikibase: <http://wikiba.se/ontology#>

SELECT ?relationship WHERE {
  entity:Q191789 ?predicate entity:Q23 .
  ?property wikibase:directClaim ?predicate .
  ?property rdfs:label ?relationship .
} LIMIT 10
```

| relationship |
| ------------ |
| spouse       |
