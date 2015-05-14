# SPARQL query examples

## Who were Feynman's employers?

* Feynman: [Q39246](https://www.wikidata.org/wiki/Q39246)
* employer: [P108](https://www.wikidata.org/wiki/Property:P108)
* Commons category (a.k.a. label): [P373](https://www.wikidata.org/wiki/Property:P373)

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX t:      <http://www.wikidata.org/prop/direct/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?employer WHERE {
  entity:Q39246 t:P108/rdfs:label ?employer .
} LIMIT 10
```

| employer                           |
| ---------------------------------- |
| Cornell University                 |
| California Institute of Technology |

## Who are Feynman's colleagues?

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX t:      <http://www.wikidata.org/prop/direct/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?employer ?colleague WHERE {
  entity:Q39246 t:P108 ?employerS .
  ?colleagueS t:P108 ?employerS .
  ?employerS rdfs:label ?employer .
  ?colleagueS rdfs:label ?colleague .
} LIMIT 10
```

| employer                           | colleague            |
| ---------------------------------- | -------------------- |
| California Institute of Technology | Eric Temple Bell     |
| California Institute of Technology | John Gamble Kirkwood |
| California Institute of Technology | Joseph Grinnell      |
| California Institute of Technology | Jesse L. Greenstein  |
| California Institute of Technology | Charles C. Steidel   |
| California Institute of Technology | Robert Bacher        |
| California Institute of Technology | Michael Aschbacher   |
| California Institute of Technology | Gerald J. Wasserburg |
| California Institute of Technology | H. Richard Crane     |
| California Institute of Technology | Steven E. Koonin     |

## What are the fields of Feynman's colleagues?

* field of work: [P101](https://www.wikidata.org/wiki/Property:P101)

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX t:      <http://www.wikidata.org/prop/direct/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?colleague ?field WHERE {
  entity:Q39246 t:P108 ?employerS .
  ?colleagueS t:P108 ?employerS .
  ?colleagueS t:P101/rdfs:label ?field .
  ?colleagueS rdfs:label ?colleague .
} LIMIT 10
```

| colleague          | field              |
| ------------------ | ------------------ |
| Carl Sagan         | astrobiology       |
| Harry Vandiver     | number theory      |
| James Ax           | model theory       |
| Mark Kac           | probability theory |
| Michael Aschbacher | group theory       |
| Eric Temple Bell   | combinatorics      |
| Sossina M. Haile   | Fuel cell          |
| Paul Olum          | topology           |
| Elfriede Abbe      | illustration       |
| Rick Durrett       | probability theory |

## What are the fields of Feynman's colleagues who are mathematicians?
physicists?

* occupation: [P106](https://www.wikidata.org/wiki/Property:P106)
* mathematician: [Q170790](https://www.wikidata.org/wiki/Q170790)

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX t:      <http://www.wikidata.org/prop/direct/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?colleague ?field WHERE {
  entity:Q39246 t:P108 ?employerS .
  ?colleagueS t:P108 ?employerS .
  ?colleagueS t:P106 entity:Q170790 .
  ?colleagueS t:P101/rdfs:label ?field .
  ?colleagueS rdfs:label ?colleague .
} LIMIT 10
```
| colleague        | field                 |
| ---------------- | --------------------- |
| Virgil Snyder    | algebraic geometry    |
| John H. Hubbard  | mathematical analysis |
| William Thurston | topology              |
| Karen Vogtmann   | topology              |
| Daina Taimina    | topology              |
| Eugene Dynkin    | algebra               |
| Karen Vogtmann   | group theory          |
| William Feller   | probability theory    |
| Kiyoshi Itō      | probability theory    |
| Harry Kesten     | probability theory    |

## Whose birthday is it?

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX t:      <http://www.wikidata.org/prop/direct/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#>

SELECT ?entity (year(?date) as ?year) WHERE {
  ?entityS t:P569 ?date .
  ?entityS rdfs:label ?entity .

  FILTER (datatype(?date) = xsd:dateTime)
  FILTER (month(?date) = month(now()))
  FILTER (day(?date) = day(now()))
} LIMIT 10
```

| entity                       | year |
| ---------------------------- | ---- |
| Annie Easley                 | 1933 |
| James Buchanan               | 1791 |
| Daniela Hantuchová           | 1983 |
| Radek Pilař                  | 1931 |
| Hugh Seymour Davies          | 1943 |
| Reinhart Koselleck           | 1923 |
| Friedrich von Hagedorn       | 1708 |
| Dietrich Schwanitz           | 1940 |
| Arthur Moeller van den Bruck | 1876 |
| Ruth Leuwerik                | 1924 |

## What happened on this day in history?

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#>

SELECT ?entity ?event (year(?date) as ?year) WHERE {
  ?entityS ?eventS ?propS .
  ?propS ?eventV ?date .
  ?entityS rdfs:label ?entity .
  ?eventS rdfs:label ?event .

  FILTER (datatype(?date) = xsd:dateTime)
  FILTER (month(?date) = month(now()))
  FILTER (day(?date) = day(now()))
} LIMIT 10
```

| entity                 | event         | year |
| ---------------------- | ------------- | ---- |
| Israel                 | head of state | 1963 |
| Annie Easley           | date of birth | 1933 |
| James Buchanan         | date of birth | 1791 |
| Daniela Hantuchová     | date of birth | 1983 |
| Radek Pilař            | date of birth | 1931 |
| Tsakhiagiin Elbegdorj  | position held | 1998 |
| Albert of Saxony       | date of birth | 1828 |
| Hugh Seymour Davies    | date of birth | 1943 |
| Reinhart Koselleck     | date of birth | 1923 |
| Friedrich von Hagedorn | date of birth | 1708 |

## What are the top most populous cities in the world with a female head of government?

```sparql
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX t:      <http://www.wikidata.org/prop/direct/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX q:      <http://www.wikidata.org/prop/qualifier/>
PREFIX p:      <http://www.wikidata.org/prop/>
PREFIX ps:     <http://www.wikidata.org/prop/statement/>

SELECT (MAX(?population) AS ?max_population) ?city ?head WHERE {
  ?cityS t:P31 entity:Q515 .           # find instances of subclasses of city
  ?cityS p:P6 ?x .                     # with a P6 (head of goverment) statement
  ?x ps:P6 ?headS .                    # ... that has the value ?headS
  ?headS t:P21 entity:Q6581072 .       # ... where the ?headS has P21 (sex or gender) female
  FILTER NOT EXISTS { ?x q:P582 ?y }   # ... but the statement has no P582 (end date) qualifier

  ?cityS t:P1082 ?population .

  # Optionally, find English labels for city and head:
  OPTIONAL {
    ?cityS rdfs:label ?city .
    FILTER (lang(?city) = "en")
  }
  OPTIONAL {
    ?headS rdfs:label ?head .
    FILTER (lang(?head) = "en")
  }

}
GROUP BY ?city ?head
ORDER BY DESC(?max_population)
LIMIT 10
```

| max_population | city          | head                        |
| -------------- | ------------- | --------------------------- |
| 2240621        | Paris         | Anne Hidalgo                |
| 2195914        | Houston       | Annise Parker               |
| 1388308        | Munich        | Christine Strobl            |
| 209860         | Rennes        | Nathalie Appéré             |
| 181513         | Trondheim     | Rita Ottervik               |
| 172693         | Albacete      | Maria Carmen Bayod Guinalio |
| 153402         | Logroño       | Cuca Gamarra                |
| 137971         | Cádiz         | Teófila Martínez            |
| 102301         | Liberec       | Martina Rosenbergová        |
| 101319         | Ancona        | Valeria Mancinelli          |

## Who discovered the most asteroids?

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX t: <http://www.wikidata.org/prop/direct/>

SELECT ?discoverer ?name (COUNT(?asteroid) AS ?count)
WHERE {
  ?asteroid t:P31 entity:Q3863 .
  ?asteroid t:P61 ?discoverer .
  OPTIONAL {
    ?discoverer rdfs:label ?name
    FILTER (lang(?name) = "en")
  }
}
GROUP BY ?discoverer ?name
ORDER BY DESC(?count)
LIMIT 10
```

| discoverer                                | name                                        | count |
| ----------------------------------------- | ------------------------------------------- | ----- |
| <http://www.wikidata.org/entity/Q213563>  | Spacewatch                                  | 1625  |
| <http://www.wikidata.org/entity/Q312255>  | Tom Gehrels                                 | 1159  |
| <http://www.wikidata.org/entity/Q231642>  | Ingrid van Houten-Groeneveld                | 1153  |
| <http://www.wikidata.org/entity/Q312755>  | Eric Walter Elst                            | 1048  |
| <http://www.wikidata.org/entity/Q1165635> | Lowell Observatory Near-Earth-Object Search | 912   |
| <http://www.wikidata.org/entity/Q1140499> | Near-Earth Asteroid Tracking                | 742   |
| <http://www.wikidata.org/entity/Q446449>  | Schelte J. Bus                              | 566   |
| <http://www.wikidata.org/entity/Q507053>  | Hiroshi Kaneda                              | 354   |
| <http://www.wikidata.org/entity/Q737057>  | Seiji Ueda                                  | 354   |
| <http://www.wikidata.org/entity/Q58962>   | Eleanor F. Helin                            | 300   |

## Who discovered the most planets?

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX entity: <http://www.wikidata.org/entity/>
PREFIX t: <http://www.wikidata.org/prop/direct/>

SELECT ?discoverer ?name (COUNT(DISTINCT ?planet) as ?count)
WHERE {
  ?ppart t:P279* entity:Q634 .
  ?planet t:P31 ?ppart .
  ?planet t:P61 ?discoverer .
  OPTIONAL {
    ?discoverer rdfs:label ?name
    FILTER (lang(?name) = "en")
  }
}
GROUP BY ?discoverer ?name
ORDER BY DESC(?count) ?name
LIMIT 10
```

| discoverer                                | name                                     | count |
| ----------------------------------------- | ---------------------------------------- | ----- |
| <http://www.wikidata.org/entity/Q123975>  | Michel Mayor                             | 2     |
| <http://www.wikidata.org/entity/Q2850870> | Anne-Marie Lagrange                      | 1     |
| <http://www.wikidata.org/entity/Q712442>  | Artie P. Hatzes                          | 1     |
| <http://www.wikidata.org/entity/Q190232>  | Clyde Tombaugh                           | 1     |
| <http://www.wikidata.org/entity/Q124013>  | Didier Queloz                            | 1     |
| <http://www.wikidata.org/entity/Q736811>  | Geoffrey Marcy                           | 1     |
| <http://www.wikidata.org/entity/Q76431>   | Johann Gottfried Galle                   | 1     |
| <http://www.wikidata.org/entity/Q20015>   | John Couch Adams                         | 1     |
| <http://www.wikidata.org/entity/Q47272>   | Kepler                                   | 1     |
| <http://www.wikidata.org/entity/Q1032158> | Optical Gravitational Lensing Experiment | 1     |
