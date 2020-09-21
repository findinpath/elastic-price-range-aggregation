Elasticsearch Price Range Aggregations
======================================

A common functionality in each webshop is the ability to show price ranges
when searching for products.

When searching for a generic product (e.g. : backpack, shoes) the users
are nowadays accustomed to receive a relevant price aggregation showing 
the distribution of the product prices from the shop.

e.g.: when searching for `backpack` on a marketplace website, there could 
be displayed the following price range listing:

* up to 20 EUR - 10
* 20 to 40 EUR - 28
* 40 to 80 EUR - 52
* 80 to 180 EUR - 12
* from 180 EUR - 4


Elasticsearch supports performing [range aggregations](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html) 
on a search request, but it requires the user to specify explicitly the price ranges:

```bash
curl -X GET "localhost:9200/products/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": { 
     "match": {
     	"category": "Luggage"
     } 
  },
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price",
        "ranges": [
          { "to": 100.0 },
          { "from": 100.0, "to": 200.0 },
          { "from": 200.0 }
        ]
      }
    }
  }  
}
'
``` 

The ranges in the price aggregation depend on the other hand pretty much on what
is being searched.
The price ranges that may be relevant to the users when searching for `shoes` will 
very likely be different from the ones that will be shown when searching for `laptop`
because most of the shoes won't cost more than 150 EUR, where most of the decent laptops 
start at 300-400 EUR. 

There is currently quite an old open feature request to support 
dynamically calculated price ranges:

https://github.com/elastic/elasticsearch/issues/9572

This proof of concept project tries to come with a few answers on how to come
up with balanced ranges for the price aggregations on Elasticsearch searches.


## Static price ranges

As described above, Elasticsearch already offers the ability of specifying
ranges for the price in a search request.
The users like to choose from a list of up to 5 price ranges in order to
avoid being overwhelmed with options.

The main drawback when using this approach is that at the request time
there is no way to know how the prices of the products will be distributed.
As a consequence, the price ranges used will sometimes not contain relevant
information.

## Use two queries

Obtain first the distribution of prices for the search by using the 
[percentiles aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-percentile-aggregation.html)
and subsequently use this information for building up the buckets of the price
aggregation to be used in the actual search.

The few buckets that will be shown to the user for the price ranges will mirror 
the actual price distribution for the product searched.
On the other hand, there are two search requests needed to be made against Elasticsearch
in order to obtain this information.

## Collapse the buckets

This method builds on top of the static price ranges previously described.
Instead of retrieving a handful of buckets for the price range aggregation,
this method relies on specifying a lot of price range buckets (e.g. : 30/40/50)   
which are then collapsed on the client side on a handful (e.g. : 3/4/5) of relevant price range
buckets.

The main performance drawback in this approach is that there are much more price range buckets
specified to be aggregated in the search request compared to the number price range buckets that are
actually needed to be displayed to the user.

## Run the project

This project contains a JUnit testcase that spawns a [testcontainers](https://www.testcontainers.org/) 
[Elasticsearch container](https://www.testcontainers.org/modules/elasticsearch/) fills it with
test data and tries through different strategies to retrieve the price range aggregations. 

Check the source code of the [PriceRangeAggregationTest.kt](src/test/kotlin/com/findinpath/PriceRangeAggregationTest.kt)
JUnit test class to see the implementation of the aggregation methods exposed above. 

Run the command

```
./gradlew test
```

for executing the tests from this project.

## Feedback

As already mentioned, this project serves as a proof of concept for performing
price aggregations. Eventual improvements to the project code or ideas regarding
alternative ways to get the price aggregations are very much welcome. 