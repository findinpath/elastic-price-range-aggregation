package com.findinpath

import com.findinpath.collapser.PriceRangeCollapser
import com.findinpath.model.Product
import org.apache.http.HttpHost
import org.elasticsearch.Version
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange
import org.elasticsearch.search.aggregations.metrics.ParsedTDigestPercentiles
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.math.BigDecimal
import java.util.*


/**
 * This test class provides a demo on how to retrieve price range aggregations on Elasticsearch.
 * The aggregation buckets for a search need to be specified within the search request.
 * This leads to one of the following limitations for the price range aggregations:
 *
 * - either a bucket will contain most of items and it will therefor not aggregate the price ranges in a relevant fashion
 * (e.g. : 0 - 100 EUR for backpacks)
 * - there would be too many buckets too choose from which could overwhelm the user
 * (e.g. : 0 - 10 EUR, 10 - 20 EUR, 20 - 30 EUR, 40 - 50 EUR, 50 - 100 EUR, 100 - 200 EUR, 200-300 EUR, 300-400 EUR, 400 - 500 EUR, over 500 EUR for backpacks)
 *
 * There is also an open feature request to support dynamically calculated price ranges:
 *
 * https://github.com/elastic/elasticsearch/issues/9572
 *
 *
 */
@Testcontainers
class PriceRangeAggregationTest {

    @BeforeEach
    fun setup() {
        getClient(elasticsearchContainer).use({ client ->
            createProductsIndex(client)
            indexProducts(client)
        })
    }

    /**
     * This demo test shows how to perform statically price range aggregations on Elasticsearch.
     *
     * The main inconvenient of this method is that the prices distribution for a specific
     * search needs to be known beforehand so that the price range aggregation buckets
     * are specified in the search request.
     */
    @Test
    fun staticAggregationDemo() {
        val client = getClient(elasticsearchContainer)

        logger.info("Perform a search on the `Luggage` category and aggregate the prices into statically defined price buckets")
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.query(QueryBuilders.matchQuery("category", "Luggage"))
        val priceRangesAggregation = AggregationBuilders.range("price_ranges")
            .field("price")
            .addUnboundedTo(100.0)
            .addRange(100.0, 200.0)
            .addUnboundedFrom(200.0)
        searchSourceBuilder.aggregation(priceRangesAggregation)
        val searchRequest = SearchRequest(PRODUCTS_INDEX)
        searchRequest.source(searchSourceBuilder)
        val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

        val priceRangesAggregationResponse = searchResponse.aggregations.get<ParsedRange>("price_ranges")
        val buckets = priceRangesAggregationResponse.buckets
        assertThat(buckets.size, equalTo(3))

        assertThat(buckets[0].to, equalTo(100.0))
        assertThat(buckets[0].docCount, equalTo(6))

        assertThat(buckets[1].from, equalTo(100.0))
        assertThat(buckets[1].to, equalTo(200.0))
        assertThat(buckets[1].docCount, equalTo(1))

        assertThat(buckets[2].from, equalTo(200.0))
        assertThat(buckets[2].docCount, equalTo(2))

    }

    /**
     * Based on some of the suggestions from the feature request
     *
     * https://github.com/elastic/elasticsearch/issues/9572
     *
     * the buckets can be calculated dynamically by executing the search request two times:
     * <ul>
     *     <li>the first request is used for getting an approximation of the aggregation buckets</li>
     *     <li>the second request is used for getting the actual document count of the previously
     *     retrieved aggregation buckets</li>
     * </ul>
     *
     * The method exposed in this method can be a viable alternative for retrieving dynamically
     * balanced aggregations for the price ranges in case that having two queries for the search
     * result doesn't pose a serious result retrieval speed inconvenient.
     */
    @Test
    fun percentilesAggregationDemo() {
        val client = getClient(elasticsearchContainer)


        logger.info("Retrieve the price distribution information for the search in the `Luggage` category")
        val percentilesSearchSourceBuilder = SearchSourceBuilder()
        percentilesSearchSourceBuilder.size(0) // aggregations are required, but search hits are not required
        percentilesSearchSourceBuilder.query(QueryBuilders.matchQuery("category", "Luggage"))
        val pricePercentilesAggregation = AggregationBuilders.percentiles("price_percentiles")
            .field("price")
            .percentiles(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0 , 90.0)
        percentilesSearchSourceBuilder.aggregation(pricePercentilesAggregation)
        val percentilesSearchRequest = SearchRequest(PRODUCTS_INDEX)
        percentilesSearchRequest.source(percentilesSearchSourceBuilder)

        val percentilesSearchResponse = client.search(percentilesSearchRequest, RequestOptions.DEFAULT)
        val pricePercentilesResponse = percentilesSearchResponse.aggregations.get<ParsedTDigestPercentiles>("price_percentiles")

        // collapse the response to a specified number of price range buckets
        // which is obviously smaller than the number of percentiles available.
        //  5 buckets would correspond to the [0-20], [20-40], [40,60], [60-80], [80-100] buckets in the percentiles
        //  3 buckets would correspond to the [0-33], [33-66], [66-100] buckets in the percentiles

        // round the percentile number corresponding to the price to the nearest 10
        val pricePercentile20 = Math.round(pricePercentilesResponse.percentile(20.0)/10.0) * 10
        val pricePercentile40 = Math.round(pricePercentilesResponse.percentile(40.0)/10.0) * 10
        val pricePercentile60 = Math.round(pricePercentilesResponse.percentile(60.0)/10.0) * 10
        val pricePercentile80 = Math.round(pricePercentilesResponse.percentile(80.0)/10.0) * 10

        // get rid of possible duplicates
        val pricePercentileBuckets = setOf(pricePercentile20, pricePercentile40, pricePercentile60, pricePercentile80)

        if (pricePercentileBuckets.size < 3){
            // there's no need for price range aggregation when the number of buckets is this small
            return
        }

        // now that we have the price buckets, we can proceed to do the price range aggregation
        logger.info("Perform the search on the `Luggage` category with price aggregation buckets based on the results of the price percentiles query")
        val priceRangesSearchSourceBuilder = SearchSourceBuilder()
        priceRangesSearchSourceBuilder.query(QueryBuilders.matchQuery("category", "Luggage"))
        val priceRangesAggregation = AggregationBuilders.range("price_ranges")
            .field("price")

        // Build the aggregation buckets in the search request
        val priceBucketsIterator = pricePercentileBuckets.iterator()
        var current = priceBucketsIterator.next().toDouble()
        priceRangesAggregation.addUnboundedTo(current)
        while (priceBucketsIterator.hasNext()){
            val from = current
            current = priceBucketsIterator.next().toDouble()
            priceRangesAggregation.addRange(from, current)
            if (!priceBucketsIterator.hasNext()) {
                priceRangesAggregation.addUnboundedFrom(current)
            }
        }

        priceRangesSearchSourceBuilder.aggregation(priceRangesAggregation)
        val priceRangesSearchRequest = SearchRequest(PRODUCTS_INDEX)
        priceRangesSearchRequest.source(priceRangesSearchSourceBuilder)
        val priceRangesSearchResponse = client.search(priceRangesSearchRequest, RequestOptions.DEFAULT)
        val priceRangesAggregationResponse = priceRangesSearchResponse.aggregations.get<ParsedRange>("price_ranges")
        val priceBuckets = priceRangesAggregationResponse.buckets

        assertThat(priceBuckets.size, equalTo(5))

        assertThat(priceBuckets[0].to, equalTo(30.0))
        assertThat(priceBuckets[0].docCount, equalTo(1))

        assertThat(priceBuckets[1].from, equalTo(30.0))
        assertThat(priceBuckets[1].to, equalTo(40.0))
        assertThat(priceBuckets[1].docCount, equalTo(3))

        assertThat(priceBuckets[2].from, equalTo(40.0))
        assertThat(priceBuckets[2].to, equalTo(80.0))
        assertThat(priceBuckets[2].docCount, equalTo(2))

        assertThat(priceBuckets[3].from, equalTo(80.0))
        assertThat(priceBuckets[3].to, equalTo(180.0))
        assertThat(priceBuckets[3].docCount, equalTo(1))

        assertThat(priceBuckets[4].from, equalTo(180.0))
        assertThat(priceBuckets[4].docCount, equalTo(2))
    }

    /**
     * This demo test shows how to collapse a set of statically defined price range aggregations
     * into a specified number of buckets.
     *
     * As already mentioned the price aggregation buckets can't be known beforehand for making
     * a random search. On the other hand, if there are enough buckets (e.g. : 20/30/40) specified in the price range
     * aggregation request, the resulting aggregations can be collapsed in a smaller number of buckets (e.g. : 3/4/5)
     * which gives concrete options to the user to choose from.
     *
     * As can be seen from the method below, it can be quite annoying to specify all the buckets when doing search
     * requests with aggregation for the price ranges. In this case it is advisable to make use of the
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-template.html">Search Template</a>
     * feature to avoid tying the application client code to the raw buckets for the aggregation of prices.
     */
    @Test
    fun collapsedAggregationDemo(){
        val client = getClient(elasticsearchContainer)

        logger.info("Perform a search on the `Luggage` category and aggregate the prices into very granular statically defined price buckets")
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder.query(QueryBuilders.matchQuery("category", "Luggage"))
        val priceRangesAggregation = AggregationBuilders.range("price_ranges")
            .field("price")
            .addUnboundedTo(10.0)
            .addRange(10.0, 20.0)
            .addRange(20.0, 30.0)
            .addRange(30.0, 40.0)
            .addRange(40.0, 50.0)
            .addRange(50.0, 60.0)
            .addRange(60.0, 70.0)
            .addRange(70.0, 80.0)
            .addRange(80.0, 90.0)
            .addRange(90.0, 100.0)

            .addRange(100.0, 110.0)
            .addRange(110.0, 120.0)
            .addRange(120.0, 130.0)
            .addRange(130.0, 140.0)
            .addRange(140.0, 150.0)
            .addRange(150.0, 160.0)
            .addRange(160.0, 170.0)
            .addRange(170.0, 180.0)
            .addRange(180.0, 190.0)
            .addRange(190.0, 200.0)

            .addRange(200.0, 250.0)
            .addRange(250.0, 300.0)

            .addRange(300.0, 350.0)
            .addRange(350.0, 400.0)

            .addRange(400.0, 450.0)
            .addRange(450.0, 500.0)

            .addRange(500.0, 600.0)
            .addRange(600.0, 700.0)
            .addRange(700.0, 800.0)
            .addRange(800.0, 900.0)
            .addRange(900.0, 1000.0)

            .addRange(1000.0, 1500.0)
            .addRange(1500.0, 2000.0)
            .addRange(2000.0, 2500.0)
            .addRange(2500.0, 3000.0)
            .addRange(3000.0, 3500.0)
            .addRange(3500.0, 4000.0)
            .addRange(4000.0, 4500.0)
            .addRange(4500.0, 5000.0)

            .addUnboundedFrom(5000.0)
        searchSourceBuilder.aggregation(priceRangesAggregation)
        val searchRequest = SearchRequest(PRODUCTS_INDEX)
        searchRequest.source(searchSourceBuilder)
        val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

        val priceRangesAggregationResponse = searchResponse.aggregations.get<ParsedRange>("price_ranges")
        val buckets = priceRangesAggregationResponse.buckets
        assertThat(buckets.size, equalTo(40))


        val collapsedPriceBuckets = PriceRangeCollapser.collapseParsedBuckets(buckets.toList() as List<ParsedRange.ParsedBucket>, 3)
        assertThat(collapsedPriceBuckets.size, equalTo(3))

        assertThat(collapsedPriceBuckets[0].to?.toDouble(), equalTo(80.0))
        assertThat(collapsedPriceBuckets[0].docCount, equalTo(6))

        assertThat(collapsedPriceBuckets[1].from?.toDouble(), equalTo(80.0))
        assertThat(collapsedPriceBuckets[1].to?.toDouble(), equalTo(250.0))
        assertThat(collapsedPriceBuckets[1].docCount, equalTo(2))

        assertThat(collapsedPriceBuckets[2].from?.toDouble(), equalTo(250.0))
        assertThat(collapsedPriceBuckets[2].docCount, equalTo(1))
    }

    private fun getClient(container: ElasticsearchContainer): RestHighLevelClient {
        return RestHighLevelClient(RestClient.builder(HttpHost.create(container.httpHostAddress)))
    }


    private fun createProductsIndex(client: RestHighLevelClient) {
        if (client.indices().exists(GetIndexRequest(PRODUCTS_INDEX), RequestOptions.DEFAULT)) {
            client.indices().delete(DeleteIndexRequest(PRODUCTS_INDEX), RequestOptions.DEFAULT)
        }

        logger.info("Create the Elasticsearch $PRODUCTS_INDEX index")
        val createProductsIndexRequest = CreateIndexRequest(PRODUCTS_INDEX)
        val productsMappingsbuilder = jsonBuilder()
        productsMappingsbuilder.startObject().apply {
            startObject("properties").apply {
                startObject("name").apply {
                    field("type", "text")
                }.endObject()
                startObject("price").apply {
                    field("type", "scaled_float")
                    field("scaling_factor", 100)
                }.endObject()
                startObject("category").apply {
                    field("type", "keyword")
                }.endObject()
            }.endObject()
        }.endObject()
        createProductsIndexRequest.mapping(productsMappingsbuilder)
        client.indices().create(createProductsIndexRequest, RequestOptions.DEFAULT)
    }

    private fun indexProducts(client: RestHighLevelClient) {
        logger.info("Adding products to the Elasticsearch $PRODUCTS_INDEX index")
        val jansport_driver_8_backpack =
            Product(
                UUID.randomUUID().toString(),
                "JanSport Driver 8 Wheeled Backpack",
                BigDecimal("78.60"),
                "Luggage"
            )
        val caribee_sky_master_70_travel_pack =
            Product(
                UUID.randomUUID().toString(),
                "Caribee Sky Master 70 Travel Pack",
                BigDecimal("205.60"),
                "Luggage"
            )
        val outdoor_runway_33_trolley_rucksack =
            Product(
                UUID.randomUUID().toString(),
                "Outdoor Runway 33 Trolley Rucksack",
                BigDecimal("134.44"),
                "Luggage"
            )
        val casual_universal_backpack =
            Product(UUID.randomUUID().toString(), "Casual Universal Backpack", BigDecimal("21.52"), "Luggage")
        val gusti_leder_rucksack =
            Product(UUID.randomUUID().toString(), "Gusti Leder nature Rucksack", BigDecimal("55.81"), "Luggage")
        val evervanz_unisex_backpack = Product(
            UUID.randomUUID().toString(),
            "EverVanz Unisex Roll Top Waterproof Hiking Backpack",
            BigDecimal("32.29"),
            "Luggage"
        )
        val gfavor_canvas_leather_backpack =
            Product(UUID.randomUUID().toString(), "G-FAVOR Canvas Leather Backpack", BigDecimal(39.98), "Luggage")
        val augur_casual_backpack =
            Product(UUID.randomUUID().toString(), "AUGUR Casual Backpack", BigDecimal(32.99), "Luggage")
        val the_bridge_story_donna_backpack = Product(
            UUID.randomUUID().toString(), "The Bridge Story Donna Leather Backpack", BigDecimal(418.60), "Luggage"
        )
        val products = listOf(
            jansport_driver_8_backpack, caribee_sky_master_70_travel_pack,
            outdoor_runway_33_trolley_rucksack, casual_universal_backpack,
            gusti_leder_rucksack, evervanz_unisex_backpack,
            evervanz_unisex_backpack, gfavor_canvas_leather_backpack,
            augur_casual_backpack, the_bridge_story_donna_backpack
        )
        products.forEach { product -> indexProduct(product, client) }
    }


    private fun indexProduct(product: Product, client: RestHighLevelClient) {
        val sourcebuilder = XContentFactory.jsonBuilder()
        sourcebuilder.startObject().apply {
            field("name", product.name)
            field("price", product.price)
            field("category", product.category)
        }.endObject()
        val request = IndexRequest(PRODUCTS_INDEX)
            .id(product.id)
            .source(sourcebuilder)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)

        client.index(request, RequestOptions.DEFAULT)

    }

    companion object {
        /**
         * Elasticsearch version which should be used for the Tests
         */
        val ELASTICSEARCH_VERSION: String = Version.CURRENT.toString()


        val PRODUCTS_INDEX: String = "products"

        private val logger = LoggerFactory.getLogger(PriceRangeAggregationTest::class.java)

        @Container
        val elasticsearchContainer =
            ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:$ELASTICSEARCH_VERSION")

    }
}