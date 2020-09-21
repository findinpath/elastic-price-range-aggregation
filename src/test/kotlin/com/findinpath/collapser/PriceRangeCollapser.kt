package com.findinpath.collapser

import org.elasticsearch.search.aggregations.bucket.range.ParsedRange
import java.math.BigDecimal

object PriceRangeCollapser {

    fun collapseParsedBuckets(
        priceBuckets: List<ParsedRange.ParsedBucket>,
        collapsedBucketsCount: Int
    ): List<PriceRangeBucket> {
        return collapsePriceRangeBuckets(priceBuckets.map(this::convert), collapsedBucketsCount)
    }

    fun collapsePriceRangeBuckets(
        priceBuckets: List<PriceRangeBucket>,
        collapsedBucketsCount: Int
    ): List<PriceRangeBucket> {
        val collapsedPriceBuckets = priceBuckets
            .filter { it.docCount > 0 }
            .toMutableList()

        while (collapsedPriceBuckets.size > collapsedBucketsCount) {
            var pointBeanIndex = 0
            while (pointBeanIndex < collapsedPriceBuckets.size - 2) {
                val priceRangeBucket1 = collapsedPriceBuckets.get(pointBeanIndex)
                val priceRangeBucket2 = collapsedPriceBuckets.get(pointBeanIndex + 1)
                val priceRangeBucket3 = collapsedPriceBuckets.get(pointBeanIndex + 2)

                val (priceRangeBucketToRemove, lowerPriceRangeBucket, upperPriceRangeBucket) =
                    getTheRangeToBeRemovedAndTheTwoRangesWithSmallerHits(
                        priceRangeBucket1,
                        priceRangeBucket2,
                        priceRangeBucket3
                    )
                val collapsedCount: Long = lowerPriceRangeBucket.docCount + upperPriceRangeBucket.docCount
                val collapsedFacet = PriceRangeBucket(
                    lowerPriceRangeBucket.from,
                    upperPriceRangeBucket.to,
                    collapsedCount
                )
                collapsedPriceBuckets.set(collapsedPriceBuckets.indexOf(priceRangeBucket2), collapsedFacet)
                collapsedPriceBuckets.remove(priceRangeBucketToRemove)
                if (collapsedPriceBuckets.size <= collapsedBucketsCount) {
                    break
                }
                pointBeanIndex += 2
            }
        }

        // fill in empty spaces in the price ranges
        collapsedPriceBuckets[0].from = null
        for (i in 1 until collapsedPriceBuckets.size){
            collapsedPriceBuckets[i].from = collapsedPriceBuckets[i-1].to
        }
        collapsedPriceBuckets[collapsedPriceBuckets.size - 1].to = null

        return collapsedPriceBuckets.toList()
    }

    private fun getTheRangeToBeRemovedAndTheTwoRangesWithSmallerHits(
        priceRangeBucket1: PriceRangeBucket,
        priceRangeBucket2: PriceRangeBucket,
        priceRangeBucket3: PriceRangeBucket
    ): Triple<PriceRangeBucket, PriceRangeBucket, PriceRangeBucket> {
        return if (priceRangeBucket1.docCount + priceRangeBucket2.docCount < priceRangeBucket2.docCount + priceRangeBucket3.docCount) {
            Triple(priceRangeBucket1, priceRangeBucket1, priceRangeBucket2)
        } else {
            Triple(priceRangeBucket3, priceRangeBucket2, priceRangeBucket3)
        }
    }


    private fun convert(bucket: ParsedRange.ParsedBucket): PriceRangeBucket {
        return PriceRangeBucket(
            if (bucket.from == Double.NEGATIVE_INFINITY) null else BigDecimal(bucket.from as Double),
            if (bucket.to == Double.POSITIVE_INFINITY) null else BigDecimal(bucket.to as Double),
            bucket.docCount
        )
    }
}