package com.findinpath.collapser

import java.math.BigDecimal

data class PriceRangeBucket(var from: BigDecimal?, var to: BigDecimal?, var docCount: Long)