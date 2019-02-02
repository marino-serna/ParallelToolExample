package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool_example.commons.BaseTesting

class MethodsTesting extends BaseTesting {


  /**
    * This test will check the functionality of one internal method.
    *
    * Since this application is an example the test are only counts, in a real application more useful test should be perform.
    */
  test("Test method wasteTime") {
    import utils._
    import spark.implicits._

    val df1 = spark.sparkContext.parallelize(Array[(String, Int,Int, Int,Int, Int)](
      ("k1", 1,2,3,4,5)
    )).toDF("key","field1","field2","field3","field4","field5")

    val df2 = spark.sparkContext.parallelize(Array[(String,Int, Int,Int, Int, Int)](
      ("k1", 1,2,3,4,5)
    )).toDF("key","field1","field2","field3","field4","field5")

    val result = wasteTime(df1,df2)

    val df1Count = df1.count()
    val resultCount = result.count()
    assert(df1Count == resultCount, "The number of registers is not matching")
  }
}