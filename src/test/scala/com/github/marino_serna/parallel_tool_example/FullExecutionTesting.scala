package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool_example.commons.{BaseTesting}
import org.apache.spark.sql.DataFrame

class FullExecutionTesting  extends BaseTesting {


  /**
    * This test will allow to check:
    * -	The whole application runs successfully
    * -	The result of each method run by ParallelTool, that should include all persisted outputs and some temporal steps.
    * -	The output works as expected (file system/schemas/etc)
    *
    * This test can be run using the test in parallel.
    * If any method that persist the output is call from more than one test you should execute the tests sequentially,
    * for SBT you could use:
    * “parallelExecution in Test := false “
    * or “Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)”
    * otherwise you will find unstable results or errors in your test.
    * Be aware that this test is designed to execute the whole application, that probably include all the methods that persist content.
    * To avoid this issue with the tests, you can test the expected functionality with this test and when a deeper testing is required
    * by using internal methods that doesn't persisting the data, like the one found in the class “MethodsTesting” of this example.
    *
    *
    * Since this application is an example the test are only counts, in a real application more useful test should be perform.
    */
  test("Full execution") {
    import utils.spark.implicits._

    prepareDataBase("project1")

    val listOfTestResults = testClassWithLogic1.startTest() :::
      testClassWithLogic2.startTest() :::
      testClassWithLogic3.startTest() :::
      Nil

    val testingResults:DataFrame = listOfTestResults
      .foldLeft((("",true)::Nil).toDF(errorHeaders:_*))((current, previous) => previous.union(current)).cache()

    val totalTest = testingResults.count()
    val testWitIssues = testingResults.filter($"result" === false)
    val totalIssueTest = testWitIssues.count()

    val testReport = testingResults.orderBy($"result").collect().map(testingEntry =>
      if (testingEntry.getBoolean(1)) {
        s"SUCCESS: ${testingEntry.getString(0)}\n"
      } else {
        s"FAIL: ${testingEntry.getString(0)}\n"
      }).foldLeft("")((current,accumulated) => s"$accumulated$current")

    logger.info(s"Test all - Regular Flow" )
    logger.info(testReport)
    logger.info(s"Executed $totalTest test; SUCCESS: ${totalTest - totalIssueTest} & FAIL $totalIssueTest" )
    assert(0 == totalIssueTest, "The number of test failed must be 0")
  }


}