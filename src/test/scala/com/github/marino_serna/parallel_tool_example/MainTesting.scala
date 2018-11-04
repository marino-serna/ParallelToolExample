package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool.ParallelTool
import com.github.marino_serna.parallel_tool_example.commons.{Commons, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.junit.Assert
import org.scalatest.FunSuite

class MainTesting  extends FunSuite with Commons {

  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.ALL)

  System.setSecurityManager(null)
  System.setProperty("spark.master", "local")
//  sys.props("nousehive") = ""
  System.setProperty("org.apache.parquet.handlers", "java.util.logging.ConsoleHandler")
  System.setProperty("java.util.logging.ConsoleHandler.level", "SEVERE")


  /**
    * Unit test can be approach in different ways, this is only an example, but when testing methods
    * that use the object parallelTool to retrieve the inputs is important to be careful avoiding interferences
    * between executions of different test.
    *
    * In this example you can see a full execution of the application.
    * After the execution is completed some validations of each method are testing the specific functionality of the method
    * (input x will generate output Y).
    *
    * Since this application is an example the test are only counts, in a real application more useful test should be perform.
    */
  test("Full execution") {

    val utils: Utils = new Utils(pathSchemaTest)

    prepareDataBase(utils)

    val parallelTool:ParallelTool = ParallelToolExampleMain.fullExecution(utils)

    testFullExecution(utils, parallelTool)

  }

  def prepareDataBase(utils: Utils):Unit = {
    val listOfSchemas:List[String] = sqlCreateDataBase()
    listOfSchemas.par.map(schemaToCreate=>utils.spark.sql(schemaToCreate))


    val createsAndDropsTables = scala.io.Source.fromFile(s"src/test/resources/create/create_tables.sql").mkString
      .replaceAllLiterally("${variableDefinedInTheClusterWithThePathForThisEnvironment}",pathSchemaTest)
      .split(";")

    createsAndDropsTables.filter(_ . contains("DROP TABLE")).par.map(utils.spark.sql)
    createsAndDropsTables.filter(_ . contains("CREATE TABLE")).par.map(utils.spark.sql)
  }

  def sqlCreateDataBase():List[String] = {
    s"""CREATE DATABASE IF NOT EXISTS $schema1""" ::
    s"""CREATE DATABASE IF NOT EXISTS $schema2""" ::
    Nil
  }

  def testFullExecution(utils: Utils, parallelTool:ParallelTool): Unit ={
    import utils.spark.implicits._
    val testClassWithLogic1 = new TestClassWithLogic1(utils, parallelTool)
    val testClassWithLogic2 = new TestClassWithLogic2(utils, parallelTool)
    val testClassWithLogic3 = new TestClassWithLogic3(utils, parallelTool)

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
    Assert.assertEquals("The number of test failed must be 0", 0,totalIssueTest)
  }


}