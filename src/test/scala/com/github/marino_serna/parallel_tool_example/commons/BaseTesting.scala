package com.github.marino_serna.parallel_tool_example.commons

import com.github.marino_serna.parallel_tool.ParallelTool
import com.github.marino_serna.parallel_tool_example.{ParallelToolExampleMain, TestFullExecutionClassWithLogic1, TestFullExecutionClassWithLogic2, TestFullExecutionClassWithLogic3}
import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class BaseTesting extends FunSuite with BeforeAndAfterAll with Commons{

  lazy val utils: Utils = UtilsTest
  lazy val parallelTool:ParallelTool = ParallelToolExampleMain.fullExecution(utils)

  lazy val testClassWithLogic1 = new TestFullExecutionClassWithLogic1(utils, parallelTool)
  lazy val testClassWithLogic2 = new TestFullExecutionClassWithLogic2(utils, parallelTool)
  lazy val testClassWithLogic3 = new TestFullExecutionClassWithLogic3(utils, parallelTool)

  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.ALL)

  override def beforeAll() : Unit = {
    super.beforeAll()
    System.setSecurityManager(null)
    System.setProperty("spark.master", "local")
    System.setProperty("org.apache.parquet.handlers", "java.util.logging.ConsoleHandler")
    System.setProperty("java.util.logging.ConsoleHandler.level", "SEVERE")
    utils.spark.conf.set("spark.sql.shuffle.partitions",  1) //ONLY use for unit testing. Helps speeding up the test with small data
  }

  def parseSQLFile(pathFile:String, locationKey:String, locationValue:String):List[String] = {
    scala.io.Source.fromFile(pathFile).mkString
      .replaceAllLiterally(locationKey,locationValue)
      .replace("';'","'#semicolon#'")
      .replace(";\"","#semicolon1#")
      .replace(";'","#semicolon2#")
      .split(";")
      .map(_.replace("#semicolon2#",";'"))
      .map(_.replace("#semicolon1#",";\""))
      .map(_.replace("#semicolon#",";")).toList
  }

  def prepareDataBase(dataBaseName:String):Unit = {

    val createAndDropsDataBases = parseSQLFile(s"src/test/resources/create/$dataBaseName/create_databases.sql",environmentVariableForDB,pathSchemaTest)

    // Drop databases is nor required but helps ensuring that every test is not affected by tests from previous executions.
    createAndDropsDataBases.filter(_ . contains("DROP DATABASE")).par.map(utils.spark.sql)

    createAndDropsDataBases.filter(_ . contains("CREATE DATABASE")).par.map(utils.spark.sql)

    val createsAndDropsTables = parseSQLFile(s"src/test/resources/create/$dataBaseName/create_tables.sql",environmentVariableForDB, pathSchemaTest)

    createsAndDropsTables.filter(_ . contains("CREATE EXTERNAL TABLE")).par.map(utils.spark.sql)
    createsAndDropsTables.filter(_ . contains("CREATE TABLE")).par.map(utils.spark.sql)
  }
}
