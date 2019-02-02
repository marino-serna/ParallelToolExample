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

    prepareDataBase(utils)
  }

  def prepareDataBase(utils: Utils):Unit = {
    val listOfSchemas:List[String] = sqlCreateDataBase()
    listOfSchemas.par.map(schemaToCreate=>utils.spark.sql(schemaToCreate))


    val createsAndDropsTables = scala.io.Source.fromFile(s"src/test/resources/create/create_tables.sql").mkString
      .replaceAllLiterally("${variableDefinedInTheClusterWithThePathForThisEnvironment}",pathSchemaTest)
      .replace("';'","'#semicolon#'")
      .split(";")
      .map(_.replace("#semicolon#",";")).toList

    createsAndDropsTables.filter(_ . contains("CREATE TABLE")).par.map(utils.spark.sql)
  }

  def sqlCreateDataBase():List[String] = {
    s"""CREATE DATABASE IF NOT EXISTS $schema1""" ::
      s"""CREATE DATABASE IF NOT EXISTS $schema2""" ::
      Nil
  }

}
