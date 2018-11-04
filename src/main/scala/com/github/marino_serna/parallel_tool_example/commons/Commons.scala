package com.github.marino_serna.parallel_tool_example.commons

import scala.language.postfixOps

/**
  * Object to add common variables
  */
trait Commons{

  val pathSchema = "gs://" //Example for GCP (google cloud)
  val pathSchemaTest = "src/test/resources/"

  //characters that are regular expressions, like {}() must be escaped: "\\{" or """\{"""
  val invalidTableNameCharacters:List[String] =  " "::","::";"::"\\{"::"\\}"::"\\("::"\\)"::"\n"::"\t"::"="::Nil
  val unwantedTableNameCharacters:List[String] = "'" :: "|" :: Nil

  val colNameOptions:List[Boolean] = true :: false :: Nil
  val replaceName :: concatenateName :: _ = colNameOptions


  val executionMode:List[String] = "full_execution" :: "selective_execution" :: Nil
  val executionModeFull :: executionModeSelective :: _ = executionMode


  val errorHeaders: List[String] = "message" :: "result"::Nil

  /**
    * Schemas
    */

  final val schema1 = "schema1"
  final val schema2 = "schema2"

  /**
    * Tables
    */

  final val table1 = "table1"
  final val table2 = "table2"

  final val table3 = "table3"
  final val table4 = "table4"
  final val table5 = "table5"
  final val table6 = "table6"

}
