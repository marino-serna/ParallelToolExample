package com.github.marino_serna.parallel_tool_example.commons

import com.github.marino_serna.parallel_tool.DataBaseStorage
import com.github.marino_serna.parallel_tool_example.commons.Schema1Tables._
import org.apache.log4j.Logger
import org.apache.spark.sql._

import scala.language.postfixOps

class Utils(pathSchemaToUse:String) extends Commons {
  val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
  import spark.implicits._

  val storage:DataBaseStorage = new DataBaseStorage(spark,pathSchemaToUse)

  def keyToSeq(groupingKey:List[ColumnName]):Seq[String] ={
    groupingKey.map(_.toString())
  }


  def normalizeColumnNameDF(df:DataFrame):DataFrame = {
    df.schema.fieldNames.foldLeft(df)(
      (elementValue,originalColumnName) => elementValue.withColumnRenamed(originalColumnName, normalizeColumnName(originalColumnName)))
  }

  def sortDataFrameByColumnName(df:DataFrame):DataFrame = {
    val sortColNames = df.schema.fieldNames.toList.sorted.map(name => $"$name")
    df.select(sortColNames:_*)
  }

  def unionByColumnNameNormalized(df:DataFrame,df2:DataFrame):DataFrame = {
    unionByColumnName(normalizeColumnNameDF(df), normalizeColumnNameDF(df2))
  }

  def unionByColumnName(df:DataFrame,df2:DataFrame):DataFrame = {
    if(df.schema.size != df2.schema.size){
      MyLogger.error(s"Union of 2 DataFrame with different size ${df.schema.size} != ${df2.schema.size}")
      df.printSchema()
      df2.printSchema()
    }
    val sortColNames = df.schema.fieldNames.toList.sorted.map(name => $"$name")
    df.select(sortColNames:_*).union(df2.select(sortColNames:_*))
  }

  def isNormalizedRequired(name:String):Boolean = {
    val columnNameClean:List[String] = invalidTableNameCharacters:::unwantedTableNameCharacters
    val hasForbiddenCharacter = columnNameClean.exists(p => name.contains(p))
    val startByNum = name.toList match {
      case num :: _ if Character.isDigit(num) =>  true
      case _ => false
    }
    hasForbiddenCharacter || startByNum
  }

  def normalizeColumnName(value:String):String={
    //create valid names for columns
    val columnNameCandidate = if(value != null){value.toLowerCase.replace(" ","_")}else{""}
    val columnNameClean:String = (invalidTableNameCharacters:::unwantedTableNameCharacters)
      .foldLeft(columnNameCandidate)((name,characterToClean) => name.replaceAll(characterToClean,""))
    val columnName = (columnNameClean.toList match {
      case num :: _ if Character.isDigit(num) =>  s"_$columnNameClean"
      case _ => columnNameClean
    }).replace(".0","") //There is no decimals, if that change replace by "_", having "." in the column name fails some times.
    columnName
  }

  /**
    * This method will consume some time, we use this instead of implementing real logic for the examples.
    * @param df1 First DataFrame
    * @param df2 Second DataFrame
    * @param duration number of times the function will be executed and consuming time
    * @return
    */
  def wasteTime(df1:DataFrame,df2:DataFrame, duration:Int=3):DataFrame ={
    duration match {
      case 0 => df1
      case _ =>
        val dfRes = df1
            .drop("field1","field3","field5")
        .join(df2
          .drop("field2","field4")
          ,Seq("key"),"inner")

      wasteTime(dfRes,df2,duration-1)
    }
  }

  /**
    * This method will consume some time, we use this instead of implementing real logic for the examples.
    * @param ds1 First DataSet
    * @param ds2 Second DataSet
    * @param duration number of times the function will be executed and consuming time
    * @return
    */
  def wasteTimeTyped(ds1:Dataset[Table1], ds2:Dataset[Table3], duration:Int=3):Dataset[Table1] ={
    duration match {
      case 0 => ds1
      case _ =>
        val dfRes = ds1
          .joinWith(ds2,ds1("key") === ds2("key"),"inner")
          .map{case (table1,table3) =>
            Table1(table1.key,table3.field1,table1.field2,table3.field3,table1.field4, table3.field5)}
        dfRes.show()

        wasteTimeTyped(dfRes,ds2,duration-1)
    }
  }
}



private object MyLogger{ // this object fix the problem of Logger not been serializable
  private val logger: Logger = Logger.getLogger("Utils")
  def error(message:String):Unit={ logger.error(message)}
  def info(message:String):Unit={ logger.info(message)}
}