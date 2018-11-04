package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool._
import com.github.marino_serna.parallel_tool_example.commons.{Commons, Utils}
import org.apache.spark.sql._

class ClassWithLogic1(utils:Utils) extends Commons{

  /**
    * Every method like “(parallelTool:ParallelTool):DataFrame” in a class that is executed will be executed
    * any other method will be ignored.
    *
    * New data can be add using any mechanism, the use of storage here is for convenience.
    *
    * Store temporal will delete the output of this method after the application is completed.
    *
    * Priority execution is the time that the method takes to be completed.
    * The suggestion is take this value in milliseconds form the logs of the application once the application is finished
    * and the amount of data is like in production.
    * The value will help the application to prioritize the execution of some methods.
    * Using a wrong value will not generate and error, only to delay de application.
    * The value doesn't need to be updated unless the application changes or the volume of data change significantly.
    *
    * @param parallelTool Allows the method to access the output of other methods already executed.
    * @return DataFrame with the result of the method
    */
  @Store(temporal = true)
  @PriorityExecution(expectedExecutionTime = 200)
  def processRawTableTemporalOutput(parallelTool:ParallelTool):DataFrame ={
    val dfTable1 = utils.storage.read(schema1,table1)
    utils.wasteTime(dfTable1,dfTable1)
  }


  /**
    * If store is defined the output of the method will be persist after the application.
    *
    * To increase the flexibility of this tool every project should create an implementation of the object storage
    * that implementation should be adapted to the specific requirements of the project.
    *
    * One of these implementations is provided by ParallelTool, that is an example for databases, that is what we will use here.
    *
    * The object store is required to have two methods, read and write.
    * After write a DataFrame must be possible to read that DataFrame using the read method.
    * The recommendation is to use “sourceName” as schema/database/path and “elementName” as name of the file or table
    *
    * The field partitions is defined to provide information of what fields will be used for partitioning the table
    * or distribute the files.
    * Using Partitions is optional and if used the implementation must be in the write method of the class store in use.
    *
    * @param parallelTool Allows the method to access the output of other methods already executed.
    * @return DataFrame with the result of the method
    */
  @Store(schema = schema1, name = table3, partitions = Array("field2"))
  @PriorityExecution(expectedExecutionTime = 192)
  def processRawTableOutputToTable(parallelTool:ParallelTool):DataFrame ={
    val dfTable2 = utils.storage.read(schema1,table2)
    utils.wasteTime(dfTable2,dfTable2)
  }

  /**
    * To read the output of any other method is required the use of parallelTool:
    * parallelTool.get("function name", instance of the class where the method is located)
    *
    * It is important to add an annotation of "DependenceOf" with the name of the method.
    * That annotation will avoid the application to get block waiting for a method that is not running,
    * especially important if the number of methods is high.
    * This will be use with the annotation PriorityExecution to calculate the critical path of the application
    *
    * @param parallelTool Allows the method to access the output of other methods already executed.
    * @return DataFrame with the result of the method
    */
  @DependenceOf(dependencies = Array("processRawTableTemporalOutput", "processRawTableOutputToTable"))
  @Store(schema = schema1, name = table4)
  @PriorityExecution(expectedExecutionTime = 2914)
  def processOutputFromOtherMethods(parallelTool:ParallelTool):DataFrame ={
    val dfTableTemporal = parallelTool.get("processRawTableTemporalOutput", this)
    val dfTable3 = parallelTool.get("processRawTableOutputToTable", this)
    utils.wasteTime(dfTableTemporal,dfTable3)
  }

}
