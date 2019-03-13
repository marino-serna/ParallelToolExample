package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool._
import com.github.marino_serna.parallel_tool_example.commons.{Commons, Utils}
import org.apache.spark.sql._

class ClassWithLogic3(utils:Utils) extends Commons{


  /**
    * This is an example where an extra layer of parallelism is added to the execution,
    * inside the parallel execution of all the main methods that is triggered by “parallelTool.startApplication”,
    * one of the threads of execution will create new threads.
    *
    * To use this functionality, the DataFrames should:
    * -	not be use in any other place of the application (otherwise will be wiser to calculate as a regular parallelTool method, and avoid recalculate the value)
    * -	The size of the DataFrames is small enough to feed in memory
    *
    * This option is slightly faster, if the re-execution of a DataFrame is not needed
    *
    * This method has two versions, the default version is the simpler and recommended, but an alternative is provided.
    *
    * @param parallelTool Allows the method to access the output of other methods already executed.
    * @return DataFrame with the result of the method
    */
  @DependenceOf(dependencies = Array("processRawTableTemporalOutput", "processRawTableOutputToTable"))
  @Store(schema = schema2, name = table6)
  def processParallelWithoutDependencies(parallelTool:ParallelTool):DataFrame ={
    val classWithLogic1 = new ClassWithLogic1(utils)
    val dfTable1:DataFrame = utils.storage.read(schema1,table1)
    val dfTable2:DataFrame = utils.storage.read(schema1,table2)
    val dfTableTemporal:DataFrame = parallelTool.get("processRawTableTemporalOutput", classWithLogic1)
    val dfTable3:DataFrame = parallelTool.get("processRawTableOutputToTable", classWithLogic1)

    //These four  calls to methods will be executed in parallel
    val functionsToExecute =
      ("likeWasteTime", dfTable1 :: dfTable2 :: 3.asInstanceOf[Integer] :: Nil) ::
      ("likeWasteTime", dfTable1 :: dfTableTemporal :: new Integer(2) :: Nil) ::
      ("likeWasteTime", dfTable3 :: dfTable2 :: Nil) ::
      ("likeWasteTime", dfTable3 :: dfTableTemporal :: Nil) ::
      Nil
    val (resultDF1 ::
      resultDF2 ::
      resultDF3 ::
      resultDF4 ::
      _ ) = parallelTool.parallelNoDependencies(this, functionsToExecute)

    //Once the step is completed the results can be use,
    // in this example are used for a second round of executions that are dependent of the previous executions.
    val moreFunctionsToExecute =
      ("likeWasteTime", resultDF1 :: resultDF2 :: Nil) ::
      ("likeWasteTime", resultDF3 :: resultDF4 :: Nil) ::
       Nil
    val (resultDF5 ::
      resultDF6 ::
      _ ) = parallelTool.parallelNoDependencies(this, moreFunctionsToExecute)

    //Now using the output of the previous executions
    utils.wasteTime(resultDF5,resultDF6)
  }

  def likeWasteTime(df1:DataFrame,df2:DataFrame, duration:Integer):DataFrame ={
    utils.wasteTime(df1,df2, duration.intValue())
  }

  def likeWasteTime(df1:DataFrame,df2:DataFrame):DataFrame ={
    utils.wasteTime(df1,df2)
  }

}
