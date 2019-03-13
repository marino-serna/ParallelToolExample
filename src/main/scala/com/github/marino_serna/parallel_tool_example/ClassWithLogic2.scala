package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool.{ParallelTool, Store, _}
import com.github.marino_serna.parallel_tool_example.commons.{Commons, Utils}
import org.apache.spark.sql.DataFrame

class ClassWithLogic2(utils:Utils) extends Commons {

  /**
    * A method can read from any method that will be executed,
    * it is not important where the methods that will be read are located, in its own class,
    * in a different class or in multiple classes.
    *
    * What is important is to be careful not to generate a loop with the main methods.
    * If two main methods are reading from each other the application will be block.
    *
    * @param parallelTool Allows the method to access the output of other methods already executed.
    * @return DataFrame with the result of the method
    */
  @DependenceOf(dependencies = Array("processRawTableTemporalOutput", "processRawTableOutputToTable"))
  @Store(schema = schema2, name = table5)
  def processOutputFromOtherMethodsInADifferentClass(parallelTool:ParallelTool):DataFrame ={
    val classWithLogic1 = new ClassWithLogic1(utils)
    val dfTableTemporal = parallelTool.get("processRawTableTemporalOutput", classWithLogic1)
    val dfTable3 = parallelTool.get("processRawTableOutputToTable", classWithLogic1)
    utils.wasteTime(dfTableTemporal,dfTable3)
  }

}
