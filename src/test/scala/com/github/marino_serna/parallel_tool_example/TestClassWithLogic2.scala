package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool.ParallelTool
import com.github.marino_serna.parallel_tool_example.commons.Utils
import org.apache.spark.sql.DataFrame

class TestClassWithLogic2(utils: Utils, parallelTool:ParallelTool) {

  val classWithLogic1 = new ClassWithLogic1(utils)
  val classWithLogic2 = new ClassWithLogic2(utils)

  def startTest():List[DataFrame]={
    val functionsToExecute =
      ("testProcessOutputFromOtherMethodsInADifferentClass", Nil) ::
        Nil

    parallelTool.parallelNoDependencies(this, functionsToExecute)
  }

  def testProcessOutputFromOtherMethodsInADifferentClass(): DataFrame = {
    import utils._
    import utils.spark.implicits._

    val dfInput1 = parallelTool.get("processRawTableTemporalOutput", classWithLogic1)
    val dfOutput = parallelTool.get("processOutputFromOtherMethodsInADifferentClass", classWithLogic2)

    //any logic and validations
    val countInput = dfInput1.count()
    val countOutput = dfOutput.count()

    ((s"testProcessOutputFromOtherMethodsInADifferentClass => Counts: $countInput == $countOutput",
      countInput == countOutput) ::
      Nil).toDF(errorHeaders: _*)
  }

}