package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool.ParallelTool
import com.github.marino_serna.parallel_tool_example.commons.Utils
import com.github.marino_serna.parallel_tool_example.commons.UtilsTest.TestResult
import org.apache.spark.sql.{DataFrame, Dataset}

class TestFullExecutionClassWithLogic2(utils: Utils, parallelTool:ParallelTool) {
  import utils.spark.implicits._

  val classWithLogic1 = new ClassWithLogic1(utils)
  val classWithLogic2 = new ClassWithLogic2(utils)

  def startTest():List[Dataset[TestResult]]={
    val functionsToExecute =
      ("testProcessOutputFromOtherMethodsInADifferentClass", Nil) ::
        Nil

    parallelTool.parallelNoDependencies(this, functionsToExecute).map(_.as[TestResult])
  }

  def testProcessOutputFromOtherMethodsInADifferentClass(): Dataset[TestResult] = {
    import utils._
    import utils.spark.implicits._

    val dfInput1 = parallelTool.get("processRawTableTemporalOutput", classWithLogic1)
    val dfOutput = parallelTool.get("processOutputFromOtherMethodsInADifferentClass", classWithLogic2)

    //any logic and validations
    val countInput = dfInput1.count()
    val countOutput = dfOutput.count()

    ((s"testProcessOutputFromOtherMethodsInADifferentClass => Counts: $countInput == $countOutput",
      countInput == countOutput) ::
      Nil).toDF(errorHeaders: _*).as[TestResult]
  }

}