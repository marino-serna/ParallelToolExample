package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool.ParallelTool
import com.github.marino_serna.parallel_tool_example.commons.Utils
import com.github.marino_serna.parallel_tool_example.commons.UtilsTest.TestResult
import org.apache.spark.sql.{DataFrame, Dataset}

class TestFullExecutionClassWithLogic3(utils: Utils, parallelTool:ParallelTool) {
  import utils.spark.implicits._

  def startTest():List[Dataset[TestResult]]={
    val functionsToExecute =
          ("testProcessParallelWithoutDependencies", Nil) ::
      Nil

        parallelTool.parallelNoDependencies(this, functionsToExecute).map(_.as[TestResult])
  }

  def testProcessParallelWithoutDependencies(): Dataset[TestResult] = {
    import utils._
    import utils.spark.implicits._

    val classWithLogic1 = new ClassWithLogic1(utils)
    val classWithLogic3 = new ClassWithLogic3(utils)

    val dfInput1 = utils.storage.read(schema1,table1)
    val dfInput3 = parallelTool.get("processRawTableTemporalOutput", classWithLogic1)
    val dfOutput = parallelTool.get("processParallelWithoutDependencies", classWithLogic3)

    //any logic and validations
    val countInput = dfInput1.count()
    val countInput3 = dfInput3.count()
    val countOutput = dfOutput.count()

    ((s"processParallelWithoutDependencies => Counts: $countInput == $countOutput && $countInput3  == $countOutput",
      countInput == countOutput && countInput3  == countOutput) ::
      Nil).toDF(errorHeaders: _*).as[TestResult]
  }
}