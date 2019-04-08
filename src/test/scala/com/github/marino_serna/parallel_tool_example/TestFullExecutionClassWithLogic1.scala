package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool.ParallelTool
import com.github.marino_serna.parallel_tool_example.commons.Utils
import com.github.marino_serna.parallel_tool_example.commons.UtilsTest.TestResult
import org.apache.spark.sql.Dataset


class TestFullExecutionClassWithLogic1(utils: Utils, parallelTool:ParallelTool) {
  import utils.spark.implicits._

  val classWithLogic1 = new ClassWithLogic1(utils)

  def startTest():List[Dataset[TestResult]]={
    println("In startTest")
    val functionsToExecute =
      ("testProcessRawTableTemporalOutput", Nil) ::
      ("testProcessRawTableOutputToTable", Nil) ::
      ("testProcessOutputFromOtherMethods", Nil) ::
        Nil

    parallelTool.parallelNoDependencies(this, functionsToExecute).map(_.as[TestResult])
  }

  def testProcessRawTableTemporalOutput(): Dataset[TestResult] = {
    import utils._
    import utils.spark.implicits._

    val dfInput = utils.storage.read(schema1,table1)
    val dfOutput = parallelTool.get("processRawTableTemporalOutput", classWithLogic1)

    //any logic and validations
    val countInput = dfInput.count()
    val countOutput = dfOutput.count()

    ((s"processRawTableTemporalOutput => Counts: $countInput == $countOutput",
      countInput == countOutput) ::
      Nil).toDF(errorHeaders: _*).as[TestResult]
  }

  def testProcessRawTableOutputToTable(): Dataset[TestResult] = {
    import utils._
    import utils.spark.implicits._

    val dfInput = utils.storage.read(schema1,table2)
    val dfOutput = parallelTool.get("processRawTableOutputToTable", classWithLogic1)

    //any logic and validations
    val countInput = dfInput.count()
    val countOutput = dfOutput.count()

    ((s"processRawTableOutputToTable => Counts: $countInput == $countOutput",
      countInput == countOutput) ::
      Nil).toDF(errorHeaders: _*).as[TestResult]
  }

  def testProcessOutputFromOtherMethods(): Dataset[TestResult] = {
    import utils._
    import utils.spark.implicits._

    val dfInput1 = parallelTool.get("processRawTableTemporalOutput", classWithLogic1)
    val dfOutput = parallelTool.get("processOutputFromOtherMethods", classWithLogic1)

    //any logic and validations
    val countInput = dfInput1.count()
    val countOutput = dfOutput.count()

    ((s"processOutputFromOtherMethods => Counts: $countInput == $countOutput",
      countInput == countOutput) ::
      Nil).toDF(errorHeaders: _*).as[TestResult]
  }

}