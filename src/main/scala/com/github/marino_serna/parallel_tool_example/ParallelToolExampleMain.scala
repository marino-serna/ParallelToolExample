package com.github.marino_serna.parallel_tool_example

import com.github.marino_serna.parallel_tool._
import com.github.marino_serna.parallel_tool_example.commons.{Commons, Utils}
import org.apache.log4j.Logger

object ParallelToolExampleMain extends Commons{

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    args.length match{
      case x if 0 to 1 contains x =>
        val defaultParams: List[String] =  executionModeFull :: Nil
        val paramsToUse = args.toList ::: defaultParams.drop(x)
        regularExecution(paramsToUse)
      case _ => wrongParameters(args.toList)
    }
  }

  def regularExecution(inputParams: List[String]):Unit= {
    val utils = new Utils(pathSchema)
    val executionMode ::  specificAttributes = inputParams
    executionMode.toLowerCase match{
      case `executionModeFull` =>
        fullExecution(utils)
      case `executionModeSelective` =>
        selectiveExecution(utils)
      case _=>
        wrongParameters(inputParams)
    }
  }

  /**
    * All the classes involved in the execution are pass as a parameter to the method startApplication of the ParallelTool
    * This will trigger the execution of every method with the signature:
    * def anyMethodName(parallelTool:ParallelTool):DataFrame ={
    *
    * @param utils Use for convenience to have access to some useful objects and methods
    * @return Return the instance of parallelTool that was used for the execution,
    *         useful if some other task is perform after the execution, like the unit tests
    */
  def fullExecution(utils:Utils):ParallelTool = {
    logger.info(s" Full execution requested")
    val parallelTool = new ParallelTool(utils.spark, utils.storage, storePrioritySchema=schema1, storePriorityTable = "priority_example")

    val classWithLogic1 = new ClassWithLogic1(utils)
    val classWithLogic2 = new ClassWithLogic2(utils)
    val classWithLogic3 = new ClassWithLogic3(utils)
    parallelTool.startApplication(classWithLogic1 :: classWithLogic2 :: classWithLogic3 :: Nil)
    logger.info(s" Full execution ended")

    parallelTool
  }

  /**
    * Sometimes only is required the execution of some methods, here you can select only the methods that you need to run.
    *
    * To perform this execution, you must be careful with the dependencies.
    *
    * If a method is requested and one dependence is not requested as part of the execution,
    * the application will assume that was already ran in a different execution and de data will be available and correct.
    * If the output of that method is temporal the execution will fail,
    * because the application won’t be able to locate the output of the method.
    *
    * @param utils Use for convenience to have access to some useful objects and methods
    * @return Return the instance of parallelTool that was used for the execution,
    *         useful if some other task is perform after the execution, like the unit tests
    */
  def selectiveExecution(utils:Utils): ParallelTool = {
    logger.info(s" Selective execution requested")
    val parallelTool = new ParallelTool(utils.spark, utils.storage)

    val classWithLogic1 = new ClassWithLogic1(utils)
    val classWithLogic3 = new ClassWithLogic3(utils)

    val functionsToExecute:Map[Object, List[String]] = Map(
      classWithLogic1 -> ("processRawTableTemporalOutput" :: "processRawTableOutputToTable" :: Nil),
      classWithLogic3 -> ("processParallelWithoutDependencies" :: Nil))

    parallelTool.startApplication(functionsToExecute)
    parallelTool
  }

  def wrongParameters(inputParams: List[String]):Unit = {
    logger.error(s" Invalid number of parameters (${inputParams.length}: ${inputParams.mkString(" ")}).")
    logger.info(s"Options are: <(${executionMode.mkString("/")})>")
    System.exit(1)
  }

}
