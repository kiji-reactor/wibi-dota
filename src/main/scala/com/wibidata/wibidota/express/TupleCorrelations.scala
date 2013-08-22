package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, Args}
import org.kiji.express.flow.KijiJob

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 8/21/13
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
class TupleCorrelations(args: Args) extends KijiJob(args) with CalcCorrelations {

  val input = Csv(args("input"), ",", ('vec, 'index, 'val))

  nonZeroCorrelations(input).write(Csv(args("output"), writeHeader = true))

}
