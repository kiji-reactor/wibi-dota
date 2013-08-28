package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, Args}
import org.kiji.express.flow.KijiJob

/**
 * Calculates the nonZeroCorrelation (see CalcCorrelation( of tuples that are stored
 * in a csv of the form (index, vec, val, size).. Filters the tuples by the size
 * field.
 *
 * @param args, arguements including
 * --input location where the tuples are stored
 * --output location to dump the result
 * --size_filter, size to filter the tupels on
 */
class TupleCorrelations(args: Args) extends KijiJob(args) with CalcCorrelations {

  val input = Csv(args("input"), ",", ('index, 'vec, 'val, 'size))
  .filter('size){size : Int => size > args("size_filter").toInt}.discard('size)

  nonZeroCorrelations(input).write(Csv(args("output")))
}
