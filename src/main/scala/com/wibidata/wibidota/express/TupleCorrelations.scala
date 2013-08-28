/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
