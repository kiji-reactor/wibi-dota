/* Copyright 2013 WibiData, Inc.
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

import com.twitter.scalding.{Mode, Csv, Args}
import org.kiji.express._
import org.kiji.express.DSL._

/**
 * Histogram generates field, value, start, end, count tuples from a Kiji table column where
 * field is the column qualifier.
 * value is the value in the column, start and end define a timestamp range, and count is the number
 * of occurrences of value that occured in column field with timestamp between [start-end).
 * Supports up to four columns
 *
 * @param args, arguements that includes the following flags:
 * --table the Kiji table to use
 * --columns the columns within the table to use, group type only, comma deliminated
 * --interval the bucket size in seconds
 */
class MultHistogram(args: Args) extends KijiJob(args){

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++ Map("hbase.client.scanner.caching" -> "100")

def toKeys(f : Product, interval : Int) : Iterable[(String, String, String, String)] = {
  f.productIterator.map({
    value =>
      val slice = value.asInstanceOf[KijiSlice[Any]]
      val slot =  slice.getFirst().version / interval
      (slice.getFirst().qualifier, slice.getFirstValue().toString,
        (interval * slot).toString,
         (interval * (slot + 1)).toString())
  }
  ).toIterable
}

  val sampleInterval = Integer.parseInt(args("interval"))
  val m = args("columns").split(",").map(c => (Column(c, all), Symbol(c))).toMap
  KijiInput(args("table"))(m)
  // Sadly scalding expects us to know the type of incoming tuples so we are forced to use
  // a switch statement
  m.size match {
    case 1 => KijiInput(args("table"))(m).flatMapTo((m.values) -> ('field, 'value, 'start, 'end))
    {m : (KijiSlice[Any]) => toKeys(Tuple1(m), sampleInterval)}.groupBy('field, 'value, 'start, 'end)
    {_.size}.write(Csv(args("outfile")))
    case 2 => KijiInput(args("table"))(m).flatMapTo((m.values) -> ('field, 'value, 'start, 'end))
    {m : (KijiSlice[Any], KijiSlice[Any]) => toKeys(m, sampleInterval)}.groupBy('field, 'value, 'start, 'end)
    {_.size}.write(Csv(args("outfile")))
    case 3 => KijiInput(args("table"))(m).flatMapTo((m.values) -> ('field, 'value, 'start, 'end))
    {m : (KijiSlice[Any], KijiSlice[Any], KijiSlice[Any]) => toKeys(m, sampleInterval)}.
      groupBy('field, 'value, 'start, 'end){_.size}.write(Csv(args("outfile")))
    case 4 => KijiInput(args("table"))(m).flatMapTo((m.values) -> ('field, 'value, 'start, 'end))
    {m : (KijiSlice[Any], KijiSlice[Any], KijiSlice[Any], KijiSlice[Any]) => toKeys(m, sampleInterval)}
      .groupBy('field, 'value, 'start, 'end){_.size}.write(Csv(args("outfile")))
    case _ => throw new RuntimeException
  }
}
