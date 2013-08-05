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

import org.kiji.express._
import com.twitter.scalding._
import org.kiji.express.DSL._

/**
 * Counts the cells in a kiji table column
 *
 * @param args, command line flags that include:
 *  --table, URI of the Kiji Table
 *  --column, a column in the table to count the cells for
 *  --output, place to dump the result
 */
class CountCells(args: Args) extends KijiJob(args) {
  KijiInput(args("table"))(Map (
    Column(args("column"), versions = all) -> 'data
  )).flatMapTo('data -> 'val){data : KijiSlice[Any] =>
    data.cells.map(x => x.datum)}.groupAll(_.size).write(Csv(args("output")))
}
