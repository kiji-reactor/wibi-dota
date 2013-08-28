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

import com.twitter.scalding.{Mode, Args}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._

/**
 * Add a row number column to the derived_data family for each player who has played
 * at least one real match. Intended to facilitate use of Scalding's matrix library.
 */
class AddRowNumbers(args: Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] =
    super.config(mode) ++ Map ("hbase.client.scanner.caching" -> "100")

  var row = 0.0

  KijiInput(args.getOrElse("table", PlayerTable))(
    Map (
      Column("match_derived_data:real_match", latest) -> 'data
    )
  ).discard('data).groupAll(gs => gs.reducers(1)).mapTo('entityId -> 'row){
    id : Object => row += 1.0; row
  }
    .insert('row_number, "row_number")
    .write(KijiOutput(args.getOrElse("table", PlayerTable))(
    Map(
      (MapFamily("derived_data")('row_number) -> 'row)
    )))
}
