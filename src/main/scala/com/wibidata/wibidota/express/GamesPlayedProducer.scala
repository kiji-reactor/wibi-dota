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

import com.twitter.scalding.{Args}
import org.kiji.express.{KijiSlice}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._

class GamesPlayedProducer(args : Args) extends KijiJob(args) {
  KijiInput(args.getOrElse("player_table", PlayerTable))(
    Map(
      MapFamily("match_derived_data", "real_match", all) -> 'real_matches
    )
  ).map('real_matches -> 'count){match_ids : KijiSlice[Double] => match_ids.cells.count({
      x => x.datum != 1.0
    })
  .toDouble}
     .discard('real_matches).insert('name, "real_matches_played")
  .write(KijiOutput(args("table-uri"))(Map(MapFamily("derived_data")('name) -> 'count)))
}
