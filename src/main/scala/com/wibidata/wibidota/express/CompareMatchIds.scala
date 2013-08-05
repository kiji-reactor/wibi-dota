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

import com.twitter.scalding.{TextLine, JsonLine, Csv, Args}
import org.kiji.express.{EntityId, KijiJob}
import cascading.pipe.joiner.OuterJoin
import org.kiji.express.DSL.{KijiInput, Column}

/**
 * Job that joins two groups of match_ids together and return all
 * cases where the two pipes contained differing ids.
 *
 * @param args
 * -- file, file containing a match in text form per a line. If missing table is used intsead
 * -- table, if present used instead of file, a table with match_ids as keys
 * -- occupied_column, needed if table is set, row in the table that is occupied
 * -- json_file, file containing json lines with a match_id : long element
 * -- output, the location to dump the output
 */
class CompareMatchIds(args: Args) extends KijiJob(args) {

  val file = args.getOrElse("file", "");

  val pipe1 = file match {
    case "" => KijiInput(args("table-uri"))(
      Map(
        Column(args("occupied_column")) -> 'oc
      )
    ).mapTo('entityId -> 'matchId){entityId: EntityId => entityId.apply(0).asInstanceOf[Long]}
    case _  => TextLine(args("table-file")).mapTo('matchId){line : String => line.toLong}
  }

  val pipe2 =  JsonLine(args("json_file"), 'match_id).project('match_id).mapTo('match_id -> 'hMatchId){
    match_id : String => match_id.toLong
  }

  pipe2.joinWithSmaller('matchId -> 'hMatchId, pipe1, joiner = new OuterJoin)
        .filter('matchId, 'hMatchId){
    ids : (Any, Any) => ids._1 == null || ids._2 == null}
    .write(Csv(args("outfile")))
}
