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

import com.twitter.scalding.{Csv, JsonLine, Args}
import org.kiji.express.{EntityId, KijiSlice, KijiJob}
import scala.collection.JavaConversions._
import org.kiji.express.DSL._
import cascading.pipe.joiner.OuterJoin

/**
 * Express job that checks the dota_players table loaded succsefully.
 * Cross checks players and number of matches those players played with
 * what is found in the rawjson. Requires two map reduce jobs.
 *
 * @param args, command line arguements with flags:
 * --table the location of the dota_players table
 * --json_file the location of the json file
 * --outfile the file to dump any misatches
 */
class CompareMatchesPerPlayer(args : Args) extends KijiJob(args) {

  val jsonCounts = JsonLine(args("json_file"), 'players)
    .project('players)
    .flatMapTo('players -> 'accountId){
    players : java.util.ArrayList[java.util.Map[String, Int]] =>
      players.iterator().map(
      x => x.get("account_id")).filter(x => !x.equals(null) && x != -1)
        .toStream
  }.groupBy('accountId){_.size('jsonCount)}

  val kijiCounts = KijiInput(args("table"))(
    Map(
      Column("data:match_id", all) -> 'matches
    )
  ).map('matches -> 'kijiCount){matches : KijiSlice[Long] => matches.cells.size}
  .discard('matches).map('entityId -> 'kijiAccountId){id : EntityId => id(0)}.discard('entityId)

  kijiCounts.joinWithSmaller('kijiAccountId -> 'accountId, jsonCounts, joiner = new OuterJoin())
    .filter('kijiAccountId, 'kijiCount, 'accountId, 'jsonCount)
  {x : (Long, Int, Long, Int) => x._1.equals(null) || x._3.equals(null) || x._2 != x._4}
    .write(Csv(args("outfile")))
}
