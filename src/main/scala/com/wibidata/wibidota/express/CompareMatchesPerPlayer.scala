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

import com.twitter.scalding._
import org.kiji.express.{EntityId, KijiSlice}
import scala.collection.JavaConversions._
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._
import cascading.pipe.joiner.OuterJoin
import com.twitter.scalding.Csv
import com.twitter.scalding.JsonLine

/**
 * Express job that checks the dota_players table loaded succsefully.
 * Cross checks players and number of matches those players played with
 * what is found in the rawjson. Requires two map reduce jobs.
 *
 * @param args, command line arguements with flags:
 * --player_table the location of the dota_players table
 * --json_file the location of the json file
 * --outfile the file to dump any misatches
 */
/*
 * Note, this will give inaccurate counts if there are duplicates in the
 * raw json files, which has happened to me before.
 * This currently runs three jobs, I have found it effective to manually
 * split up the jobs and save the intermediate results.
 */
class CompareMatchesPerPlayer(args : Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++ Map(
    "hbase.client.scanner.caching" -> "100"
  )

  val jsonCounts =
    JsonLine(args("json_file"), 'players)
    .project('players)
    .flatMapTo('players -> 'jsonAccountId){
    players : java.util.ArrayList[java.util.Map[String, Number]] =>
      players.iterator().map(x =>
      x.get("account_id")).filter(x => x != null && x.longValue() != 4294967295L).toIterable
    }.groupBy('jsonAccountId){_.size('jsonCount)}

  val kijiCounts = KijiInput(args.getOrElse("player_table", PlayerTable))(
    Map(
      Column("data:match_id", all) -> 'matches
    )
  ).map('matches -> 'matches){matches : KijiSlice[Long] => matches.cells.size}
  .map('entityId -> 'entityId){id : EntityId => id(0)}
  .rename('matches -> 'kijiCounts)
  .rename('entityId -> 'kijiAccountId)

  kijiCounts.joinWithSmaller('kijiAccountId -> 'jsonAccountId, jsonCounts, joiner = new OuterJoin())
    .filter('kijiAccountId, 'kijiCounts, 'jsonAccountId, 'jsonCounts)
  {x : (Long, Int, Long, Int) => x._1.equals(null) || x._3.equals(null) || x._2 != x._4}
    .write(Csv(args("outfile")))
}
