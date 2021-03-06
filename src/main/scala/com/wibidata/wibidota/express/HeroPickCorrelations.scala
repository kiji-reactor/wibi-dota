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

import com.twitter.scalding.{Args, Csv, Mode}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._
import scala.Some
import org.kiji.express.{AvroRecord, EntityId, KijiSlice}


/**
 * Caculates the correlation coefficients between the number of games people have played
 * with given heroes.
 *
 * @param args command line arguements including
 * --num_players, number of players being used
 * --player_table, string of the dota_players location (defualts to wibidota/dota_players)
 * --output, where to dump the output, defaults to "pick_correlations"
 * --hero_ids, range hero ids can take, defaults to 1-104
 * --size_filter, filter out hero picks from players with less than ten games
 */
class HeroPickCorrelations(args: Args) extends KijiJob(args) with CalcCorrelations {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = {
    super.config(mode) ++ Map("hbase.client.scanner.caching" -> "100")
  }

  val numPlayers = args("num_players").toInt

  val input = KijiInput(args.getOrElse("player_table", PlayerTable))(
    Map (
      Column("data:player", versions = all) -> 'player,
      Column("match_derived_data:real_match", versions = all) -> 'rm
    ))
    .filter('rm){rm : KijiSlice[Double] => rm.cells.size > args("size_filter").toInt}
    .flatMapTo(('entityId, 'player, 'rm) -> ('hero, 'id, 'game)){
    f : (EntityId, KijiSlice[AvroRecord], KijiSlice[Double]) =>
      val realMatchesTimes = f._3.cells.map(x => x.version).toSet
      f._2.cells.filter(x => realMatchesTimes.contains(x.version))
        .map(x => (x.datum("hero_id").asInt())).groupBy(identity).map(x => (x._1, f._1(0), x._2.size))
  }.rename('hero -> 'vec).rename('id -> 'index).rename('game -> 'val)

  val vecStr = args.getOrElse("hero_ids", "1-104")
  val vectors : Option[Seq[Int]] = {val r = vecStr.split("-").map(x => x.toInt); Some(r(0) to r(1))}
  correlations(input, numPlayers, vectors).write(Csv(args.getOrElse("output", "pick_correlations")))
}
