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

import com.wibidata.wibidota.express.DefaultResourceLocations._
import com.twitter.scalding.{Csv, Mode, Args}
import org.kiji.express._
import com.wibidata.wibidota.DotaValues
import org.kiji.express.flow._

/**
 * Calculates the winrates of each hero across given time intervals and writes
 * that data to the heroes table.
 *
 * @param args, includes
 * -- interval the length of time in seconds to count win rates by
 *
 */
  class HeroWinRates(args: Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] =
    super.config(mode) ++ Map ("hbase.client.scanner.caching" -> "100")

  val interval = args("interval").toLong * 1000 // Convert seconds -> milliseconds

  KijiInput(args.getOrElse("matches_table", MatchesTable))(
    Map (
      MapFamily("derived_data", "real_match", latest) -> 'real_match,
      Column("data:player_data", versions = latest) -> 'players,
      Column("data:radiant_win", versions = latest) -> 'r_win
    )
  )
    // For each player emit a tuple indicating their hero, if they won or lost, and the time
    .flatMapTo(('players, 'real_match, 'r_win) -> ('win, 'hero_id, 'time)) {
    fields : (KijiSlice[AvroRecord], KijiSlice[Double], KijiSlice[Boolean]) =>
      if(!fields._2.getFirstValue().equals(null) && fields._2.getFirstValue() != 1.0){
        None
      }
      val rWin = fields._3.getFirstValue();
      val slot = interval * (fields._1.getFirst().version / interval)
      fields._1.getFirstValue()("players").asList.map({playerEle =>
        val player = playerEle.asRecord()
        val radiantPlayer = DotaValues.radiantPlayer(player("player_slot").asInt())
        if((radiantPlayer && rWin) || (!radiantPlayer && !rWin)){
          (1.0, player("hero_id").asInt().toLong, slot)
        } else {
          (0.0, player("hero_id").asInt().toLong, slot)
        }
      })
  }
    // Group by hero and time and averge to find hero winrates over a time periond
    .groupBy('hero_id, 'time){gb => (gb.size('games_played).
      average('win -> 'win_rate))}
    // Format everything so we can write it to the hero table
    .map('hero_id -> 'hero_id){hero_id : Long => EntityId(hero_id.toInt)}
    .rename('hero_id -> 'entityId)
    .map('games_played -> 'games_played){x : Long => x.toDouble}
    .insert('win_rate_column, "win_rate_" + (interval/1000).toString)
   .insert('games_played_column, "games_played_" + (interval/1000).toString)
  .write(KijiOutput(args.getOrElse("hero_table", HeroesTable), 'time)(
    Map(
      (MapFamily("data")('win_rate_column) -> 'win_rate),
      (MapFamily("data")('games_played_column) -> 'games_played)
  )))
}
