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

import com.twitter.scalding.{Csv, Mode, Args}
import com.wibidata.wibidota.DotaValues
import com.wibidata.wibidota.express.DefaultResourceLocations._
import org.kiji.express.flow.{KijiJob, Column, KijiInput, all}
import org.kiji.express.{AvroRecord, EntityId, KijiSlice}
import cascading.pipe.joiner.{LeftJoin, RightJoin, OuterJoin}
import org.slf4j.LoggerFactory

/**
 * Produces tuples of (id, hero, skill rating, size) based on data
 * in the heroes table and the dota_player table. The skill rating is
 * calculated based on the win rate of a player with that hero relative
 * to that hero's average winrate in the same time period as fetched from
 * the heroes table. While not useful in of itself the result of this
 * job is a useful intermediate result.
 * Can use be used to just grab raw_winrates as skill rating without
 * referencing the heroes table with the --raw flag.
 *
 * @param args
 * --win_rate_col columns in the heroes table that contains the average
 * win rate for each hero for the needed timestamps
 * --interval the timestamp interval to use when cross referencing data
 * from the heroes table, ignored if --raw is used
 * --output, where to dump the tuples
 * --drop_first, ignore the first n games for each hero (this is to
 * avoid games their MMR is likey inaccurate)
 * --min_games, min number of games a player needs to play with the hero
 * to for use to use that player hero pair as an output
 * --raw, use raw winrates as a skill measure rather then adjusting (interval flag
 * will be ignored
 */
class GetHeroSkillTuples(args : Args) extends KijiJob(args) {

  val LOG = LoggerFactory.getLogger(this.getClass())

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++ Map(
    "hbase.client.scanner.caching" -> "100"
  )

  val raw = args.boolean("raw")
  val table = args.getOrElse("matches_table", MatchesTable)
  val interval = if(raw) 1 else args("interval").toLong * 1000

  if(raw){
    System.out.println("Using raw winrates");
  } else {
    System.out.println("Using adjusted winrates");
  }
  // Grab tuples for each (hero, time, player, winrate) we find in the player table
  val playerWinRates = KijiInput(args.getOrElse("players_table", PlayerTable))(
    Map(
      Column("data:player", all) -> 'player,
      Column("data:radiant_win", all) -> 'radiantWin,
      Column("match_derived_data:real_match", all) -> 'realMatch
    )
  ).rename('entityId -> 'id).map('id -> 'id){id : EntityId => id(0)}
    .flatMap(('player, 'radiantWin, 'realMatch) -> ('hero, 'win, 'time)){
    f : (KijiSlice[AvroRecord], KijiSlice[Boolean], KijiSlice[Double]) =>
      val rms = f._3.cells.filter(x => x.datum >= 2.0).map(x => x.version).toSet
      // Drop games that are not real matches and some of the first ones
      f._1.orderChronologically().cells.zip(f._2.orderChronologically().cells)
        .filter(x => rms.contains(x._1.version)).drop(args("drop_first").toInt).map(x => {
        val player = x._1.datum
        val radiantPlayer = DotaValues.radiantPlayer(player("player_slot").asInt())
        val rWin = x._2.datum
        val win = radiantPlayer == rWin
        (player("hero_id").asInt(), if(win) 1.0 else 0.0, interval * (x._1.version / interval))
      })
        // Do a group by to filter out heroes with too few games and then reflatten
        .groupBy(x => x._1).filter(p => p._2.size >= args("min_games").toInt).flatMap(x => x._2)
  }.project(('hero, 'win, 'time, 'id))

  val output = if(raw){
    playerWinRates.discard('time).groupBy('id, 'hero){gb => gb.average('win).size('size)}
  } else {
    // Grab tuples with the average winrate for a (hero, time) pairs
    val averageWinRates = KijiInput(args.getOrElse("heroes_table", HeroesTable))(
      Map(
        Column(args("win_rate_col"), all) -> 'wr
      )
    ).rename('entityId, 'hero_stats).map('hero_stats -> 'hero_stats){x : EntityId => x(0)}
      .flatMap('wr -> ('winRate, 'timeSlot)){
      wr : KijiSlice[Double] => wr.cells.map(x => (x.datum, x.version))
    }.discard('wr)

    // Finally do a join to turn the win_rate into a skill score
    averageWinRates.joinWithSmaller(('hero_stats, 'timeSlot) -> ('hero, 'time), playerWinRates, new  LeftJoin)
      .filter(('winRate, 'timeSlot, 'hero)){x : (Int, Long, java.lang.Integer) =>
      if(x._3 == null){
        LOG.warn("For hero " + x._1 + " at time" + x._2 + " found a game played with no corresponding win rate stat. Skipping this game.")
        false;
      } else true}
      .discard('hero_stats, 'timeSlot, 'time)
      .map(('win, 'winRate) -> 'score){
      x : (Double, Double) => x._1 - x._2
    }.discard('win, 'winRate)
     .groupBy('id, 'hero){gb => gb.average('score).size('size)}
  }

  output.write(Csv(args("output")))
}