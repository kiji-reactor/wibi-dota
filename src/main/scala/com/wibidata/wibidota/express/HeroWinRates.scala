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
class HeroWinRates(args: Args) extends KijiJob(args)  {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++
    Map(
      "hbase.client.scanner.caching" -> "20",
      "cascading.spill.threshold" -> "2000",
      "cascading.spill.list.threshold" -> "2000",
      "cascading.spillmap.threshold" -> "2000"
    )

  val interval = args("interval").toLong * 1000

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
      val slot = interval * (fields._2.getFirst().version / interval)
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
    // GroupBy these tuples, manually set a lowish spill threshold so aggregateBy does not steal too much RAM
    .groupBy('hero_id, 'time){gb => (gb.spillThreshold(10).size('games_played).average('win -> 'win_rate))}
    // Format everything so we can write it to the hero table
    .map('hero_id -> 'hero_id){hero_id : Long => EntityId(hero_id.toInt)}
    .rename('hero_id -> 'entityId)
    .map('games_played -> 'games_played){x : Long => x.toDouble}
    .insert('win_rate_column, "win_rate_" + interval.toString)
   .insert('games_played_column, "games_played_" + interval.toString)
//  .write(Csv("hero_win_rate_test"))
  .write(KijiOutput(args.getOrElse("hero_table", HeroesTable), 'time)(
    Map(
      (MapFamily("data")('win_rate_column) -> 'win_rate),
      (MapFamily("data")('games_played_column) -> 'games_played)
  )))
}
