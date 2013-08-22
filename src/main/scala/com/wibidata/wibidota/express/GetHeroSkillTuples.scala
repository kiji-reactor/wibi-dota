package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, Mode, Args}
import com.wibidata.wibidota.DotaValues
import com.wibidata.wibidota.express.DefaultResourceLocations._
import org.kiji.express.flow.{KijiJob, Column, KijiInput, all}
import org.kiji.express.{AvroRecord, EntityId, KijiSlice}
import cascading.pipe.joiner.{RightJoin, OuterJoin}
import org.slf4j.LoggerFactory

/**
 * Produces tuples of (id, hero, skill rating) based on data
 * in the heroes table amd the dota_player table. The skill rating is
 * calculated based on the win rate of a player with that hero relative
 * to that hero's average winrate in the same time period as fetched from
 * the heroes table.
 *
 * @param args
 * --win_rate_col columns in the heroes table that contains the average
 * win rate for each hero for the needed timestamps
 * --interval the timestamp interval to use when cross referencing data
 * from the heroes table
 * --output, where to dump the tuples
 * --drop_first, ignore the first n games for each hero (this is to
 * avoid games their MMR is likey inaccurate)
 * --min_games, min number of games a player needs to play with the hero
 * to for use to use that player hero pair as an output
 */
class GetHeroSkillTuples(args : Args) extends KijiJob(args) {

  val LOG = LoggerFactory.getLogger(this.getClass())

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++ Map(
    "hbase.client.scanner.caching" -> "100"
  )

  val table = args.getOrElse("matches_table", MatchesTable)
  val interval = args("interval").toLong * 1000

  // Grab tuples with the average winrate for a (hero, time) pairs
  val averageWinRates = KijiInput(args.getOrElse("heroes_table", HeroesTable))(
    Map(
      Column(args("win_rate_col"), all) -> 'wr
    )
  ).rename('entityId, 'hero_stats).map('hero_stats -> 'hero_stats){x : EntityId => x(0)}
   .flatMap('wr -> ('winRate, 'timeSlot)){
    wr : KijiSlice[Double] => wr.cells.map(x => (x.datum, x.version))
  }.discard('wr)

  // Grab tuples for each ()hero, time, player, winrate)
  val playerWinRates = KijiInput(args.getOrElse("players_table", PlayerTable))(
    Map(
      Column("data:player", all) -> 'player,
      Column("data:radiant_win", all) -> 'radiantWin,
      Column("match_derived_data:real_match", all) -> 'realMatch
    )
  ).rename('entityId -> 'id).map('id -> 'id){id : EntityId => id(0)}
    .flatMap(('player, 'radiantWin, 'realMatch) -> ('hero, 'win, 'time)){
    f : (KijiSlice[AvroRecord], KijiSlice[Boolean], KijiSlice[Double]) =>
      f._1.orderChronologically()
      f._2.orderChronologically()
      val rms = f._3.cells.filter(x => x.datum >= 2.0).map(x => x.version).toSet
      // Drop games that are not real matches and some of the first ones
      f._1.orderChronologically().cells.zip(f._2.orderChronologically().cells)
        .filter(x => rms.contains(x._1.version)).drop(args("drop_first").toInt).map(x => {
        val player = x._1.datum
        val radiantPlayer = DotaValues.radiantPlayer(player("player_slot").asInt())
        val rWin = x._2.datum
        val win = radiantPlayer == rWin
        (player("hero_id").asInt(), win, interval * (x._1.version / interval))
      })
        // Do a group by to filter out heroes with too few games and then reflatten
        .groupBy(x => x._1).filter(p => p._2.size >= args("min_games").toInt).flatMap(x => x._2)
  }.project(('hero, 'win, 'time, 'id))

  // Finally do a join to turn the win_rate into a skill score
  averageWinRates.joinWithSmaller(('winRate, 'timeSlot) -> ('hero, 'time), playerWinRates, new  RightJoin)
    .filter(('winRate, 'timeSlot, 'hero)){x : (Int, Long, java.lang.Integer) =>
      if(x._3 == null){
        LOG.warn("For hero " + x._1 + " at time" + x._2 + " found a game played with no corresponding win rate stat. Skipping this game.")
        false;
      } else true}
    .discard('hero_stats, 'timeSlot)
    .map(('win, 'winRate) -> 'score){
    x : (Boolean, Double) => if(x._1) 1.0 - x._2 else -x._2
  }.discard('win, 'winRate)
    .groupBy('hero, 'id){gb => gb.average('score)}
    .write(Csv(args("output")))

}