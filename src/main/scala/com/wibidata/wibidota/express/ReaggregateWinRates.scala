package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, Mode, Args}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._
import org.kiji.express.{EntityId, KijiSlice, Cell}

/**
 * Class that can turn fine grained win rate and games played samples
 * per each hero into more corse grained samples. Like HeroWinRates
 * writes to columns:
 * win_rate_<end_interval>
 * games_played_<end_interval>
 *
 * and expects to read from columns
 *
 * win_rate_<start_interval>
 * games_played_<end_interval>
 *
 * @param args, arguements including
 * start_interval, the length of time (in seconds) between the fine grained samples
 * end_interval, the length of time between the new corse grained sample
 */
class ReaggregateWinRates(args: Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++
    Map (
      "hbase.client.scanner.caching" -> "50"
    )

  val startInterval = args("start_interval").toLong * 1000
  val endInterval = args("end_interval").toLong * 1000

  KijiInput(args.getOrElse("heroes_table", HeroesTable))(
    Map (
      MapFamily("data",
        "win_rate_" + startInterval.toString + "|games_played_" + startInterval.toString, all) -> 'data
    )
  )
    .map('entityId -> 'entityId){id : EntityId => id(0)}
    .flatMap('data -> ('weighted_win_rate, 'games, 'slot)){
    data : (KijiSlice[Double]) =>
      val ordered = data.orderByQualifier();
    val size = ordered.size
      ordered.cells.slice(0, size/2).zip(ordered.cells.slice(size/2, size)).
        map({x =>
        val gp = x._1
        val wr = x._2
        if(wr.version != gp.version) throw new RuntimeException()
        (wr.datum * gp.datum, gp.datum, wr.version / endInterval)
      }).toIterable}
     .discard('data)
    .groupBy('entityId, 'slot){gb => gb.sum('games -> 'total_games).sum('weighted_win_rate -> 'total_weighted_win_rate)}
    .map(('total_games, 'total_weighted_win_rate) -> 'win_rate){
    x : (Double, Double) =>
      System.out.println("games, weighed wr: " + x.toString())
      x._2 / x._1}
    .map('entityId -> 'entityId){id : Number => EntityId(id.intValue())}
    .map('slot -> 'slot){x : Long => x * endInterval}
    .insert('win_rate_column, "win_rate_" + (endInterval / 1000).toString)
    .insert('games_played_column, "games_played_" + (endInterval / 1000).toString)
    .write(KijiOutput(args.getOrElse("heroes_table", HeroesTable), 'slot)(
    Map(
      (MapFamily("data")('win_rate_column) -> 'win_rate),
      (MapFamily("data")('games_played_column) -> 'total_games)
    )))
}