package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, Args, Mode}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._
import org.kiji.express.{AvroRecord, KijiSlice}
import com.wibidata.wibidota.DotaValues

/**
 * Groups the winrates of games by the the valueS in an arbitrary column of
 * match_derived_data in dota_players. Returns the standard deviation, average,
 * and count.
 *
 * @param args
 */
class GroupWinrate(args: Args) extends KijiJob(args) {
  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++
    Map (
      "hbase.client.scanner.caching" -> "100",
      "mapred.task.profile" -> "true",
  "mapred.task.profile.reduces" -> "0",
  "mapred.task.profile.map" -> "0-2",
  "mapred.task.profile.params" -> "-agentlib:hprof=cpu=samples,heap=sites,depth=9,force=n,thread=y,verbose=n,file=%s"

    )

  KijiInput(args.getOrElse("table", PlayerTable))(
    Map (
      Column("match_derived_data:" + args("column"), all) -> 'values,
      Column("data:radiant_win", all) -> 'rWin,
      Column("data:player", all) -> 'player
    )
  )
    .flatMapTo(('values, 'rWin, 'player) -> ('values, 'win)){
    x : (KijiSlice[Double], KijiSlice[Boolean], KijiSlice[AvroRecord]) =>
      val useMatches = x._1.cells.map(a => a.version).toSet
      val m = (x._1.cells, x._2.cells.filter(x => useMatches.contains(x.version)), x._3.cells.filter(x => useMatches.contains(x.version))).zipped.toIterable.map({c =>
        val win = c._2.datum == DotaValues.radiantPlayer(c._3.datum("player_slot").asInt())
        (c._1 .datum, if(win) 1.0 else 0.0)
      }).toIterable
      m
  }.groupBy('values){gb => gb.sizeAveStdev('win -> ('size, 'av, 'std))}
    .write(Csv(args("output"), writeHeader = true))
}
