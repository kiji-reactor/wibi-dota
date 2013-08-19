package com.wibidata.wibidota.express

import com.twitter.scalding.{Mode, Csv, Args}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._
import org.kiji.express.{AvroRecord, KijiSlice}
import com.wibidata.wibidota.DotaValues

class WinRateVsGamesPlayed(args: Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++ Map(
    "hbase.client.scanner.caching" -> "20"
  )

  val playerWinRates = KijiInput(args.getOrElse("players_table", PlayerTable))(
    Map(
      Column("data:player", all) -> 'player,
      Column("data:radiant_win", all) -> 'radiantWin,
      Column("match_derived_data:real_match", all) -> 'realMatch
    )
  )
    .flatMapTo(('player, 'radiantWin, 'realMatch) -> ('win, 'game_number)){
    f : (KijiSlice[AvroRecord], KijiSlice[Boolean], KijiSlice[Double]) =>
      val rms = f._3.cells.filter(x => x.datum >= 2.0).map(x => x.version).toSet
      var game = 0
      f._1.cells.zip(f._2.cells).filter(x => rms.contains(x._1.version))
        .map(x => {
        val player = x._1.datum
        val radiantPlayer = DotaValues.radiantPlayer(player("player_slot").asInt())
        val rWin = x._2.datum
        if((radiantPlayer && rWin) || (!radiantPlayer && !rWin)){
          1.0
        } else {
          0.0
        }
//      })
      }).sliding(args("window_size").toInt).map(x => {
        game += 1;
        (x.sum / x.size, game)
      }).toIterable
  }
  .groupBy('game_number){_.sizeAveStdev('win-> ('size, 'av, 'std))}
    .write(Csv(args("output"), writeHeader = true))
}
