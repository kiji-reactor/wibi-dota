package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, Mode, Args}
import org.kiji.express.{EntityId, AvroRecord, KijiSlice, KijiJob}
import org.kiji.express.DSL._
import com.wibidata.wibidota.DotaValues

class HeroWinRates(args: Args) extends KijiJob(args)  {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++ Map("hbase.client.scanner.caching" -> "50")

  val interval = args("interval").toLong * 1000

  KijiInput(args("matches_table"))(
    Map (
      MapFamily("derived_data", "real_match", latest) -> 'real_match,
      Column("data:player_data", versions = latest) -> 'players,
      Column("data:radiant_win", versions = latest) -> 'r_win
    )
  ).flatMapTo(('players, 'real_match, 'r_win) -> ('win, 'hero_id, 'time)) {
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
  }.groupBy('hero_id, 'time){_.average('win -> 'win_rate)}
  .map('hero_id -> 'entityId){hero_id : Long => EntityId.fromComponents(args("hero_table"), Seq(hero_id.toInt))}
  .insert('field, "win_rate_" + args("interval"))
  .write(KijiOutput(args("hero_table"), 'time)(Map(MapFamily("data")('field) -> 'win_rate)))
}
