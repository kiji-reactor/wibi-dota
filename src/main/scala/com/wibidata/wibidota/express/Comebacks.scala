package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, Mode, Args}
import org.kiji.express.flow._
import org.slf4j.LoggerFactory
import org.kiji.express.KijiSlice
import com.wibidata.wibidota.DotaValues
import com.wibidata.wibidota.express.DefaultResourceLocations._
import com.twitter.scalding.Csv

/**
 * Job that creates counts the number of barracks the winning team had
 * left standing for each "real_match"
 *
 *
 * @param args, comamnd line arguements with flags:
 * --matches_table, optional, the location of the dota_matches table defaults
 * to wibidota/dota_matches
 * --output, file to store the output
 */

class Comebacks(args : Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] =
    super.config(mode) ++ Map("hbase.client.scanner.caching" -> "100")

  val table = args.getOrElse("matches_table", MatchesTable)

  KijiInput(table)(
    Map(
      Column("data:radiant_win", latest) -> 'radiantWin,
      Column("derived_data:real_match", latest) -> 'realMatch,
      Column("data:dire_barracks_status", latest) -> 'direRax,
      Column("data:radiant_barracks_status", latest) -> 'radiantRax
    )
  ).discard('entityId)
    // Only count real matches with no abandons
    .filter('realMatch){x : KijiSlice[Double] => x.getFirstValue() >= 2.0}
    .mapTo(('direRax, 'radiantRax, 'radiantWin) -> 'raxesLeft){
    x : (KijiSlice[Int], KijiSlice[Int], KijiSlice[Boolean]) =>
    // If the radiant win return their rax counts and vice versa
      if(x._3.getFirstValue()){
        Integer.bitCount(x._2.getFirstValue())
      } else {
        Integer.bitCount(x._1.getFirstValue())
      }
  }.groupBy('raxesLeft){_.size}.write(Csv(args("output"), writeHeader = true))
}
