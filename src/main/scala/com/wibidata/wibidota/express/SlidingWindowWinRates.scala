package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, Mode, Args}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._
import org.kiji.express.{KijiSlice, EntityId, Cell}
import org.slf4j.{LoggerFactory}
import scala.collection.IterableLike

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
class SlidingWindowWinRates(args: Args) extends KijiJob(args) {

  val LOG = LoggerFactory.getLogger(this.getClass())

  def toTuple(gp : Cell[Double], wr : Cell[Double]) : (Double, Double) = {
    if(wr.version != gp.version) throw new RuntimeException
    (wr.datum, gp.datum)
  }


  def calcWindowStats(window : Seq[((Double, Double), (Double, Double))]) : (Double, Double) = {
    // Calculate the new games played and win rate
    val total_gps = window.map(elements => elements._1._1).sum;
    val win_rate =  window.map(elements => elements._2._1 * elements._1._2).sum / total_gps
    (win_rate, total_gps)
  }

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++
    Map (
      "hbase.client.scanner.caching" -> "200"
    )

  val startInterval = args("start_interval").toLong
  val interval = startInterval * 1000
  val windowSize = args("window_size").toInt
  val windowStep = args("window_step").toInt

  if(windowSize % 2 == 0){
    throw new RuntimeException("Cannot compute sliding window with even numbered window size")
  }

  val winRateColName = "win_rate_SW_" + windowSize + "_" + windowStep + "_" + startInterval
  val gamesPlayedColName = "games_played_SW_" + windowSize + "_" + windowStep + "_" + startInterval

  KijiInput(args.getOrElse("heroes_table", HeroesTable))(
    Map (
      MapFamily("data",
        "\\bwin_rate_" + startInterval + "\\b|\\bgames_played_" + startInterval + "\\b", all) -> 'data
    )
  ).flatMapTo(('entityId, 'data) -> ('entityId, 'games, 'win_rate, 'slot)){
    f : (EntityId, KijiSlice[Double]) =>

      val id = f._1

      val data = f._2

      // We order the list by qualifer so the first half is game_played cell and the second win_rate cells
      val ordered = data.orderByQualifier().cells;
      val size = ordered.size

      // Create a list of (win_rate, games_played) tuples per timeslot we are interested in,
      // fill in missing values with zeroes
      var on = 0
      val zipped = (ordered(0).version to ordered(size - 1).version by -interval).map{x =>
        if (ordered(on).version == x) {System.out.println("using"); on += 1; toTuple(ordered(on - 1), ordered(on + size/2 - 1)) } else (0.0,0.0)
      }

      (0 to zipped.size - 1).map({i =>
        val start = if(i < windowSize / 2) 0 else if (i + windowSize / 2 + 1 > zipped.size) zipped.size - windowSize else i - windowSize / 2
        val slice = zipped.slice(start, start + windowSize)
        val total_gps = slice.map(elements => elements._2).sum;
        val win_rate =  slice.map(elements => elements._1 * elements._2).sum / total_gps
        (id, total_gps, win_rate, ordered(0).version - i * interval)
      })
  }
    // Write the results
    .insert('win_rate_column, winRateColName)
    .insert('games_played_column, gamesPlayedColName)
    .write(KijiOutput(args.getOrElse("heroes_table", HeroesTable), 'slot)(
    Map(
      (MapFamily("data")('win_rate_column) -> 'win_rate),
      (MapFamily("data")('games_played_column) -> 'games)
    )))
}