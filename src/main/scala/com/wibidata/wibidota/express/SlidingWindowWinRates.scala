package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, Mode, Args}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._
import org.kiji.express.{KijiSlice, EntityId, Cell}
import org.slf4j.{LoggerFactory}
import scala.collection.IterableLike

/**
 * Class that can turn fine grained win rates and games played samples
 * per hero into sliding window average games played and win rates per hero
 * writes to columns:
 * win_rate_SW_<windows_size>_<window_step>_<start_interval>
 * games_played_SW_<windows_size>_<window_step>_<start_interval>
 *
 * The entity-id will be the hero_id, the timestamp will be the 'middle'
 * timestamp of the window rounded down to the nearest <start_interval>
 *
 * Expects to read from columns
 *
 * win_rate_<start_interval>
 * games_played_<end_interval>
 *
 * We take special care to generate an output for each timestamp we find in
 * win_rate_<start_interval> starting with the smallest and proceeding by
 * window_step to the latest
 * This means (in theory) for any hero that hero's sliding average winrate
 * at a paricular time by looking up
 * (timestamp / (interval * window_step)) * (timestamp * windowstep)
 *
 * @param args, arguements including
 * start_interval, the size of each 'slot' in the window
 * window_size, the number slots wide the window will be
 * window_step, the number of slots to move the window forward
 */
class SlidingWindowWinRates(args: Args) extends KijiJob(args) {

  val LOG = LoggerFactory.getLogger(this.getClass())

  /*
   * Sanity checks to ensure the version match then returns the data in the cells
   */
  def toTuple(gp : Cell[Double], wr : Cell[Double]) : (Double, Double) = {
    if(wr.version != gp.version) throw new RuntimeException
    (wr.datum, gp.datum)
  }

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++
    Map (
      "hbase.client.scanner.caching" -> "100"
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
  ).filter('entityId){id : EntityId => id(0) == 14}.flatMapTo(('entityId, 'data) -> ('entityId, 'games, 'win_rate, 'slot)){
    f : (EntityId, KijiSlice[Double]) =>

      val id = f._1

      val data = f._2

      // We order the list by qualifer so the first half is game_played cell and the second win_rate cells
      val ordered = data.orderByQualifier().cells.reverse;
      val size = ordered.size

      // Create a list of (win_rate, games_played) tuples per timeslot we are interested in,
      // fill in missing values with zeroes
      var on = 0
      val zipped = (ordered(0).version to ordered(size - 1).version by (interval * windowStep)).map{x =>
        if (ordered(on).version == x) {on += windowStep; toTuple(ordered(on - windowStep), ordered(on + size/2 - windowStep)) } else (0.0,0.0)
      }

      // Move along the list and calculate the stats we want per each interval
      (0 to zipped.size - 1).map({i =>
        val start = if(i < windowSize / 2) 0 else if (i + windowSize / 2 + 1 > zipped.size) zipped.size - windowSize else i - windowSize / 2
        val slice = zipped.slice(start, start + windowSize)
        val total_gps = slice.map(elements => elements._1).sum;
        val win_rate =  slice.map(elements => elements._1 * elements._2).sum / total_gps
        (id, total_gps, win_rate, ordered(0).version + i * interval * windowStep)
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