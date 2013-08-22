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
 * to_file, optionally write the results to file instead of the heroes table
 */
class SlidingWindowWinRates(args: Args) extends KijiJob(args) {

  val LOG = LoggerFactory.getLogger(this.getClass())

  /*
   * Sanity checks to ensure the versions match then returns the data in the cells
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
  val minGames = args("min_games").toInt

  if(windowSize % 2 == 0){
    throw new RuntimeException("Cannot compute sliding window with even numbered window size")
  }

  val winRateColName = "win_rate_average_" + windowStep * startInterval
  val gamesPlayedColName = "games_played_average_" + windowStep * startInterval


  val rates = KijiInput(args.getOrElse("heroes_table", HeroesTable))(
    Map (
      MapFamily("data",
        "\\bwin_rate_" + startInterval + "\\b|\\bgames_played_" + startInterval + "\\b", all) -> 'data
    )
  )
    .flatMapTo(('entityId, 'data) -> ('entityId, 'games, 'win_rate, 'slot)){
    f : (EntityId, KijiSlice[Double]) =>

      val id = f._1

      val data = f._2

      // We order the list by qualifer so the first half is game_played cell and the second win_rate cells
      val ordered = data.orderByQualifier().cells.reverse//.filter(x => x.version < 1373965200000L);
      val size = ordered.size

      // Create a list of (win_rate, games_played) tuples per timeslot we are interested in,
      // fill in missing values with zeroes. We might pad the list with (0.0) tuples to ensure that
      // our first value is a multiple of windowStep*interval
      var on = 0
      val start = ordered(0).version - interval * ((ordered(0).version / interval) % (windowStep))
      val zipped = (start to ordered(size - 1).version by interval).map{x =>
        if (ordered(on).version == x) {on += 1; toTuple(ordered(on - 1), ordered(on + size/2 - 1  )) } else (0.0,0.0)
      }.toList

      // Move along the list and calculate the stats we want per each interval
      // This is potentially very expensive, but we relay on the fact that the win_rate columns we are reading
      // from contains coarse grained enough samples (and thus few enough entries per cell) to make this run in decent time.
      (0 to zipped.size - 1 by windowStep).map({i =>
        var numGames = zipped(i)._1
        var weightedWinRate = zipped(i)._1 * zipped(i)._2
        var sliceStart = i
        var sliceEnd = i
        // Expand outwards from i until we have a window with sufficient number of games and
        // of sufficient size to return an ouput. We attempt to expand in both direction is possible
        while(numGames < minGames || (sliceEnd - sliceStart + 1 < windowSize)){
          if(sliceStart > 0){
            sliceStart -= 1
            numGames += zipped(sliceStart)._1
            weightedWinRate += zipped(sliceStart)._1 * zipped(sliceStart)._2
          }
          if(sliceEnd < zipped.size - 1){
            sliceEnd += 1
            numGames += zipped(sliceEnd)._1
            weightedWinRate += zipped(sliceEnd)._1 * zipped(sliceEnd)._2
          }
        }
        (id, numGames, weightedWinRate / numGames, start + i * interval)
      })
  }

  if(args.boolean("to_file")){
    rates.write(Csv(args("to_file"), writeHeader = true))
  } else {
    // Write the results
    rates
      .insert('win_rate_column, winRateColName)
      .insert('games_played_column, gamesPlayedColName)
      .write(KijiOutput(args.getOrElse("heroes_table", HeroesTable), 'slot)(
      Map(
        (MapFamily("data")('win_rate_column) -> 'win_rate),
        (MapFamily("data")('games_played_column) -> 'games)
      )))
  }
}