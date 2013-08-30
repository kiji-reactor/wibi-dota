package com.wibidata.wibidota.express

import com.twitter.scalding.{SequenceFile, Args, Csv, Mode}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._
import scala.Some
import org.kiji.express.{AvroRecord, EntityId, KijiSlice}
import org.apache.mahout.math.{SequentialAccessSparseVector, VectorWritable, OrderedIntDoubleMapping}


/**
 * Produces tuples of (playerId, heroId, percent played hero).
 * The output can be used by TuplesToSequenceFile. Note we use
 * a row number rather than account_id to identify players so
 * the ids will fill into a reasonable range of values.
 *
 * @param args
 * --row_number_col. name of the column the contains the row numbers
 * to use. Ignore rows without this field.
 * --output, file to dump the outputs
 */
class HeroPicks(args: Args) extends KijiJob(args) with CalcCorrelations {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = {
    super.config(mode) ++ Map("hbase.client.scanner.caching" -> "100")
  }

  val input = KijiInput(args.getOrElse("player_table", PlayerTable))(
    Map (
      Column("data:player", versions = all) -> 'player,
      Column("match_derived_data:real_match", versions = all) -> 'rm,
      Column(args("row_number_col"), versions = latest) -> 'rowNumber
    ))
    .flatMapTo(('rowNumber, 'player, 'rm) -> ('id, 'hero, 'percent)){
    f : (KijiSlice[Double], KijiSlice[AvroRecord], KijiSlice[Double]) =>
      val realMatchesTimes = f._3.cells.map(x => x.version).toSet
      val gamesPerHero = f._2.cells.filter(x => realMatchesTimes.contains(x.version))
          .map(x => (x.datum("hero_id").asInt())).groupBy(identity).mapValues(x => x.size)
      val sum = gamesPerHero.values.sum.toDouble
      gamesPerHero.map(x => (f._1.getFirstValue(), x._1, x._2 / sum))
  }

  input.write(Csv(args("output")))
}
