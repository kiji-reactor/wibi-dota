package com.wibidata.wibidota.express

import com.twitter.scalding.{Args}
import org.kiji.express.{KijiSlice}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._

class GamesPlayedProducer(args : Args) extends KijiJob(args) {
  KijiInput(args.getOrElse("player_table", PlayerTable))(
    Map(
      MapFamily("match_derived_data", "real_match", all) -> 'real_matches
    )
  ).map('real_matches -> 'count){match_ids : KijiSlice[Double] => match_ids.cells.count({
      x => x.datum != 1.0
    })
  .toDouble}
     .discard('real_matches).insert('name, "real_matches_played")
  .write(KijiOutput(args("table-uri"))(Map(MapFamily("derived_data")('name) -> 'count)))
}
