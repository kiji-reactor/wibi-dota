package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, TextLine, Args}
import org.kiji.express.{KijiSlice, EntityId, KijiJob}
import org.kiji.express.DSL._

class GamesPlayedProducer(args : Args) extends KijiJob(args) {
  KijiInput(args("table-uri"))(
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
