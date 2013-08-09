package com.wibidata.wibidota.express

import com.twitter.scalding.{Csv, JsonLine, Args}
import org.kiji.express.flow.KijiJob
import scala.collection.JavaConversions._


class JsonLinePerPlayer(args : Args) extends KijiJob(args) {

  val id = args("player_id").toLong

  JsonLine(args("json_file"), ('match_id, 'players))
    .project('match_id, 'players)
    .flatMapTo(('match_id, 'players) -> 'out){
    f : (Long, java.util.ArrayList[java.util.Map[String, Number]]) =>
        if(f._2.exists(x => x.get("account_id") != null && x.get("account_id").longValue() == id))
          Some(f._1) else None
  }.write(Csv(args("output_file")))
}
