package com.wibidata.wibidota.express

import com.twitter.scalding.{JsonLine, Mode, Args}
import org.kiji.express.{EntityId, AvroRecord, KijiSlice, KijiJob}
import org.kiji.express.DSL._
import scala.collection.JavaConversions._

/**
 * Adds hero n
 *
 * @param args
 */
class AddHeroNames(args: Args) extends KijiJob(args)  {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++ Map("hbase.client.scanner.caching" -> "50")

  JsonLine(args("hero_names"), 'heroes)
    .flatMap('heroes -> ('name, 'entityId)){heroes : java.util.ArrayList[java.util.Map[String, Object]] =>
      heroes.map(x => (x.get("localized_name").toString,
        EntityId.fromComponents(args("hero_table"), Seq(x.get("id").toString.toInt))))
  }
  .write(KijiOutput(args("hero_table"))('name -> "names:name"))
}
