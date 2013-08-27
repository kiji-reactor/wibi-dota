package com.wibidata.wibidota.express

import com.twitter.scalding.{JsonLine, Mode, Args}
import scala.collection.JavaConversions._
import org.kiji.express.flow.{KijiOutput, KijiJob}
import org.kiji.express.EntityId
import com.wibidata.wibidota.express.DefaultResourceLocations._

/**
 * Adds hero names to the heroes table by parsing
 * a json files containing both the hero names and hero ids
 *
 * @param args, arguements including
 * --hero_name_file the json file
 * --hero_table location of the hero table, defaults to wibidota/heroes
 */
class AddHeroNames(args: Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] =
    super.config(mode) ++ Map("hbase.client.scanner.caching" -> "50")

  JsonLine(args("hero_name_file"), 'heroes)
    // JsonLne outputs Java classes so we have tp use those here
    .flatMap('heroes -> ('name, 'entityId)){heroes : java.util.ArrayList[java.util.Map[String, Object]] =>
      heroes.map(x => (x.get("localized_name").toString,
        EntityId(x.get("id").toString.toInt)))
  }
  .write(KijiOutput(args.getOrElse("hero_table", HeroesTable))('name -> "names:name"))
}
