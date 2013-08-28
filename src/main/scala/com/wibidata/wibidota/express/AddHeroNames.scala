/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
