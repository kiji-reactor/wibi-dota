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

import com.twitter.scalding.{Csv, JsonLine, Args}
import org.kiji.express.flow.KijiJob
import scala.collection.JavaConversions._


class JsonLinesPerPlayer(args : Args) extends KijiJob(args) {

  val id = args("player_id").toLong

  JsonLine(args("json_file"), ('match_id, 'players))
    .project('match_id, 'players)
    .flatMapTo(('match_id, 'players) -> 'out){
    f : (Long, java.util.ArrayList[java.util.Map[String, Number]]) =>
        if(f._2.exists(x => x.get("account_id") != null && x.get("account_id").longValue() == id))
          Some(f._1) else None
  }.write(Csv(args("output_file")))
}
