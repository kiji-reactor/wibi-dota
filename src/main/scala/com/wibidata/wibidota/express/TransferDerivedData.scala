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

import com.twitter.scalding._

import org.kiji.express._
import org.kiji.express.flow._
import scala.Some
import com.wibidata.wibidota.express.DefaultResourceLocations._

/**
 * Transfers the most recent derived_data information from the dota_matches table
 * to the dota_players table. For each most recent value in the derived_data map family of the
 * matches table this will issue a put request to corresponding matches_derived_data
 * field in the dota_players field.for the same value.
 *
 * @param args, command line args including the flags
 * --table-in the dota_matches table
 * --table-out the dota_players table
 * --regex transfer only columns in the derived_data family that matches given regex
 */
// This is generally slow, it is perferable to use either PortDerivedData.java or DerivedDataHFiles.java
class TransferDerivedData(args: Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++
    Map("hbase.client.scanner.caching" -> "50")

  def extractPlayerIds(players : KijiSlice[AvroRecord]) : List[Int] = {
    players.getFirstValue().apply("players").asList().
      flatMap{player : AvroValue =>
      val elem = player.apply("account_id")
      if(elem.equals(null) || elem.asInt() == -1){
        None
      } else {
        Some(elem.asInt())
      }
    }
  }

  KijiInput(args.getOrElse("table-in", MatchesTable))(
    Map (
      MapFamily("derived_data", "", latest) -> 'data,
      Column("data:player_data", versions = latest) -> 'players
    )
  ).discard('entityId)
   .map('players -> 'account_ids){extractPlayerIds}
   .filter('account_ids){account_ids : List[Int] => account_ids.length != 0}
   .map('players -> 'time){players : KijiSlice[AvroRecord] => players.getFirst().version}
    .discard('players)
    .flatMap('data -> ('field, 'value)){
    data : KijiSlice[Double] =>
      (data.cells.map(x => (x.qualifier, x.datum))) }
   .discard('data)
   .flatMap('account_ids -> 'entityId)
  {account_ids : List[Int] => account_ids.map(id => EntityId(id))}
   .discard('account_ids)
   .write(KijiOutput(args.getOrElse("table-out", PlayerTable), 'time)(Map(MapFamily("match_derived_data")('field) -> 'value)))
}
