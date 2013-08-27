/* Copyright 2013 WibiData, Inc.
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
import com.wibidata.wibidota.DotaValues.{LeaverStatus, LobbyType}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._;

/**
 * Runs a job that produces a 'real_match' column that contains values indicating a game was
 * a 'real' game, or game where we expect the win to be an accurate reflection of the player's
 * skill at Dota. Our criteria for this is:
 * Game mode is AP, CM, RD, SD, AR, LP, Compendium, or Unknown 0
 * Lobby is Solo Queue, Team Match, PMMR, or Tournament
 * All 10 players are human
 * Games meeting this criteria have a value in this field, either
 * 3.0 if all players stayed, 2.0 if all plays did not recieve an abandon (but might have disconnected
 * before the game ended) or 1.0 if at least one player abandoned.
 *
 */
class DeriveRealMatch(args: Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = super.config(mode) ++ Map(
    "hbase.client.scanner.caching" -> "100"
  )

  // Is the game a real match based on its game mode and lobby type
  def isRealMatch(gmCell : KijiSlice[Int], ltCell : KijiSlice[Int]): Boolean = {
    val gameMode = gmCell.getFirstValue();
    val lobbyType = LobbyType.fromInt(ltCell.getFirstValue());
    return ((gameMode < 6 || gameMode == 12 || gameMode == 14)  &&
      (lobbyType == LobbyType.SOLO_QUEUE || lobbyType == LobbyType.TEAM_MATCH ||
        lobbyType == LobbyType.PUBLIC_MATCHMAKING ||lobbyType == LobbyType.TOURNAMENT ))
  }

  // 3.0, 2.0, or 1.0 depending on the conditions stated above
  def statusFromLeavers(playersCell : KijiSlice[AvroRecord]) : Double = {
    val players = playersCell.getFirstValue()("players").asList()
    var allStayed = true
    for(i <- 0 to (players.size - 1)){
      val ls = players(i)("leaver_status").asInt()
      if(ls >= 2){
        return 1.0
      } else {
        allStayed = (allStayed && ls == 0)
      }
    }
    if(allStayed) 3.0 else 2.0
  }

  val table = args.getOrElse("matches_table", MatchesTable)

  KijiInput(table)(
    Map(
      Column("data:human_players", latest) -> 'human_players,
      Column("data:player_data", latest) -> 'players,
      Column("data:game_mode", latest) -> 'game_mode,
      Column("data:lobby_type", latest) -> 'lobby_type
    )
  )
    // Filter out bad game modes and lobby types
    .filter('game_mode, 'lobby_type){
    fields : (KijiSlice[Int], KijiSlice[Int]) =>
      isRealMatch(fields._1, fields._2)
  }.discard('game_mode, 'lobby_type)

    // Filter out game with less then 10 players,
    .filter('human_players){
    human_players : KijiSlice[Int] =>
      human_players.getFirstValue() == 10
  }.discard('human_players)

    // Add the values we want to insert and write
    .map('players -> 'status){statusFromLeavers}
    .discard('players)
    .insert('name, "real_match").insert('time, 0L)
    .write(KijiOutput(table, 'time)(Map(MapFamily("derived_data")('name) -> 'status)))
}