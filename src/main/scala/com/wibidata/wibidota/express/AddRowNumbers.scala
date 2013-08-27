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

package com.wibidata.wibidota.express

import com.twitter.scalding.{Mode, Args}
import org.kiji.express.flow._
import com.wibidata.wibidota.express.DefaultResourceLocations._

/**
 * Add a row number column to the derived_data family for each player who has played
 * at least one real match. Intended to facilitate use of Scalding's matrix library.
 */
class AddRowNumbers(args: Args) extends KijiJob(args) {

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] =
    super.config(mode) ++ Map ("hbase.client.scanner.caching" -> "100")

  var row = 0.0

  KijiInput(args.getOrElse("table", PlayerTable))(
    Map (
      Column("match_derived_data","real_match", latest) -> 'data
    )
  ).discard('data).groupAll(gs => gs.reducers(1)).mapTo('entityId -> 'row){
    id : Object => row += 1.0; row
  }
    .insert('row_number, "row_number")
    .write(KijiOutput(args.getOrElse("table", PlayerTable))(
    Map(
      (MapFamily("derived_data")('row_number) -> 'row)
    )))
}
