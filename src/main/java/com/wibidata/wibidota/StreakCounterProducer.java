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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wibidata.wibidota;
import com.wibidata.wibidota.avro.Player;
import org.apache.hadoop.io.LongWritable;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.*;

import java.io.IOException;

/**
 * Class to gather statistics about a player's likelihood to win or lose a match
 * depending on the number of matches won or lost in a row before.
 */
// TODO: Should case some of this work in the derived_data column
public class StreakCounterProducer extends KijiProducer {

  // We discards a player's first n matches
  private static final int BURN_IN =  12;

  @Override
  public void produce(KijiRowData input, ProducerContext context) throws IOException {
    double score = 0;
    int game = 0;
    long prevTime = 0;

    for(Long time : input.getTimestamps("data", "player").descendingSet()){

      // Make sure this is a 'serious' game
      if(!input.containsCell("match_derived_data", "real_match", time) ||
          (Double) input.getValue("match_derived_data", "real_match", time) < 2.0){
        continue;
      }

      // Check if we won
      Player self = input.getValue("data", "player", time);
      boolean radiantWin = (Boolean) input.getValue("data", "radiant_win", time);
      boolean radiantPlayer = DotaValues.radiantPlayer(self.getPlayerSlot());
      boolean winner = radiantWin == radiantPlayer;

      game++;

      // Write updates for the streaks we are tracking
      if(game > BURN_IN){
        context.put("streak_all", time, score);
      }

      // Update out win counter for the next iteration
      if(winner){
        if(score > 0){
          score++;
        } else {
          score = 1;
        }
      } else {
        if(score < 0){
          score--;
        } else {
          score = -1;
        }
      }
      prevTime = time;
    }
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(Integer.MAX_VALUE)
        .add("data", "radiant_win")
        .add("match_derived_data", "real_match")
        .add("data", "player");
    return builder.addColumns(def).build();
  }

  @Override
  public String getOutputColumn() {
    return "match_derived_data";
  }
}
