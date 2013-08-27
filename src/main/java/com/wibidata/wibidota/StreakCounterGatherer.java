package com.wibidata.wibidota;

import com.wibidata.wibidota.avro.Player;
import org.apache.hadoop.io.IntWritable;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to used gather statistics about a player's likelihood to win or lose a match
 * depending on the number of matches won or lost in a row before.
 * Expected to read from dota_players and emits keys of the
 * number of games won or lost previously in a row (negative if it was games lost,
 * positive otherwise) along with IntArrayWritables
 * containing the number of games that were played and the number of games won
 * by player who had just lost or won that number of games.
 */
public class StreakCounterGatherer extends KijiGatherer<IntWritable, IntArrayWritable> {

  private final Logger LOG = LoggerFactory.getLogger(this.getClass());

  // We discards some each player's first matches, can be mutated by
  // setting streakcounter_burn_in in the the conif file
  private static int burnIn = 12;

  // Map partial aggregation, the memory space will be proportional to the longest winning and
  // losing streak a player handed to the mapper had, which should fit in memory without a hitch
  private static final Map<Integer, int[]> valueMap = new HashMap<Integer, int[]>();

  @Override
  public void setup(GathererContext context) throws java.io.IOException {
    String burnInStr = getConf().get("streakcounter.burn_in");
    if(burnInStr != null){
      burnIn = Integer.parseInt(burnInStr);
      LOG.info("streakcounter.burn_in set to: " + burnIn);
    } else {
      LOG.info("streakcounter.burn_in not set, defaulting to: " + burnIn);
    }
    super.setup(context);
  }

  @Override
  public void gather(KijiRowData input, GathererContext<IntWritable, IntArrayWritable> context) throws IOException {
    int score = 0;
    int game = 0;

    for(Long time : input.getTimestamps("data", "player").descendingSet()){
      game++;

      // Make sure this is a 'serious' game
      if(!input.containsCell("match_derived_data", "real_match", time) ||
          (Double) input.getValue("match_derived_data", "real_match", time) < 2.0){
        continue;
      }

      // Work out if we won or not
      Player self = input.getValue("data", "player", time);
      boolean radiantWin = (Boolean) input.getValue("data", "radiant_win", time);
      boolean radiantPlayer = DotaValues.radiantPlayer(self.getPlayerSlot());
      boolean winner = radiantWin == radiantPlayer;


      // Write the update to our aggregator
      if(game > burnIn){
        if(!valueMap.containsKey(score)){
          valueMap.put(score, new int[]{0, 0});
        }
        valueMap.get(score)[0] += 1;
        if(winner)
          valueMap.get(score)[1] += 1;
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
    }
  }

  @Override
  public void cleanup(GathererContext<IntWritable, IntArrayWritable> context) throws IOException {
    // Spew the results from out aggregation
    for(Map.Entry<Integer, int[]> entry : valueMap.entrySet()){
      context.write(new IntWritable(entry.getKey()), new IntArrayWritable(new IntWritable[]{
          new IntWritable(entry.getValue()[0]),
          new IntWritable(entry.getValue()[1])
      }));
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
  public Class<?> getOutputKeyClass() {
    return IntWritable.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return IntArrayWritable.class;
  }
}