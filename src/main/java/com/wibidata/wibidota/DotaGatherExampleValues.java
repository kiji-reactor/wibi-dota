package com.wibidata.wibidota;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * A Map Reduce job built to gather example matches for each value  and columns could take,
 * Currently only works for fields in Player objects, Run on the raw JSON encoded data.
 * Useful if you need to manually check what values map to what in game events.
 */
public class DotaGatherExampleValues extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(DotaGatherExampleValues.class);

    static enum Counters {
        MALFORMED_MATCH_LINES
    }

    // Maximum number of examples to print per a value
    private static int MAX_EXAMPLES = 10;

    // Field to gather values from
    private static String FIELD = "leaver_status";

    // String to represent null
    private static String NULL_STR = "null";

    /**
     * A Mapper class that sends a limited number of examples
     * to the reducers
     */
    public static class EnumGatherMap extends Mapper<LongWritable, Text, Text, Text> {

        private final static JsonParser PARSER = new JsonParser();

        // Just to caches this value
        private static final IntWritable ONE = new IntWritable(1);

        private static final java.util.Map<String, Integer> VALUES_SENT = new HashMap<String, Integer>();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                JsonObject matchData = PARSER.parse(value.toString()).getAsJsonObject();
                for (JsonElement playerElem : matchData.get("players").getAsJsonArray()) {
                    JsonObject playerData = playerElem.getAsJsonObject();
                    JsonElement fieldValue = playerData.get(FIELD);
                    String fieldStr = (fieldValue == null ?
                        NULL_STR : fieldValue.getAsString());
                    if(VALUES_SENT.get(fieldStr) == null) {
                        VALUES_SENT.put(fieldStr, 1);
                        context.write(new Text(fieldStr), value);
                        break;
                    } else {
                        Integer cur = VALUES_SENT.get(fieldStr);
                        if (cur < MAX_EXAMPLES) {
                            VALUES_SENT.put(fieldStr, cur + 1);
                            context.write(new Text(fieldStr), value);
                            break;
                        }
                    }
                }
            } catch (IllegalStateException e) {
                // Indicates malformed JSON.
                context.getCounter(Counters.MALFORMED_MATCH_LINES).increment(1);
            }
        }
    }

    /**
     * Combiner class that aggregates lines
     */
    public static class AppendText
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            for(Text text : values) {
                out.append(text + "\n");
            }
            context.write(key, new Text(out.toString()));
        }
    }

    /**
     * Reducer, aggregates lines, creates a summary, throttles excess lines
     */
    public static class EnumGatherReducer
            extends Reducer<Text, Text, Text, Text> {

        private final static JsonParser PARSER = new JsonParser();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder allLines = new StringBuilder();
            StringBuilder summaryString = new StringBuilder("\n***** Key: " + key + " *****\nMatch ids:\n");
            int valuesAdded = 0;
            for(Text text : values) {
                for(String line : text.toString().split("\n")) {
                    valuesAdded++;
                    JsonObject matchData = PARSER.parse(line).getAsJsonObject();
                    long matchId = matchData.get("match_id").getAsLong();
                    for (JsonElement playerElem : matchData.get("players").getAsJsonArray()) {
                        JsonObject playerData = playerElem.getAsJsonObject();
                        String fieldValue = (playerData.get(FIELD) == null ? NULL_STR :
                            playerData.get(FIELD).getAsString());
                        if(key.toString().equals(fieldValue)) {
                            summaryString.append("Match: " + matchId + " player: " +
                                    playerData.get("player_slot").getAsInt() + " (" +
                                    DotaValues.playerSlot(playerData.get("player_slot").getAsInt()) + ")\n");
                            break;
                        }
                    }
                    allLines.append(line);
                    if(valuesAdded > MAX_EXAMPLES) {
                        break;
                    }
                }
                if(valuesAdded > MAX_EXAMPLES) {
                    break;
                }
            }
            context.write(key, new Text(summaryString.toString() +
                "\n***** LINES *****\n" + allLines.toString()));
        }
    }

    /**
     * Runs the job, requires that the gson package is made
     * available to the cluster.
     *
     * @args Should contain the input and output file
     * @throws Exception is there was a problem running the job
     */
    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new DotaGatherExampleValues(), args);
        System.exit(res);
    }

    public final int run(final String[] args) throws Exception {
        Job job = new Job(super.getConf(), "Dota Gatherer Example Values");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(EnumGatherMap.class);
        job.setCombinerClass(AppendText.class);
        job.setReducerClass(EnumGatherReducer.class);

        job.setJarByClass(DotaGatherExampleValues.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return -1;
        }
    }
}
