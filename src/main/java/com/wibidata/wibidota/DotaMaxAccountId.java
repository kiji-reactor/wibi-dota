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

package com.wibidata.wibidota;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

/**
 * A Map Reduce job built to find the maximum value the 'account id' field can take in the
 * raw  dota-matches JSON excluding anonmous accounts
 */
public class DotaMaxAccountId extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(DotaMaxAccountId.class);

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

      private final static JsonParser PARSER = new JsonParser();

      private static long max = Long.MIN_VALUE;

      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
        try{
          JsonObject matchData = PARSER.parse(value.toString()).getAsJsonObject();
          for (JsonElement playerElem : matchData.get("players").getAsJsonArray()) {
            JsonObject playerData = playerElem.getAsJsonObject();
            JsonElement acccountElem = playerData.get("account_id");
            if(acccountElem != null) {
              long accountId = acccountElem.getAsLong();
              // 4294967295L indicates anonymous and so can be ignored
              if(accountId != 4294967295L && accountId > max) {
                context.write(new Text("account_id"), new LongWritable(accountId));
                max = accountId;
              }
            }
          }

        } catch (IllegalStateException e) {
          // Ignore
        }
      }
    }

    /**
     * Reducer that continues to search for the max value
     */
    public static class TakeMax
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            long max = Long.MIN_VALUE;
            for(LongWritable id : values) {
              if(id.get() > max) {
                max = id.get();
              }
            }
            context.write(key, new LongWritable(max));
        }
    }

  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new DotaMaxAccountId(), args);
    System.exit(res);
  }

  public final int run(final String[] args) throws Exception {
    Job job = new Job(super.getConf(), "Dota Max Builder");
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setMapperClass(DotaMaxAccountId.Map.class);
    job.setCombinerClass(DotaMaxAccountId.TakeMax.class);
    job.setReducerClass(DotaMaxAccountId.TakeMax.class);

    job.setJarByClass(DotaMaxAccountId.class);

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
