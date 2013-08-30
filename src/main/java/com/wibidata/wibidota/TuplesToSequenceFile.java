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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Job that converts a matrix as expressed with sparse tuples of the form
 * (column, row, value) stored in a csv file to a sequence files of
 * VectorWritables.
 *
 * ex. kiji jar $WIBIDOTA/lib/wibi-dota-0.0.1.jar com.wibidata.wibidota.TuplesToSequenceFile -libjars \
 * $WIBIDOTA/lib/mahout-core-0.7-cdh4.1.2-job.jar tuples_csv seq_files
 */
public class TuplesToSequenceFile  extends Configured implements Tool {

  // Max size of vectors
  public static final int MAX_SIZE = 626825;

  public static class ReadTuples extends Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> {

    private static final IntWritable outKey = new IntWritable(0);
    private static final DoubleArrayWritable  outValue = new DoubleArrayWritable(new DoubleWritable[]{new DoubleWritable(0), new DoubleWritable(0)});

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] split = value.toString().split(",");
      outKey.set((int) Double.parseDouble(split[1]));
      outValue.get(0).set(Double.parseDouble(split[0]));
      outValue.get(1).set(Double.parseDouble(split[2]));
      context.write(outKey, outValue);
    }
  }

  public static class IndexedValue {
    public double value;
    public int index;
    public IndexedValue(double value, double index){
      this.value = value;
      this.index = (int) index;
    }
  }

 public static class ReduceToSeqArray extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, VectorWritable> {
   @Override
   protected void reduce(IntWritable key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
     List<IndexedValue> valueList = new ArrayList<IndexedValue>();
     for(DoubleArrayWritable iw : values){
       valueList.add(new IndexedValue(iw.get(0).get(), iw.get(1).get()));
     }
     Vector vec;
     if(valueList.size() > MAX_SIZE / 2){
       vec = new DenseVector(MAX_SIZE);
     } else {
       vec = new SequentialAccessSparseVector(MAX_SIZE, valueList.size());
     }
     for(int i = 0; i < valueList.size(); i++){
       IndexedValue iv = valueList.get(i);
       vec.set(iv.index, iv.value);
       // Free up the memory so we are not holding 2 copies
       valueList.set(i, null);
     }
     context.write(key, new VectorWritable(new NamedVector(vec, "" + key.get())));
   }
 }

  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new TuplesToSequenceFile(), args);
    System.exit(res);
  }


  public final int run(final String[] args) throws Exception {
    Job job = new Job(super.getConf(), "tuples-to-vectors");
    job.setJarByClass(TuplesToSequenceFile.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DoubleArrayWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);

    job.setMapperClass(ReadTuples.class);
    job.setReducerClass(ReduceToSeqArray.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return -1;
    }
  }
}
