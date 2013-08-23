package com.wibidata.wibidota;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiReducer;

import java.io.IOException;

/**
 * For each key, takes values that are int arrays containing two 
 * integers and emits the sum of the first integers and the quotient
 * of the sum of second integers and the sum of the first
 */
// Intended for use with StreakCounterGatherer
public class StreakCounterReducer extends KijiReducer<IntWritable, IntArrayWritable,
    IntWritable, Text> {

  @Override
  protected void reduce(IntWritable key, Iterable<IntArrayWritable> values,
                        Context context) throws IOException, InterruptedException {
    double sum1 = 0;
    double sum2 = 0;
    for(IntArrayWritable dw : values){
      sum2 += dw.get(0).get();
      sum1 += dw.get(1).get();
    }
    double mean = sum1 / sum2;
    context.write(key, new Text(mean + "," + sum2));
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return IntWritable.class;
  }
}