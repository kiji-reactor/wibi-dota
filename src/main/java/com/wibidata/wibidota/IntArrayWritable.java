package com.wibidata.wibidota;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Simple extension of ArrayWritable to allow us to store arrays
 * of int in a writable object,
 */
public class IntArrayWritable extends ArrayWritable {

  public IntArrayWritable() {
    super(IntWritable.class);
  }

  public IntArrayWritable(IntWritable[] values) {
    super(IntWritable.class, values);
  }

  public IntWritable get(int idx) {
    return (IntWritable) get()[idx];
  }

}
