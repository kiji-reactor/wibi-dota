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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Simple extension of ArrayWritable to allow us to store arrays
 * of int in a writable object,
 */
public class DoubleArrayWritable extends ArrayWritable {

  public DoubleArrayWritable() {
    super(IntWritable.class);
  }

  public DoubleArrayWritable(IntWritable[] values) {
    super(IntWritable.class, values);
  }

  public IntWritable get(int idx) {
    return (IntWritable) get()[idx];
  }

}
