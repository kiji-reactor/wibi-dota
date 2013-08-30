package com.wibidata.wibidota;

import org.apache.avro.mapred.SequenceFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.*;

/**
 * Class to interpret the results of running kmeans with Mahout.
 * Reads (cluster, vector) pairs from wherever kmean dumped them and
 * joins them locally with (vector_id, vector) pairs stored
 * somewhere else to print out the names of the vectors that
 * belong to each cluster. Currently interprets the vectors ids as
 * hero_ids so the output will be cluster -> {heros in that cluster}
 */
public class GetClusters extends Configured implements Tool {

  /**
   * Print out the clusters
   *
   * @param args, the first arguement is the location of the clusteredVectors,
   *              the second the location of the vector_name, vector pairs
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new GetClusters(), args);
    System.exit(res);
  }

  // Crude way to hash the vectors, but I only anticipate having
  // about 100 unique vectors
  private double hashVector(Vector vec){
    return vec.zSum() + vec.norm(2);
  }

  public final int run(final String[] args) throws Exception {
    List<Vector> vecs = new ArrayList<Vector>();
    Configuration conf = super.getConf();
    Path path = new Path(args[0]);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] status = fs.listStatus(path);
    Map<Double, Integer> clusterMapping = new HashMap<Double, Integer>();
    for (int i=0;i<status.length;i++){
      if(!status[i].getPath().getName().contains("part")){
        continue;
      }
      SequenceFile.Reader reader = new SequenceFile.Reader(
          conf,
          SequenceFile.Reader.file(status[i].getPath()));
      IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
      WeightedVectorWritable value = (WeightedVectorWritable) reader.getValueClass().newInstance();
      while (reader.next(key, value)){
        clusterMapping.put(hashVector(value.getVector()), key.get());
        System.out.println("Got Vector: " + key.get());
      }
      reader.close();
    }
    System.out.println("Got clustered vectors");
    System.out.print("Number of vectors: " + clusterMapping.size());

    path = new Path(args[1]);
    status = fs.listStatus(path);
    HashMap<Integer, List<String>> clusters = new HashMap<Integer, List<String>>();
    for (int i=0;i<status.length;i++){
      if(!status[i].getPath().getName().contains("part")){
        continue;
      }
      SequenceFile.Reader reader = new SequenceFile.Reader(
          conf,
          SequenceFile.Reader.file(status[i].getPath()));
      IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
      VectorWritable vec = (VectorWritable) reader.getValueClass().newInstance();
      while (reader.next(key, vec)){
        String hero = DotaValues.getHeroName(key.get());
        System.out.println("Got Vector: " + key.get());
        int cluster =  clusterMapping.get(hashVector(vec.get()));
        if(!clusters.containsKey(cluster)){
          clusters.put(cluster, new ArrayList<String>());
        }
        clusters.get(cluster).add(hero);
      }
      reader.close();
    }
    for(int cluster : clusters.keySet()){
      System.out.println("\nCluster: " + cluster);
      for(String hero : clusters.get(cluster)){
        System.out.println((hero));
      }
    }
    return 0;

  }
}

