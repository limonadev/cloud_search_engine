/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.limonadev;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

  public static class FileCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String w = itr.nextToken();
        word.set(Character.toString(w.charAt(0)));
        context.write(word, new IntWritable(w.length()));
      }
    }
  }

  public static class FileCountReducer extends Reducer<Text, FloatWritable, Text, IntWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {

      /*
       * float sum = 0; int size = 0; for (FloatWritable val : values) { sum +=
       * val.get(); size++; } result.set(sum / size); context.write(key, result);
       */
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: pagerank <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "file count");
    job.setJarByClass(PageRank.class);
    job.setMapperClass(FileCountMapper.class);
    job.setCombinerClass(FileCountReducer.class);
    job.setReducerClass(FileCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileSystem hdfs = FileSystem.get(conf);

    Path groupPath = new Path(otherArgs[0]);
    long fileNumber = 0;

    for (int i = 1; i < otherArgs.length - 1; ++i) {
      Path inputPath = new Path(otherArgs[i]);
      FileInputFormat.addInputPath(job, inputPath);
      fileNumber += hdfs.getContentSummary(inputPath).getFileCount();
    }

    try {
      FSDataOutputStream out = hdfs.create(Path.mergePaths(groupPath, new Path("/count.txt")));
      out.writeLong(fileNumber);
      out.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    Path outputDir = new Path(otherArgs[otherArgs.length - 1]);
    FileOutputFormat.setOutputPath(job, outputDir);

    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
