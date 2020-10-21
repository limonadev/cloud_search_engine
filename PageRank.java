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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

  public static class OutUrlMapper extends Mapper<Object, Text, Text, Text> {

    private Text fileKey = new Text();
    private Text url = new Text();

    private Pattern pattern = Pattern.compile("www.[a-zA-Z0-9]+.com", Pattern.CASE_INSENSITIVE);
    private Matcher matcher;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      FileSplit fileSplit = (FileSplit) context.getInputSplit();

      String filename = fileSplit.getPath().getName();
      fileKey.set(filename);

      while (itr.hasMoreTokens()) {
        String word = itr.nextToken();
        matcher = pattern.matcher(word);

        if (matcher.find()) {
          url.set(matcher.group(0));
          context.write(fileKey, url);
        }
      }
    }
  }

  public static class OutUrlReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> urls, Context context) throws IOException, InterruptedException {
      Set<String> seenUrls = new HashSet<>();
      seenUrls.add(key.toString());

      String urlsAndSize = "";
      long size = 0;

      for (Text u : urls) {
        String url = u.toString();
        if (!seenUrls.contains(url)) {
          seenUrls.add(url);
          urlsAndSize += " " + url;
          size++;
        }
      }

      urlsAndSize += " " + Long.toString(size);

      result.set(urlsAndSize);
      context.write(key, result);
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
    job.setMapperClass(OutUrlMapper.class);
    // job.setCombinerClass(OutUrlReducer.class);
    job.setReducerClass(OutUrlReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

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
