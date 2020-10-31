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

  public static class BaseRankMapper extends Mapper<Object, Text, Text, Text> {

    private Text fileKey = new Text();
    private Text rank = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      double baseRank = conf.getDouble("base_rank", -1.0);

      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String filename = fileSplit.getPath().getName();

      fileKey.set(filename);
      rank.set(Double.toString(baseRank));

      context.write(fileKey, rank);
    }
  }

  public static class BaseRankReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> urls, Context context) throws IOException, InterruptedException {

      Text rank = urls.iterator().next();
      context.write(key, rank);
    }
  }

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
      /// If there is no output URLs, it's necessary to appear at least
      context.write(fileKey, fileKey);
    }
  }

  public static class OutUrlReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> urls, Context context) throws IOException, InterruptedException {
      Set<String> seenUrls = new HashSet<>();
      // seenUrls.add(key.toString());

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

  public static class CombineUrlRankMapper extends Mapper<Object, Text, Text, Text> {

    private Text count = new Text();
    private Text rank = new Text();
    private Text urlFrom = new Text();
    private Text urlTo = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      FileSplit fileSplit = (FileSplit) context.getInputSplit();

      String filename = fileSplit.getPath().getName();

      if (itr.countTokens() == 2) { /// This means the line came from Rank files
        urlFrom.set(itr.nextToken());
        rank.set("**" + itr.nextToken() + "**");

        context.write(urlFrom, rank);
      } else if (itr.countTokens() == 3) {
        urlFrom.set(itr.nextToken());
        count.set("##1##");

        context.write(urlFrom, count);
      } else if (itr.countTokens() > 3) {
        urlFrom.set(itr.nextToken());

        while (itr.hasMoreTokens()) {
          String token = itr.nextToken();
          urlTo.set(token);

          if (itr.hasMoreTokens()) {
            context.write(urlFrom, urlTo);
          } else {
            count.set("##" + token + "##");
            context.write(urlFrom, count);
          }
        }
      }
    }
  }

  public static class CombineUrlRankReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    private Pattern countPattern = Pattern.compile("\\#\\#.+\\#\\#", Pattern.CASE_INSENSITIVE);
    private Matcher countMatcher;
    private Pattern rankPattern = Pattern.compile("\\*\\*.+\\*\\*", Pattern.CASE_INSENSITIVE);
    private Matcher rankMatcher;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String count = "", rank = "";
      String urlsAndRank = "";

      for (Text v : values) {
        String raw = v.toString();
        countMatcher = countPattern.matcher(raw);
        rankMatcher = rankPattern.matcher(raw);

        if (countMatcher.find()) {
          count = raw.substring(2, raw.length() - 2);
        } else if (rankMatcher.find()) {
          rank = raw.substring(2, raw.length() - 2);
        } else {
          urlsAndRank += " " + v.toString();
        }
      }

      /// This means the web is not "pointing" other webs
      if (count.equals("1")) {
        result.set(" -1");
        context.write(key, result);
        return;
      }

      double realEntry = Double.parseDouble(rank) / (Integer.parseInt(count) - 1);
      urlsAndRank += " " + Double.toString(realEntry);
      result.set(urlsAndRank);
      context.write(key, result);

    }
  }

  public static class DistributeEntryMapper extends Mapper<Object, Text, Text, Text> {

    private Text destUrl = new Text();
    private Text entry = new Text();
    private Text temp = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      temp.set("0");

      if (itr.countTokens() > 2) {
        String source = itr.nextToken();
        String rawEntry = "";
        while (itr.hasMoreTokens()) {
          rawEntry = itr.nextToken();
        }
        entry.set(rawEntry);

        itr = new StringTokenizer(value.toString());
        itr.nextToken(); // Ignore the first because is the source

        while (itr.hasMoreTokens()) {
          String token = itr.nextToken();
          if (!itr.hasMoreTokens())
            break;

          if (source.equals(token)) {
            destUrl.set(source);
            context.write(destUrl, temp);

            continue;
          }

          destUrl.set(token);
          context.write(destUrl, entry);
        }
      } else if (itr.countTokens() == 2) {
        String source = itr.nextToken();
        destUrl.set(source);

        context.write(destUrl, temp);
      }
    }
  }

  public static class DistributeEntryReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> entries, Context context) throws IOException, InterruptedException {
      double finalRank = 0.0;
      System.out.println(key.toString());
      for (Text entry : entries) {
        String raw = entry.toString();
        finalRank += Double.parseDouble(raw);
        System.out.print(raw + " ");
      }
      System.out.println();
      result.set(Double.toString(finalRank));

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

    String pathPrefix = otherArgs[0] + "results/";

    /// Counting the total url number (files)
    long fileNumber = Integer.parseInt(otherArgs[3]);

    conf.setDouble("base_rank", 1.0 / fileNumber);
    /// Until here

    Job baseRankJob = new Job(conf, "set base rank");
    baseRankJob.setJarByClass(PageRank.class);
    baseRankJob.setMapperClass(BaseRankMapper.class);
    baseRankJob.setReducerClass(BaseRankReducer.class);
    baseRankJob.setOutputKeyClass(Text.class);
    baseRankJob.setOutputValueClass(Text.class);

    Path firstOutput = new Path(pathPrefix + "first_output/");

    FileOutputFormat.setOutputPath(baseRankJob, firstOutput);

    Job urlCountJob = new Job(conf, "url count");
    urlCountJob.setJarByClass(PageRank.class);
    urlCountJob.setMapperClass(OutUrlMapper.class);
    urlCountJob.setReducerClass(OutUrlReducer.class);
    urlCountJob.setOutputKeyClass(Text.class);
    urlCountJob.setOutputValueClass(Text.class);

    Path inputPath = new Path(otherArgs[1]);
    FileInputFormat.addInputPath(baseRankJob, inputPath);
    FileInputFormat.addInputPath(urlCountJob, inputPath);

    // Path outputDir = new Path(otherArgs[otherArgs.length - 1]);
    Path secondOutput = new Path(pathPrefix + "second_output/");
    FileOutputFormat.setOutputPath(urlCountJob, secondOutput);

    /// Executing the set base rank job
    baseRankJob.waitForCompletion(true);
    /// Executing the count of out URLS for each file
    urlCountJob.waitForCompletion(true);

    Job combineJob = new Job(conf, "combine previous outputs");
    combineJob.setJarByClass(PageRank.class);
    combineJob.setMapperClass(CombineUrlRankMapper.class);
    combineJob.setReducerClass(CombineUrlRankReducer.class);
    combineJob.setOutputKeyClass(Text.class);
    combineJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(combineJob, firstOutput);
    FileInputFormat.addInputPath(combineJob, secondOutput);

    Path thirdOutput = new Path(pathPrefix + "third_output/");
    FileOutputFormat.setOutputPath(combineJob, thirdOutput);

    /// Executing the combination of out URLs, counts and ranks
    combineJob.waitForCompletion(true);

    Job distributeJob = new Job(conf, "distribute entries");
    distributeJob.setJarByClass(PageRank.class);
    distributeJob.setMapperClass(DistributeEntryMapper.class);
    distributeJob.setReducerClass(DistributeEntryReducer.class);
    distributeJob.setOutputKeyClass(Text.class);
    distributeJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(distributeJob, thirdOutput);

    Path fourthOutput = new Path(pathPrefix + "fourth_output/");
    FileOutputFormat.setOutputPath(distributeJob, fourthOutput);

    distributeJob.waitForCompletion(true);

    int iterations = Integer.parseInt(otherArgs[4]);
    for (int i = 0; i < iterations; i++) {
      combineJob = new Job(conf, "combine previous outputs");
      combineJob.setJarByClass(PageRank.class);
      combineJob.setMapperClass(CombineUrlRankMapper.class);
      combineJob.setReducerClass(CombineUrlRankReducer.class);
      combineJob.setOutputKeyClass(Text.class);
      combineJob.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(combineJob, fourthOutput);
      FileInputFormat.addInputPath(combineJob, secondOutput);

      thirdOutput = new Path(pathPrefix + "third_output_" + Integer.toString(i + 1) + "_iteration/");
      FileOutputFormat.setOutputPath(combineJob, thirdOutput);

      /// Executing the combination of out URLs, counts and ranks
      combineJob.waitForCompletion(true);

      distributeJob = new Job(conf, "distribute entries");
      distributeJob.setJarByClass(PageRank.class);
      distributeJob.setMapperClass(DistributeEntryMapper.class);
      distributeJob.setReducerClass(DistributeEntryReducer.class);
      distributeJob.setOutputKeyClass(Text.class);
      distributeJob.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(distributeJob, thirdOutput);

      if (i + 1 == iterations) {
        fourthOutput = new Path(otherArgs[2]);
      } else {
        fourthOutput = new Path(pathPrefix + "fourth_output_" + Integer.toString(i + 1) + "_iteration/");
      }
      FileOutputFormat.setOutputPath(distributeJob, fourthOutput);

      distributeJob.waitForCompletion(true);
    }
  }
}