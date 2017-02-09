/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.pair.PairOfFloats;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import tl.lin.data.pair.PairOfIntFloat;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, PairOfIntFloat, FloatWritable> {
    private String[] sources = null;
    private ArrayList<TopScoredObjects<Integer>> queue;

    @Override
    public void setup(Context context) throws IOException {
      sources = context.getConfiguration().getStrings("sources");
      int k = context.getConfiguration().getInt("n", 100);
      queue = new ArrayList<TopScoredObjects<Integer>>(sources.length);
      for (int i = 0; i < sources.length; i++){
        queue.add(new TopScoredObjects<Integer>(k));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      ArrayListOfFloatsWritable temp = node.getPageRank();
      for (int i = 0; i < sources.length; i++){
        System.out.println(node.getNodeId());
        queue.get(i).add(node.getNodeId(), temp.get(i));
        queue.set(i, queue.get(i)); //***
        // queue.add(node.getNodeId(), node.getPageRank());
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException { //***

      PairOfIntFloat key = new PairOfIntFloat();
      // FloatWritable value = new FloatWritable();
      FloatWritable value = new FloatWritable();
      for (int i = 0; i < sources.length; i++){
        for (PairOfObjectFloat<Integer> pair : queue.get(i).extractAll()) {
          key.set(i, pair.getLeftElement());
          value.set(pair.getRightElement());
          context.write(key, value);
        }
      }
    }
  }

  private static class MyReducer extends
      Reducer<PairOfIntFloat, FloatWritable, IntWritable, Text> {
    private String[] sources = null;
    private static ArrayList<TopScoredObjects<Integer>> queue;

    @Override
    public void setup(Context context) throws IOException {
      sources = context.getConfiguration().getStrings("sources");
      int k = context.getConfiguration().getInt("n", 100);
      queue = new ArrayList<TopScoredObjects<Integer>>(sources.length);
      for (int i = 0; i < sources.length; i++){
        queue.add(new TopScoredObjects<Integer>(k));
      }
    }

    @Override 
    public void reduce(PairOfIntFloat nid, Iterable<FloatWritable> iterable, Context context)
        throws IOException {
      Iterator<FloatWritable> iter = iterable.iterator();
      queue.get(nid.getLeftElement()).add((int) nid.getRightElement(), iter.next().get());
      queue.set(nid.getLeftElement(), queue.get(nid.getLeftElement()));
      // Shouldn't happen. Throw an exception.
      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      Text value = new Text();
      for (int i = 0; i < sources.length; i++){
        // System.out.printf("Source: %d", queue.get(i).extractAll().get(0).getLeftElement());
        for (PairOfObjectFloat<Integer> pair : queue.get(i).extractAll()) {
          key.set(pair.getLeftElement());
          // We're outputting a string so we can control the formatting.
          // value.set(String.format("%.5f", pair.getRightElement()));
          value.set(String.format("%.5f", (float) Math.pow(Math.E,pair.getRightElement())));
          System.out.printf("%.5f %d",(float) Math.pow(Math.E, pair.getRightElement()), pair.getLeftElement());
          // System.out.println(pair.getLeftElement());
          context.write(key, value);
        }
        System.out.println("");
      }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";


  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("sources").create(SOURCES));


    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + sources);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.setStrings("sources", sources);


    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(PairOfIntFloat.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    // Text instead of FloatWritable so we can control formatting

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);
    Path path = new Path(outputPath + "/part-r-00000");
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
      IntWritable id = new IntWritable();
      Text mass = new Text();
      int count = 0;
      while (reader.next(id,mass)){
        if (count == n - 1){
          System.out.printf("%s %d \n\n",mass.toString(),id.get());
          count = 0;
        }
        else if (count == 0){
          System.out.printf("Source: %d \n",id.get());
          count++;
        }
        else {
          System.out.printf("%s %d \n",mass.toString(),id.get());
          count++;
        }
      }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}