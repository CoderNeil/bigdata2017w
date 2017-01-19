/**
 * Bespin: reference implementations of "big data" algorithms
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;

import java.io.IOException;
import java.util.*;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    private static final class MyMapperCount extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private static final PairOfStrings PAIR = new PairOfStrings();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());

//            if (tokens.size() < 2) return;
//            HashMap<String,Integer> wordAppear = new HasMap<String, Integer>();
            Set<String> wordAppear = new HashSet<String>();
            for (int i = 0; i < tokens.size() && i < 40; i++) {
                String word = tokens.get(i);
                if (!wordAppear.contains(word)) {
                    wordAppear.add(word); //check if 1 can be Integer
                    PAIR.set(word, "*");
                    context.write(PAIR, ONE);
                }
            }
            PAIR.set("***", "*");
            context.write(PAIR, ONE);
        }
    }

    private static final class MyCombinerCount extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class MyReducerCount extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class MyMapperPair extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private static final PairOfStrings PAIR = new PairOfStrings();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());

            int totalLines = 0;

//            if (tokens.size() < 2) return;
            ArrayList<String> wordAppear = new ArrayList<String>();
            for (int i = 0; i < tokens.size() && i < 40; i++) {
                String word = tokens.get(i);
                if (!wordAppear.contains(word)) {
                    wordAppear.add(word); //check if 1 can be Integer
                }
            }
            for (int i = 0; i < wordAppear.size(); i++) {
                for (int j = 0; j < wordAppear.size(); j++) {
                    if (i == j) continue;
                    PAIR.set(wordAppear.get(i), wordAppear.get(j));
                    context.write(PAIR, ONE);
                }
            }
        }
    }

    private static final class MyCombinerPair extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class MyReducerPair extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfStrings> {
        //        private static final IntWritable VALUE = new IntWritable();
        private static final PairOfStrings PMIPAIR = new PairOfStrings();
        private float marginal = 0.0f;
        private int threshold = 10;
        private int totalLines = 0;
        private HashMap<String, Integer> wordAppear = new HashMap<String, Integer>();

        @Override
        public void setup(Context context) throws IOException {
            threshold = context.getConfiguration().getInt("threshold", 10);

            Path path = new Path("wordCount/part-r-00000");
//            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            PairOfStrings key = new PairOfStrings();
            IntWritable value = new IntWritable();
            SequenceFile.Reader reader =
                    new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));

            while (reader.next(key, value)) {
                if (key.getLeftElement().equals("***")) {
                    totalLines = Integer.parseInt(value.toString());
                } else {
                    wordAppear.put(key.getLeftElement(), Integer.parseInt(value.toString()));
                }
            }
//            System.out.println("==========================am i reading correctly=========" + totalLines);
            reader.close();

        }

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
//            System.out.println("==========================threshold=========" + threshold + "   sum   " + sum);

            if (sum >= threshold) {
                double numOfX = wordAppear.get(key.getLeftElement());
                double numOfY = wordAppear.get(key.getRightElement());
                double PMI = Math.log10((sum * totalLines) / (numOfX * numOfY));
//                System.out.println("==========================num of x =========" + numOfX);
//                System.out.println("==========================num of y =========" + numOfY);
//                System.out.println("==========================PMI=========" + PMI);
                PMIPAIR.set(String.valueOf(PMI), String.valueOf(sum));
                context.write(key, PMIPAIR);
            }
        }
    }

    private static final class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
        @Override
        public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private PairsPMI() {
    }

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-threshold", metaVar = "[num]", usage = "the threshold")
        int threshold = 10;

        @Option(name = "-textOutput", usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
        boolean textOutput = false;
    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - threshold: " + args.threshold);
        LOG.info(" - text output: " + args.textOutput);

        Job countjob = Job.getInstance(getConf());
        countjob.setJobName(PairsPMI.class.getSimpleName());
        countjob.setJarByClass(PairsPMI.class);

        countjob.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(countjob, new Path(args.input));
        FileOutputFormat.setOutputPath(countjob, new Path("wordCount"));

        countjob.setMapOutputKeyClass(PairOfStrings.class);
        countjob.setMapOutputValueClass(IntWritable.class);
        countjob.setOutputKeyClass(PairOfStrings.class);
        countjob.setOutputValueClass(IntWritable.class);
//        if (args.textOutput) {
//            countjob.setOutputFormatClass(TextOutputFormat.class);
//        } else {
            countjob.setOutputFormatClass(SequenceFileOutputFormat.class);
//        }

        countjob.setMapperClass(MyMapperCount.class);
        countjob.setCombinerClass(MyCombinerCount.class);
        countjob.setReducerClass(MyReducerCount.class);
        countjob.setPartitionerClass(MyPartitioner.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path("wordCount");
        FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        countjob.waitForCompletion(true);
        System.out.println("Count Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


        Job pairjob = Job.getInstance(getConf());
        pairjob.setJobName(PairsPMI.class.getSimpleName());
        pairjob.setJarByClass(PairsPMI.class);

        pairjob.getConfiguration().setInt("threshold", args.threshold);
        pairjob.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(pairjob, new Path(args.input));
        FileOutputFormat.setOutputPath(pairjob, new Path(args.output));

        pairjob.setMapOutputKeyClass(PairOfStrings.class);
        pairjob.setMapOutputValueClass(IntWritable.class);
        pairjob.setOutputKeyClass(PairOfStrings.class);
        pairjob.setOutputValueClass(IntWritable.class);
//        if (args.textOutput) {
            pairjob.setOutputFormatClass(TextOutputFormat.class);
//        } else {
//            pairjob.setOutputFormatClass(SequenceFileOutputFormat.class);
//        }

        pairjob.setMapperClass(MyMapperPair.class);
        pairjob.setCombinerClass(MyCombinerPair.class);
        pairjob.setReducerClass(MyReducerPair.class);
        pairjob.setPartitionerClass(MyPartitioner.class);

        // Delete the output directory if it exists already.
        outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        startTime = System.currentTimeMillis();
        pairjob.waitForCompletion(true);
        System.out.println("Pair Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
    }
}

