import java.io.*;
import java.util.HashMap;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapred.*;
// import org.apache.hadoop.util.*;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // get filename
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            // get document ID
            String docId = fileName.replace(".txt","");

            // get each line of document
            String line = value.toString().replaceAll("\\p{Punct}", "").toLowerCase();

            // Divide lines into tokens
            StringTokenizer tokenizer = new StringTokenizer(line);

            // map output is (word, docId)
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken();
                // don't count the docId word
                if (docId.equals(nextToken)) {
                    continue;
                }
                word.set(nextToken);
                context.write(word, new Text(docId));
            }
        }
    }



    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
            Iterator<Text> vals = values.iterator();
            while (vals.hasNext()) {
                // value for each line is docId (key = word, value = docId)
                String docId = vals.next().toString();

                // get count for each docId from hashMap
                Integer currentCount = hashMap.get(docId);

                // update count for each docId
                if (currentCount == null) {
                    hashMap.put(docId, 1);
                }
                else {
                    currentCount = currentCount + 1;
                    hashMap.put(docId, currentCount);
                }
            }
            // output.collect(key, new Text(hashMap.toString()));

            // Set output format
            boolean isFirst = true;
            StringBuilder toReturn = new StringBuilder();
            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                if(!isFirst) {
                    toReturn.append("\t");
                }
                isFirst = false;
                toReturn.append(entry.getKey()).append(": ").append(entry.getValue());
            }

            context.write(key, new Text(toReturn.toString()));
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(TinyGoogle.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexReducer.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        try {
            job.waitForCompletion(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
