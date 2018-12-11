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

public class TinyGoogle {

    public static HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

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

    public static void mapReduceIndex(Path inPath, Path outPath) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TinyGoogle");
        job.setJarByClass(TinyGoogle.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexReducer.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

        FileOutputStream fout = new FileOutputStream("tiny_google.indx");
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(hashMap);
        fout.close();
        oos.close();
    }

    public static boolean indexExists() {
        if (new File("tiny_google.indx").isFile()) {
            return true;
        }

        return false;
    }

    public static void buildInvertedIndex(int mode) throws IOException, InterruptedException {
        System.out.println("____________________________________________________________________");
        if (mode == 1) {
            System.out.println("Building new Inverted Index.");
        } else {
            System.out.println("Adding new directory to Inverted Index.");
        }
        System.out.println("____________________________________________________________________");
        Scanner kb = new Scanner(System.in);
        System.out.print("Please enter path of a directory to index:\t");
        Path inPath = new Path(kb.nextLine());
        System.out.print("Please enter output path:\t");
        Path outPath = new Path(kb.nextLine());
        System.out.println("____________________________________________________________________");
        if (mode == 1) {
            System.out.println("\tPlease wait while the inverted index is generated ... ");
        } else {
            System.out.println("\tPlease wait while the inverted index is updated ... ");
        }
        System.out.println("____________________________________________________________________");
        try {
            mapReduceIndex(inPath, outPath);
        } catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("____________________________________________________________________");
        return;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        System.out.println("____________________________________________________________________");
        System.out.println("\t\tWelcome to TinyGoogle!");
        System.out.println("____________________________________________________________________");
        int input;
        Scanner kb = new Scanner(System.in);
        if (!indexExists()) {
            while(true) {
                System.out.println("No existing inverted index was found.\nWould you like to generate one now?\n\t1. Yes\n\t2. No");
                input = kb.nextInt();
                if(input > 2 || input < 1) {
                    System.out.println("Invalid option. Please try again.\n");
                } else if (input == 1) {
                    buildInvertedIndex(0);
                    break;
                } else {
                    System.out.println("Note: No queries will be possible until a directory is indexed.");
                    System.out.println("____________________________________________________________________");
                    break;
                }
            }
        }
        else {
            while(true) {
                System.out.println("An index already exists on disk.\nWould you like to use the existing inverted index or build a new one?\n\t1. Use existing\n\t2. Build new");
                input = kb.nextInt();
                if(input > 2 || input < 1){
                    System.out.println("Invalid option. Please try again.\n");
                } else if (input == 2) {
                    System.out.println("The existing inverted index will be deleted.");
                    new File("tiny_google.indx").delete();
                    buildInvertedIndex(0);
                    break;
                } else {
                    System.out.println("Using the existing inverted index.");
                    FileInputStream fin = new FileInputStream("tiny_google.indx");
                    ObjectInputStream ois = new ObjectInputStream(fin);
                    hashMap = (HashMap<String, Integer>) ois.readObject();
                    System.out.println("____________________________________________________________________");
                    break;
                }
            }
        }

        do {
            System.out.println("Please choose an option:\n\t1. Perform a search query \n\t2. Add another directory to the index\n\t3. Quit");
            input = kb.nextInt();
            if(input > 3 || input < 1){
                System.out.println("Not a valid option.\nPlease try again.");
                continue;
            }
            if (input == 1 && !indexExists()) {
                System.out.println("No inverted index exists.\nPlease create an inverted index before attempting a query.");
                System.out.println("____________________________________________________________________");
            } else if (input == 1) {
                // query the inverted index
            } else if (input == 2) {
                new File("tiny_google.indx").delete();
                buildInvertedIndex(1);
            }
        } while(input != 3);
        System.out.println("____________________________________________________________________");
        System.out.println("Goodbye!");
        System.out.println("____________________________________________________________________");

    }
}
