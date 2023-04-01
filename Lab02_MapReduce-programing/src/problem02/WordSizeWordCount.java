import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordSizeWordCount {

  public static class WordSizeWordCountMapper
    extends Mapper<Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      // loop for each word in the line
      while (itr.hasMoreTokens()) {
        // retrieve the next word
        String curToken = itr.nextToken();

        // create word size in map reduce object 
        IntWritable wordSize = new IntWritable(curToken.length());

        // write to output of mapper
        context.write(wordSize, new Text(curToken));
      }
    }
  }

  public static class Combiner
      extends Reducer<IntWritable,Text,IntWritable, Text> {
    private IntWritable result = new IntWritable(); 

    public void reduce(IntWritable key, Iterable<Text> values,Context context)
      throws IOException,InterruptedException {
      int sum=0;

      // count the number of words with the same size
      for(Text x: values)
      {
          sum+= 1;
      }

      // set the result to map reduce object
      result.set(sum);

      // write to output of combiner with format <SizeOfWord, NumberOfWords but in Text object>
      context.write(key, new Text(result.toString()));

    }
  }

  public static class WordSizeWordCountReducer
    extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      int sum = 0;

      // gather all NumberOfWords of each SizeOfWord 
      for (Text val : values) {
        sum += Integer.parseInt(val.toString());
      }

      // write to the output with format <SizeOfWord, Total NumberOfWords>
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word size word count");
    job.setJarByClass(WordSizeWordCount.class);
    job.setMapperClass(WordSizeWordCountMapper.class);
    job.setCombinerClass(Combiner.class);
    job.setReducerClass(WordSizeWordCountReducer.class);

    // outputs of mapper have <SizeOfWord, Word> format 
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    // outputs of reducer have <SizeOfWord, TotalNumberOfWords> format
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
