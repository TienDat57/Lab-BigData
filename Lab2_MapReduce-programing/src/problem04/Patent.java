import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Patent {
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      //Defining a local variable K of type Text
      private Text _key = new Text();
      //Defining a local variable v of type Text
      private Text _value = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         //Converting the record (single line) to String and storing it in a String variable line
         String line = value.toString();
         //StringTokenizer is breaking the record (line) according to the delimiter whitespace
         StringTokenizer tokenizer = new StringTokenizer(line," ");
         //Iterating through all the tokens and forming the key value pair
         while (tokenizer.hasMoreTokens()) {
            String jiten= tokenizer.nextToken();
            _key.set(jiten);
            String jiten1= tokenizer.nextToken();
            _value.set(jiten1);
            //Sending to output collector which inturn passes the same to reducer
            context.write(_key,_value);
         }
      }
   }

   public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         //Defining a local variable sum of type int
         int sum = 0;
         /*
         * Iterates through all the values available with a key and add them together
         * and give the final result as the key and sum of its values
         */
         for(Text x : values)
         {
            sum++;
         }     
         //Dumping the output in context object
         context.write(key, new IntWritable(sum));
      }
   }

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "patent");
      job.setJarByClass(Patent.class);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      Path outputPath = new Path(args[1]);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      outputPath.getFileSystem(conf).delete(outputPath);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}