public class CDRCDRConstants {
   public static final int FROM_PHONE_NUMBER = 0;
   public static final int TO_PHONE_NUMBER = 1;
   public static final int CALL_START_TIME = 2;
   public static final int CALL_END_TIME = 3;
   public static final int STD_FLAG = 4;
}

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CallDataRecord {

   public class CDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

      private Text fromPhoneNumber = new Text();
      private IntWritable callDuration = new IntWritable();

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String[] record = value.toString().split("\\|");

         // check if the call is STD and calculate the call duration
         if (record[Constants.STD_FLAG].equals("1")) {
            int duration = calculateDuration(record[Constants.CALL_START_TIME], record[Constants.CALL_END_TIME]);
            fromPhoneNumber.set(record[Constants.FROM_PHONE_NUMBER]);
            callDuration.set(duration);
            context.write(fromPhoneNumber, callDuration);
         }
      }

      // helper method to calculate call duration in minutes
      private int calculateDuration(String startTime, String endTime) {
         LocalDateTime startDateTime = LocalDateTime.parse(startTime,
               DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
         LocalDateTime endDateTime = LocalDateTime.parse(endTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
         Duration duration = Duration.between(startDateTime, endDateTime);
         return (int) duration.toMinutes();
      }
   }

   public class CDRReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

      private IntWritable result = new IntWritable();

      public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         result.set(sum);
         context.write(key, result);
      }
   }

   // public static void main(String[] args) throws Exception {
   // Configuration conf = new Configuration();
   // Job job = Job.getInstance(conf, "Call Data Record");
   // job.setJarByClass(CallDataRecord.class);
   // job.setMapperClass(CDRMapper.class);
   // job.setCombinerClass(CDRReducer.class);
   // job.setReducerClass(CDRReducer.class);
   // job.setOutputKeyClass(Text.class);
   // job.setOutputValueClass(IntWritable.class);
   // FileInputFormat.addInputPath(job, new Path(args[0]));
   // FileOutputFormat.setOutputPath(job, new Path(args[1]));
   // System.exit(job.waitForCompletion(true) ? 0 : 1);
   // }

   public static void main(String[] args) throws Exception {

      if (args.length != 2) {
         System.err.println("Usage: CDRJob <input path> <output path>");
         System.exit(-1);
      }

      Job job = new Job();
      job.setJarByClass(CDRJob.class);
      job.setJobName("CDR Analytics");

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.setMapperClass(CDRMapper.class);
      job.setNumReduceTasks(0);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      boolean success = job.waitForCompletion(true);
      System.exit(success ? 0 : 1);
   }
}
