import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueListeners {

  public static class UniqueListenersMapper
    extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      //split the line by | expression
      String[] parts = value.toString().split("[|]");

      // the first ele is the userId and the second ele is the trackId
      context.write(new Text(parts[1]), new Text(parts[0]));
    }
  }

  public static class UniqueListenersReducer
    extends Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      HashSet<String> set = new HashSet<String>();

      // add all listeners to the set to get all unique listeners
      for (Text val : values) {
        set.add(val.toString());
      }
      context.write(key, new IntWritable(set.size()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "unique listeners");
    job.setJarByClass(UniqueListeners.class);
    job.setMapperClass(UniqueListenersMapper.class);
    job.setReducerClass(UniqueListenersReducer.class);

    // output format of mapper is <trackId, userId>
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // output format of reducer is <trackId, number of unique listeners>
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
