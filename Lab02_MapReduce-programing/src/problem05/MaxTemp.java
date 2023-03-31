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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemp {

  public static class MaxTempMapper
    extends Mapper<Object, Text, LongWritable, IntWritable> {

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String year = itr.nextToken();
      String temp = itr.nextToken();

      // split year and temp and write as <year, temp> format
      context.write(new LongWritable(Integer.parseInt(year)), new IntWritable(Integer.parseInt(temp)));
    }
  }

  public static class MaxTempReducer
    extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int max = 0;

      // loop each temp in the same year and find the max temp
      for (IntWritable val : values) {
        if (val.get() > max) {
          max = val.get();
        }
      }

      // outputs will have format <year, maxTemp>
      result.set(max);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MaxTemp.class);
    job.setMapperClass(MaxTempMapper.class);
    job.setReducerClass(MaxTempReducer.class);

    // year is set as key so I set the output key as LongWritable
    job.setOutputKeyClass(LongWritable.class);

    // temp is not so big so I set the output value as IntWritable
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
