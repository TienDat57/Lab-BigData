import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageSalary {

  public static class AverageSalaryMapper
    extends Mapper<Object, Text, Text, FloatWritable> {

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      // split the line with format: dept_id, emp_id (guess), salary
      StringTokenizer itr = new StringTokenizer(value.toString());
      String deptId = itr.nextToken();
      String empId = itr.nextToken();
      String salary = itr.nextToken();

      // write output as format <deptId, salary>
      context.write(
        new Text(deptId),
        new FloatWritable(Float.parseFloat(salary))
      );
    }
  }

  public static class AverageSalaryReducer
    extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(
      Text key,
      Iterable<FloatWritable> values,
      Context context
    ) throws IOException, InterruptedException {
      float sum = 0;
      float count = 0;

      // get total salary of the department and count number of employees as well
      for (FloatWritable val : values) {
        sum += val.get();
        count += 1;
      }

      // write the average salary of the department = sum / count
      context.write(key, new FloatWritable(sum / count));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "average salary");
    job.setJarByClass(AverageSalary.class);
    job.setMapperClass(AverageSalaryMapper.class);
    job.setCombinerClass(AverageSalaryReducer.class);
    job.setReducerClass(AverageSalaryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
  }
}
