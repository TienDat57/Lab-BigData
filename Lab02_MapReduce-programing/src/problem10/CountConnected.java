import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.Stack;

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

public class CountConnected {
   public static class CountConnectedMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
      // _vertex to store the vertex
      private IntWritable _vertex = new IntWritable();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         // StringTokenizer is breaking the record (line) according to the delimiter whitespace
         StringTokenizer itr = new StringTokenizer(value.toString());
         _vertex.set(Integer.parseInt(itr.nextToken()));
         // Iterating through all the tokens and forming the key value pair
         while (itr.hasMoreTokens()) {
            context.write(_vertex, new IntWritable(Integer.parseInt(itr.nextToken())));
         }
      }
   }

   public static class ComponentReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
      // _result to store the result
      private IntWritable _result = new IntWritable();
      // _visited to store the visited vertex, 
      // _stack to store the vertex that need to be visited
      private Set<Integer> _visited = new HashSet<Integer>();
      private Stack<Integer> _stack = new Stack<Integer>();
      // _count to store the number of connected component
      private int _count = 0;

      public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         /**
          * If the vertex is not visited, then we push it into the stack and start to visit it and all the vertex that it is connected to.
          *   If the vertex is visited, then we skip it.
          *   If the vertex is not visited, then we increase the number of connected component by 1.
          *   We also need to clear the _visited and _stack for the next connected component.
          *   Finally, we output the number of connected component in the graph.
          */
         if (!_visited.contains(key.get())) {
            _stack.push(key.get());
            while (!_stack.isEmpty()) {
               int v = _stack.pop();
               if (!_visited.contains(v)) {
                  _visited.add(v);
                  // Iterating through all the values and pushing them into the stack
                  for (IntWritable val : values) {
                     _stack.push(val.get());
                  }
               }
            }
            _count++;
         }
         _result.set(_count);
         context.write(new Text("Number of connected component in graph: "), _result);
      }
   }

   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Connected Components");
      job.setJarByClass(CountConnected.class);
      job.setMapperClass(CountConnectedMapper.class);
      job.setReducerClass(ComponentReducer.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}