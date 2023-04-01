import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class DeIdentifyData {
   private static Logger logger = Logger.getLogger(DeIdentifyData.class);
   public static Integer[] encryptCol = { 2, 3, 4, 5, 6, 8 };
   private static byte[] samplekey = new String("samplekey1234567").getBytes();

   public static class DeIndentifyMap extends Mapper<Object, Text, NullWritable, Text> {
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         // initialize the tokenizer with the input line and the delimiter "," 
         String[] tokens = value.toString().split(",");
         
         // convert the array to a list and then use the Collections.addAll() method to add the array elements to the list
         List<Integer> list = new ArrayList<Integer>();
         Collections.addAll(list, encryptCol);

         String result = "";
         // iterate through the tokens and check if the index of the token is in the list of columns to encrypt 
         // if it is, encrypt the token and append it to the new string 
         // if it is not, append the token to the new string 
         for (int i = 0; i < tokens.length; i++) {
            if (list.contains(i + 1)) {
               if (result.length() > 0)
                  result += ",";
               result += encrypt(tokens[i], samplekey);
            } else {
               if (result.length() > 0)
                  result += ",";
               result += tokens[i];
            }
         }

         // after the loop, write the new string to the context 
         context.write(NullWritable.get(), new Text(result.toString()));
      }
   }	

   public static void main(String[] args) throws Exception {
      if (args.length != 2) {
         System.out.println("usage: [input] [output]");
         System.exit(-1);
      }

      Job job = Job.getInstance(new Configuration());
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      job.setMapperClass(DeIndentifyMap.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setJarByClass(DeIdentifyData.class);
      job.waitForCompletion(true);
   }

   // method to encrypt the data 
   // NOTE: Referenced from folder Lab 02 in drive of the course
   public static String encrypt(String strToEncrypt, byte[] key) {
      try {
         // use the AES algorithm to encrypt the data 
         Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
         final SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
         cipher.init(Cipher.ENCRYPT_MODE, secretKey);
         
         // use the Base64 encoding to encode the encrypted data 
         final String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes()));
         return encryptedString.trim();
      } catch (Exception e) {
         logger.error("Error while encrypting", e);
      }
      return null;
   }
}