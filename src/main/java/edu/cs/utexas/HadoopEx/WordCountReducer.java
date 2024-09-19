package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, IntWritable, Text, IntWritable> {

   public void reduce(Text text, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       int sum = 0;
       
       for (IntWritable value : values) {
           sum += value.get();
       }
       
       context.write(text, new IntWritable(sum));
   }

   /* Print out the top five results or store them in a folder/file. The content of the file should include the top-5
but must not be written in order in that file */
}