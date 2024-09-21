package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, IntPairWritable, Text, IntPairWritable> {

   public void reduce(Text text, Iterable<IntPairWritable> values, Context context)
           throws IOException, InterruptedException {
	   
        int sumErrors = 0;  
        int sumTrips = 0;
        
        // Iterate through all IntPairWritable values associated with this key
        for (IntPairWritable value : values) {
            sumErrors += value.getTotalErrors();   
            sumTrips += value.getTotalTrips();
        }
        
        // Write out the key (DriverID) and the summed IntPairWritable
        context.write(text, new IntPairWritable(sumErrors, sumTrips));
   }

   /* Print out the top five results or store them in a folder/file. The content of the file should include the top-5
but must not be written in order in that file */
}