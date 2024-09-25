package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

// /* Task 1 */
// public class WordCountReducer extends  Reducer<Text, IntWritable, Text, IntWritable> {
/* Task 2 & 3 */
public class WordCountReducer extends  Reducer<Text, IntPairWritable, Text, IntPairWritable> {

   private Logger logger = Logger.getLogger(WordCountReducer.class);

   /* Task 2 & 3 */
   public void reduce(Text text, Iterable<IntPairWritable> values, Context context)
           throws IOException, InterruptedException {


   /* Task 1 */
   // public void reduce(Text text, Iterable<IntWritable> values, Context context)
   //          throws IOException, InterruptedException {

      // Task 1
      //  int sumHours = 0;

      //  for(IntWritable value: values){
      //    sumHours += value.get();
      //    // logger.info("sum: " + sumHours);

      //  }
	   
      // Task 2
        int sumErrors = 0;  
        int sumTrips = 0;
      
      // // Task 3
      //    float totalBank = 0;
      //    int totalSeconds = 0;

        
      //   Iterate through all IntPairWritable values associated with this key
        for (IntPairWritable value : values) {
            //Task 2
            sumErrors += value.getTotalErrors();   
            sumTrips += value.getTotalTrips();
            
            // // Task 3
            //    totalBank += value.getTotalBank();
            //    totalSeconds += value.getTotalSeconds();
        }
        
        /* Task 1 */
      //   context.write(text, new IntWritable(sumHours));


        // Task 2
        context.write(text, new IntPairWritable(sumErrors, sumTrips));

      //  /* Task 3 */
      //    context.write(text, new IntPairWritable(totalBank, totalSeconds));
   }

   /* Print out the top five results or store them in a folder/file. The content of the file should include the top-5
but must not be written in order in that file */
}