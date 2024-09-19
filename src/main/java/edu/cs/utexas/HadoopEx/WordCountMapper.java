package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] parsedText = value.toString().split(","); //value is line of Hadoop text

		if(parsedText.length < 17){
			return;
		}

		 // Extracting GPS coordinates
		String pickupLatitude = parsedText[6];
        String pickupLongitude = parsedText[7];
        String dropoffLatitude = parsedText[8];
        String dropoffLongitude = parsedText[9];

		if(invalidGPS(pickupLatitude, pickupLongitude, dropoffLatitude, dropoffLongitude)){
			String[] timeFormat = parsedText[3].split(":");
			int hourInt = Integer.parseInt(timeFormat[0]); // 00 -> returns 0.
			if(hourInt > -1 && hourInt < 24){
				word.set(timeFormat[0]);
				context.write(word, counter);
			}else{
				return; // Skip since time format is not supported.
			}
		}

		
		/* Starter code */
		// StringTokenizer itr = new StringTokenizer(value.toString());
		// while (itr.hasMoreTokens()) {
		// 	word.set(itr.nextToken());
		// 	context.write(word, counter);
		// }
	}

	// Helper function to check if any of the GPS coordinates contain errors (zeros or empty strings)
    private boolean invalidGPS(String pickupLat, String pickupLon, String dropoffLat, String dropoffLon) {
        return pickupLat.equals("0") || pickupLon.equals("0") || 
               dropoffLat.equals("0") || dropoffLon.equals("0") || 
               pickupLat.isEmpty() || pickupLon.isEmpty() || 
               dropoffLat.isEmpty() || dropoffLon.isEmpty();
    }
}