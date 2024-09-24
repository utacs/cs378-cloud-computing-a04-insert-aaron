package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.cs.utexas.HadoopEx.IntPairWritable;

public class WordCountMapper extends Mapper<Object, Text, Text, IntPairWritable> {

	// Create a counter and initialize with 1
	// private final IntWritable counter = new IntWritable(1);
	int trip = 1;
	int error = 0;
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] parsedText = value.toString().split(","); //value is line of Hadoop text

		if(isInvalidLine(parsedText)){
			return;
		}
	
		// Extracting GPS coordinates
		String pickupLatitude = parsedText[6];
		String pickupLongitude = parsedText[7];
		String dropoffLatitude = parsedText[8];
		String dropoffLongitude = parsedText[9];

		if(invalidGPS(pickupLatitude, pickupLongitude, dropoffLatitude, dropoffLongitude)){
			String[] dateInfo = parsedText[2].split(" "); // dateInfo format = 2013-01-01 00:02:00
			String timeFormat = dateInfo[1];
			String [] hourTime = timeFormat.split(":");
			int hourInt = Integer.parseInt(hourTime[0]); // 00 -> returns 0.
			/* Task 1 */
				// if(hourInt > -1 && hourInt < 24){
				// 	word.set(timeFormat[0]);
				// 	context.write(word, counter);
				// }else{
				// 	return; // Skip since time format is not supported.
				// }
			error = 1;
		}

		// /* Task 2 */	
		// IntPairWritable dataPair = new IntPairWritable(error, trip);
		// word.set(parsedText[1]);
		// context.write(word, dataPair); 


		/* Task 3  */
		// float amount = Integer.parseInt(parsedText[16]);
		// int seconds = Integer.parseInt(parsedText[4]);
		// IntPairWritable moneyPair = new IntPairWritable(amount, seconds);
		// context.write(word, moneyPair); 
	}

	// Helper function to check if any of the GPS coordinates contain errors (zeros or empty strings)
    private boolean invalidGPS(String pickupLat, String pickupLon, String dropoffLat, String dropoffLon) {
		return pickupLat == null || pickupLon == null || dropoffLat == null || dropoffLon == null || 
			   pickupLat.trim().equals("0") || pickupLon.trim().equals("0") || 
			   dropoffLat.trim().equals("0") || dropoffLon.trim().equals("0") || 
			   pickupLat.trim().equals("0.0") || pickupLon.trim().equals("0.0") || 
			   dropoffLat.trim().equals("0.0") || dropoffLon.trim().equals("0.0") || 
			   pickupLat.trim().isEmpty() || pickupLon.trim().isEmpty() || 
			   dropoffLat.trim().isEmpty() || dropoffLon.trim().isEmpty();
	}
	

	public boolean isInvalidLine(String [] line){
		if (line.length != 17){
			return true;
		}
		try{
			float fare = Float.parseFloat(line[11]);
			float surcharge = Float.parseFloat(line[12]);
			float mtaTax = Float.parseFloat(line[13]);
			float tip = Float.parseFloat(line[14]);
			float tolls = Float.parseFloat(line[15]);
			float totalAmount = Float.parseFloat(line[16]);

			float calculatedTotal = fare + surcharge + mtaTax + tip + tolls;
			if(Math.abs(calculatedTotal - totalAmount) > .01){
				return true;
			}
		}catch (NumberFormatException e){
			return true;
		}

		return false;
	}
}