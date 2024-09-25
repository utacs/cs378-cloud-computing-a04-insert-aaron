package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class DriverInfo implements Comparable<DriverInfo> {
    private final Text DriverID;
    private  int gpsErrors;
    private  int totalTrips;
    private  FloatWritable avgErrorRatio;
    private  float totalBank;
    private  int totalSeconds;

    public DriverInfo(Text DriverID, IntPairWritable values) {
       this.DriverID = DriverID;
       this.gpsErrors = values.getTotalErrors();
       this.totalTrips = values.getTotalTrips();
       this.avgErrorRatio = new FloatWritable(0);
       this.totalBank = values.getTotalBank();
       this.totalSeconds = values.getTotalSeconds();
    }

    public DriverInfo(Text DriverID, FloatWritable avgErrorRatio) {
        this.DriverID = DriverID;
        this.avgErrorRatio = avgErrorRatio;
        this.gpsErrors = 0;
        this.totalTrips = 0;
        this.totalBank = 0;
        this.totalSeconds = 0;
    }

    public FloatWritable getAvgErrorRatio() {
        return avgErrorRatio;
    }

    public Text getDriverID() {
        return DriverID;
    }

    // Task 2
    public int getTotalErrors() {
        return gpsErrors;
    }

    public int getTotalTrips() {
        return totalTrips;
    }
    
    public void updateTotals(int gpsErrors, int trips) {
        this.gpsErrors += gpsErrors;
        this.totalTrips += trips;
    }

    // Task 3
    public float getTotalBank() {
        return totalBank;
    }

    public int getTotalSeconds() {
        return totalSeconds;
    }
    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
    @Override  // For sorting in descending order of driver error ratio
        public int compareTo(DriverInfo other) {
            int currentRatio = totalTrips != 0 ? (int) (((float) getTotalErrors() / getTotalTrips()) * 100) : 0;
            int otherRatio = other.getTotalTrips() != 0 ? (int) (((float) other.getTotalErrors() / other.getTotalTrips()) * 100) : 0;
            return Integer.compare(currentRatio, otherRatio);
        }


        public String toString(){
            int currentRatio = (int) (((float) getTotalErrors() / getTotalTrips()) * 100);
            return "("+DriverID.toString() +" , "+ currentRatio+")";
        }
}




