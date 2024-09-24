package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class DriverInfo implements Comparable<DriverInfo> {
    private final Text DriverID;
    private final int gpsErrors;
    private final int totalTrips;
    private final FloatWritable avgErrorRatio;
    private final float totalBank;
    private final int totalSeconds;

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
            int currentRatio = (int) (((float) getTotalErrors() / getTotalTrips()) * 100);
            int otherRatio = (int) (((float) other.getTotalErrors() / other.getTotalErrors()) * 100);
            int diff = currentRatio - otherRatio;
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){
            int currentRatio = (int) (((float) getTotalErrors() / getTotalTrips()) * 100);
            return "("+DriverID.toString() +" , "+ currentRatio+")";
        }
}




