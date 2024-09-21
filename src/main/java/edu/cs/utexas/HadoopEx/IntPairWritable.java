package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class IntPairWritable implements Writable {
    private int errors;
    private int trips;

    // Default constructor (required by Hadoop)
    public IntPairWritable() {
    }

    public IntPairWritable(int errors, int trips) {
        this.errors = errors;
        this.trips = trips;
    }

    public int getTotalErrors() {
        return errors;
    }

    // public void s(int first) {
    //     this.first = first;
    // }

    public int getTotalTrips() {
        return trips;
    }

    // public void setSecond(int second) {
    //     this.second = second;
    // }

    // Serialization
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(errors);
        out.writeInt(trips);
    }

    // Deserialization
    @Override
    public void readFields(DataInput in) throws IOException {
        errors = in.readInt();
        trips = in.readInt();
    }

    @Override
    public String toString() {
        return errors + "\t" + trips;
    }
}
