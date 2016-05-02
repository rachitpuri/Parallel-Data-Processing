package edu.neu.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable long object which implements a simple, efficient,
 * serialization protocol, based on {@link DataInput} and {@link DataOutput}
 * 
 * @author Rushikesh Badami, Bhavin Vora
 * @modified Adib Alwani
 */
public class LongWritable implements Writable, Cloneable, Comparable<LongWritable> {

	private long value;

	public LongWritable() { }
	
	public LongWritable(long value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readLong();
	}

	@Override
	public int compareTo(LongWritable o) {
		return Long.compare(this.value, o.get());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Writable) {
			LongWritable o = (LongWritable) obj;
			return o.value == this.value;
		}
		return super.equals(obj);
	}
	
	@Override
	public String toString() {
		return String.valueOf(value);
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return new LongWritable(value);
	}

	public long get() {
		return value;
	}

	public void set(long value) {
		this.value = value;
	}
}
