package tap.core.io;

import java.io.OutputStream;

public class BinaryComparableOutputStream {
	private OutputStream output;
	
	public BinaryComparableOutputStream(OutputStream output) {
		this.output = output; 
	}
	
	public void writeBool(boolean value, SortOrder order) {
	}
	
	public void writeInt(int value, SortOrder order) {
	}
	
	public void writeLong(long value, SortOrder order) {
	}
	
	public void writeString(String value, SortOrder order) {
	}
	
	public void writeFloat(float value, SortOrder order) {
	}
	
	public void writeDouble(double value, SortOrder order) {
	}
	
	public void writeNull(SortOrder order) {
	}
	
	public void writeBool(boolean value) {
		writeBool(value, SortOrder.ASCENDING);
	}
	
	public void writeInt(int value) {
		writeInt(value, SortOrder.ASCENDING);
	}
	
	public void writeLong(long value) {
		writeLong(value, SortOrder.ASCENDING);
	}
	
	public void writeString(String value) {
		writeString(value, SortOrder.ASCENDING);
	}
	
	public void writeFloat(float value) {
		writeFloat(value, SortOrder.ASCENDING);
	}
	
	public void writeDouble(double value) {
		writeDouble(value, SortOrder.ASCENDING);
	}
	
	public void writeNull() {
		writeNull(SortOrder.ASCENDING);
		
	}
}
