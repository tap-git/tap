package tap.core.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.util.Utf8;

public class Bytes {

	public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
	public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
	public static final int UBYTE_MAX_VALUE = 255;
	public static final byte ESC = 1;
	public static final byte TERM = 0;
	
	private static int putByte(byte[] bytes, int offset, byte val) {
		bytes[offset] = val;
		return offset + 1;
	}
	
	private static int putInt(byte[] bytes, int offset, int val) {
		if (bytes.length - offset < SIZEOF_INT) {
			throw new IllegalArgumentException("Not enough room to put an int at"
					+ " offset " + offset + " in a " + bytes.length + " byte array");
		}
		for(int i= offset + 3; i > offset; i--) {
			bytes[i] = (byte) val;
			val >>>= 8;
		}
		bytes[offset] = (byte) val;
		return offset + SIZEOF_INT;
	}

	private static int toInt(byte[] bytes, int offset) {
		int n = 0;
		for(int i = offset; i < (offset + SIZEOF_INT); i++) {
			n <<= 8;
			n ^= bytes[i] & 0xFF;
		}
		return n;
	}
	  
	private static int putLong(byte[] bytes, int offset, long val) {
		if (bytes.length - offset < SIZEOF_LONG) {
			throw new IllegalArgumentException("Not enough room to put a long at"
					+ " offset " + offset + " in a " + bytes.length + " byte array");
		}
		for(int i = offset + 7; i > offset; i--) {
			bytes[i] = (byte) val;
			val >>>= 8;
		}
		bytes[offset] = (byte) val;
		return offset + SIZEOF_LONG;
	}

	private static long toLong(byte[] bytes, int offset) {
		long n = 0;
		for(int i = offset; i < (offset + SIZEOF_LONG); i++) {
			n <<= 8;
			n ^= bytes[i] & 0xFF;
		}
		return n;
	}
	
	public static int putInt(byte[] bytes, int offset, int val, SortOrder order) {
		if(order == SortOrder.ASCENDING) {
			val = val - Integer.MIN_VALUE;
		} else {
			val = Integer.MAX_VALUE - val; 
		}
		return putInt(bytes, offset, val); 
	}
	
	public static int toInt(byte[] bytes, int offset, SortOrder order) {
		if(order == SortOrder.ASCENDING) {
			return toInt(bytes, offset) + Integer.MIN_VALUE;
		} else {
			return Integer.MAX_VALUE - toInt(bytes, offset);
		}
	}
	
	public static int putLong(byte[] bytes, int offset, long val, SortOrder order) {
		if(order == SortOrder.ASCENDING) {
			val = val - Long.MIN_VALUE;
		} else {
			val = Long.MAX_VALUE - val;
		}
		return putLong(bytes, offset, val);
	}
	
	public static long toLong(byte[] bytes, int offset, SortOrder order) {
		if(order == SortOrder.ASCENDING) {
			return toLong(bytes, offset) + Long.MIN_VALUE;
		} else {
			return Long.MAX_VALUE - toLong(bytes, offset);
		}
	}
	
	public static int putBoolean(byte[] bytes, int offset, boolean val, SortOrder order) {
		int b;
		if(order == SortOrder.ASCENDING)
			b = val ? 1 : 0;
		else
			b = val ? 0 : 1; 
		return putByte(bytes, offset, (byte) b); 
	}
	
	public static boolean toBoolean(byte[] bytes, int offset, SortOrder order) {
		return bytes[offset] == (order == SortOrder.ASCENDING ? 1 : 0);
	}
	
	public static int writeString(String val, OutputStream out, SortOrder order) throws IOException {
		byte[] bytes = new Utf8(val).getBytes();
		int newLength = bytes.length;
		
		int pend = 0;
		for(int i = 0; i < bytes.length; ++i) {
			byte b = bytes[i];
			
			if(b == ESC || b == TERM) { // escape
				++newLength;
				if(pend != i) { // write accumulated bytes
					out.write(bytes, pend, i - pend);
					pend = i;
				}
				out.write(order == SortOrder.ASCENDING ? ESC : UBYTE_MAX_VALUE - ESC);
			}
			
			if(order == SortOrder.DESCENDING) {
				bytes[i] = (byte) (UBYTE_MAX_VALUE - bytes[i]);
			}
		}
		
		if(pend <= bytes.length - 1) {
			out.write(bytes, pend, bytes.length - pend);
		}
		
		out.write(order == SortOrder.ASCENDING ? TERM : UBYTE_MAX_VALUE - TERM);
		newLength += 1;
		
		return newLength;
	}
	
	public static byte[] getBytes(String val, SortOrder order) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream(val.length());
		writeString(val, out, order);
		return out.toByteArray();
	}
	
	public static byte[] getBytes(int val, SortOrder order) {
		byte[] bytes = new byte[4];
		putInt(bytes, 0, val, order);
		return bytes;
	}

	public static byte[] getBytes(long val, SortOrder order) {
		byte[] bytes = new byte[8];
		putLong(bytes, 0, val, order);
		return bytes;
	}
	
	public static byte[] getBytes(boolean val, SortOrder order) {
		byte[] bytes = new byte[1];
		putBoolean(bytes, 0, val, order);
		return bytes;
	}
	
	public static int compare(byte[] b1, int s1, int l1,
			byte[] b2, int s2, int l2) {
		int end1 = s1 + l1;
		int end2 = s2 + l2;
		for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
			int a = (b1[i] & 0xff);
			int b = (b2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return l1 - l2;
	}
	
	public static int compare(byte[] b1, byte[] b2) {
		return compare(b1, 0, b1.length, b2, 0, b2.length);
	}
}
