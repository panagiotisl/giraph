package org.apache.giraph.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.io.OutputBitStream;

public class CompressionUtils {

	
	public static int MIN_INTERVAL_LENGTH = 4;

	/** Default value of <var>k</var>. */
	public final static int DEFAULT_ZETA_K = 3;
	/**
	 * The value of <var>k</var> for &zeta;<sub><var>k</var></sub> coding (for
	 * residuals).
	 */
	protected static int zetaK = DEFAULT_ZETA_K;
	
//	protected static byte[] bytes = {1 << 0, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, (byte) (1 << 7)};

		/**
		 * tests if bit is set in a byte
		 * 
		 * @param my_byte
		 *            the byte to be tested
		 * @param pos
		 *            the position in the byte to be tested
		 * @returns true or false depending on the bit being set
		 * 
		 * */
		public static boolean isSet(byte my_byte, int pos) {
//			if (pos > 7 || pos < 0)
//				throw new IllegalArgumentException("not a valid bit position: " + pos);
			return (my_byte & (1 << pos)) != 0;
//			return (my_byte & CompressionUtils.bytes[pos]) !=0;
		}

		/**
		 * tests if bit is set in a byte
		 * 
		 * @param byteWritable
		 *            the byte to be updated
		 * @param pos
		 *            the position in the byte to be set
		 * @returns the updated byte
		 * 
		 * */
		public static byte set_bit(byte my_byte, int pos) {
//			if (pos > 7 || pos < 0)
//				throw new IllegalArgumentException("not a valid bit position: "
//						+ pos);
			return (byte) (my_byte | (1 << pos));
		}

		/**
		 * tests if bit is set in a byte
		 * 
		 * @param my_byte
		 *            the byte to be updated
		 * @param pos
		 *            the position in the byte to be unset
		 * @returns the updated byte
		 * 
		 * */
		public static byte unset_bit(byte my_byte, int pos) {
//			if (pos > 7 || pos < 0)
//				throw new IllegalArgumentException("not a valid bit position: "
//						+ pos);
			return (byte) (my_byte & ~(1 << pos));
		}
		
		public static byte[] toByteArray(int value) {
			return new byte[] { (byte) (value >> 24), (byte) (value >> 16),
					(byte) (value >> 8), (byte) value };
		}

		public static int fromByteArray(byte[] bytes) {
			return bytes[0] << 24 | (bytes[1] & 0xFF) << 16
					| (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
		}
		
		public static byte[] addBytes(byte[] original, byte[] bindex, byte my_byte){
			byte[] destination = new byte[original.length + 5];
			System.arraycopy(original, 0, destination, 0, original.length);
			System.arraycopy(bindex, 0, destination, original.length, 4);
			destination[destination.length-1] = my_byte;
			return destination;
		}		

		
		/**
		 * This method tries to express an increasing sequence of natural numbers
		 * <code>x</code> as a union of an increasing sequence of intervals and an
		 * increasing sequence of residual elements. More precisely, this
		 * intervalization works as follows: first, one looks at <code>x</code> as a
		 * sequence of intervals (i.e., maximal sequences of consecutive elements);
		 * those intervals whose length is &ge; <code>minInterval</code> are stored
		 * in the lists <code>left</code> (the list of left extremes) and
		 * <code>len</code> (the list of lengths; the length of an integer interval
		 * is the number of integers in that interval). The remaining integers,
		 * called <em>residuals</em> are stored in the <code>residual</code> list.
		 * 
		 * <P>
		 * Note that the previous content of <code>left</code>, <code>len</code> and
		 * <code>residual</code> is lost.
		 * 
		 * @param x
		 *            the list to be intervalized (an increasing list of natural
		 *            numbers).
		 * @param minInterval
		 *            the least length that a maximal sequence of consecutive
		 *            elements must have in order for it to be considered as an
		 *            interval.
		 * @param left
		 *            the resulting list of left extremes of the intervals.
		 * @param len
		 *            the resulting list of interval lengths.
		 * @param residuals
		 *            the resulting list of residuals.
		 * @return the number of intervals.
		 */
		private static <E extends Writable> int intervalize(final IntArrayList edges, final int minInterval, final IntArrayList left, final IntArrayList len, final IntArrayList residuals) {
			int nInterval = 0;
			int vl = edges.size();
			int v[] = edges.elements();
			int i, j;

			for (i = 0; i < vl; i++) {
				j = 0;
				if (i < vl - 1 && v[i] + 1 == v[i + 1]) {
					do
						j++;
					while (i + j < vl - 1 && v[i + j] + 1 == v[i + j + 1]);
					j++;
					// Now j is the number of integers in the interval.
					if (j >= minInterval) {
						left.add(v[i]);
						len.add(j);
						nInterval++;
						i += j - 1;
					}
				}
				if (j < minInterval)
					residuals.add(v[i]);
			}
			return nInterval;
		}
		

		private static int intervalize(IntArrayList edges, IntArrayList weights,
				int minInterval, IntArrayList left, IntArrayList len,
				IntArrayList residuals, IntArrayList intervalWeights, IntArrayList residualWeights) {
			int nInterval = 0;
			int vl = edges.size();
			int v[] = edges.elements();
			int i, j;

			for (i = 0; i < vl; i++) {
				j = 0;
				if (i < vl - 1 && v[i] + 1 == v[i + 1]) {
					do
						j++;
					while (i + j < vl - 1 && v[i + j] + 1 == v[i + j + 1]);
					j++;
					// Now j is the number of integers in the interval.
					if (j >= minInterval) {
						left.add(v[i]);
						len.add(j);
						for(int k=i;k<k+j;k++){
							intervalWeights.add(weights.get(k));
						}
						nInterval++;
						i += j - 1;
					}
				}
				if (j < minInterval)
					residuals.add(v[i]);
					residualWeights.add(weights.get(i));
			}
			return nInterval;
		}		
		
		
		
	public static <E extends Writable> void diffComp(IntArrayList edges, OutputBitStream obs) throws IOException {
		// We write the degree.
		obs.writeInt(edges.size(), 32);
		final int residual[], residualCount;
		IntArrayList left = new IntArrayList();
		IntArrayList len = new IntArrayList();
		IntArrayList residuals = new IntArrayList();
		// If we are to produce intervals, we first compute them.
		final int intervalCount = intervalize(edges, MIN_INTERVAL_LENGTH, left, len, residuals);
		// We write the number of intervals.
		obs.writeGamma(intervalCount);
//		bitsForIntervals += t;

		int currIntLen;
		int prev = 0;
		// TODO decide on this
		if(intervalCount>0){
			obs.writeInt(left.getInt(0), 32);
			currIntLen = len.getInt(0);
			prev = left.getInt(0) + currIntLen;
			obs.writeGamma(currIntLen - MIN_INTERVAL_LENGTH);
		}
		

		
		// We write out the intervals.
		for (int i = 1; i < intervalCount; i++) {
			obs.writeGamma(left.getInt(i) - prev - 1);
//			bitsForIntervals += t;
			currIntLen = len.getInt(i);
			prev = left.getInt(i) + currIntLen;
//			intervalisedArcs += currIntLen;
			obs.writeGamma(currIntLen - MIN_INTERVAL_LENGTH);
//			bitsForIntervals += t;
		}

		residual = residuals.elements();
		residualCount = residuals.size();

		// Now we write out the residuals, if any
		if (residualCount != 0) {
//			residualArcs += residualCount;
//			updateBins(currNode, residual, residualCount, residualGapStats);
			// TODO decide on this
			if(intervalCount>0){
				writeResidual(obs, Fast.int2nat((long) (prev = residual[0]) - left.getInt(0)));
			}
			else{
				obs.writeInt((prev = residual[0]), 32);
			}
//			bitsForResiduals += t;
			for (int i = 1; i < residualCount; i++) {
				if (residual[i] == prev)
					throw new IllegalArgumentException("Repeated successor " + prev + " in successor list of this node");
				writeResidual(obs, residual[i] - prev - 1);
//				bitsForResiduals += t;
				prev = residual[i];
			}

		}
		obs.flush();
	}
	

	
	public static void diffComp(IntArrayList edges, IntArrayList weights, OutputBitStream obs) throws IOException {
		// We write the degree.
		obs.writeInt(edges.size(), 32);
		final int residual[], residualCount;
		IntArrayList left = new IntArrayList();
		IntArrayList len = new IntArrayList();
		IntArrayList residuals = new IntArrayList();
		IntArrayList intervalWeights = new IntArrayList();
		IntArrayList residualWeights = new IntArrayList();
		// If we are to produce intervals, we first compute them.
		final int intervalCount = intervalize(edges, weights, MIN_INTERVAL_LENGTH, left, len, residuals, intervalWeights, residualWeights);
		// We write the number of intervals.
		obs.writeGamma(intervalCount);
//		bitsForIntervals += t;

		int currIntLen;
		int prev = 0;
		int currIntervalEdge = 0;
		// TODO decide on this
		if(intervalCount>0){
			obs.writeInt(left.getInt(0), 32);
			currIntLen = len.getInt(0);
			prev = left.getInt(0) + currIntLen;
			obs.writeGamma(currIntLen - MIN_INTERVAL_LENGTH);
			for(int k = currIntervalEdge ; k < currIntervalEdge + currIntLen; k++){
				obs.writeGamma(intervalWeights.get(k));
			}
			currIntervalEdge+=currIntLen;
		}
		

		
		// We write out the intervals.
		for (int i = 1; i < intervalCount; i++) {
			obs.writeGamma(left.getInt(i) - prev - 1);
			currIntLen = len.getInt(i);
			prev = left.getInt(i) + currIntLen;
			obs.writeGamma(currIntLen - MIN_INTERVAL_LENGTH);
			for(int k = currIntervalEdge ; k < currIntervalEdge + currIntLen; k++){
				obs.writeGamma(intervalWeights.get(k));
			}
			currIntervalEdge+=currIntLen;
		}

		residual = residuals.elements();
		residualCount = residuals.size();

		// Now we write out the residuals, if any
		if (residualCount != 0) {
			// TODO decide on this
			if(intervalCount>0){
				writeResidual(obs, Fast.int2nat((long) (prev = residual[0]) - left.getInt(0)));
			}
			else{
				obs.writeInt((prev = residual[0]), 32);
			}
			obs.writeGamma(residualWeights.get(0));
			for (int i = 1; i < residualCount; i++) {
				if (residual[i] == prev)
					throw new IllegalArgumentException("Repeated successor " + prev + " in successor list of this node");
				writeResidual(obs, residual[i] - prev - 1);
				obs.writeGamma(residualWeights.get(i));
				prev = residual[i];
			}

		}
		obs.flush();
	}

	
	


	/**
	 * Writes a residual to the given stream.
	 * 
	 * @param obs
	 *            a graph-file output bit stream.
	 * @param residual
	 *            the residual.
	 * @return the number of written bits.
	 */
	protected final static int writeResidual(final OutputBitStream obs, final int residual) throws IOException {
//		switch (residualCoding) {
//		case GAMMA:
//			return obs.writeGamma(residual);
//		case ZETA:
			return obs.writeZeta(residual, zetaK);
//		case DELTA:
//			return obs.writeDelta(residual);
//		case GOLOMB:
//			return obs.writeGolomb(residual, zetaK);
//		case NIBBLE:
//			return obs.writeNibble(residual);
//		default:
//			throw new UnsupportedOperationException("The required residuals coding (" + residualCoding + ") is not supported.");
//		}
	}

	
	/**
	 * Writes a residual to the given stream.
	 * 
	 * @param obs
	 *            a graph-file output bit stream.
	 * @param residual
	 *            the residual.
	 * @return the number of written bits.
	 */
	protected final static int writeResidual(final OutputBitStream obs, final long residual) throws IOException {
//		switch (residualCoding) {
//		case GAMMA:
//			return obs.writeLongGamma(residual);
//		case ZETA:
			return obs.writeLongZeta(residual, zetaK);
//		case DELTA:
//			return obs.writeLongDelta(residual);
//		case GOLOMB:
//			return (int) obs.writeLongGolomb(residual, zetaK);
//		case NIBBLE:
//			return obs.writeLongNibble(residual);
//		default:
//			throw new UnsupportedOperationException("The required residuals coding (" + residualCoding + ") is not supported.");
//		}
	}

	public static void resComp(IntArrayList edges, OutputBitStream obs) throws IOException {
		// We write the degree.
		int size = edges.size();
		int prev;
		obs.writeInt(size, 32);
		if (size != 0) {
			obs.writeInt((prev = edges.getInt(0)), 32);
			for (int i = 1; i < size; i++) {
//				if (edges.getInt(i) == prev)
//					throw new IllegalArgumentException("Repeated successor " + prev + " in successor list of this node");
				writeResidual(obs, edges.getInt(i) - prev - 1);
				prev = edges.getInt(i);
			}
		}
		obs.flush();

		
	}
	
	public static int isAlone(Byte my_byte) {
		switch(my_byte){
		case 1:
			return 0;
		case 2:
			return 1;
		case 4:
			return 2;
		case 8:
			return 3;
		case 16:
			return 4;
		case 32:
			return 5;
		case 64:
			return 6;
		case -128:
			return 7;
		default:
			return -1;
		}
		
	}
	
	public static void main(String[] args) {
		
		System.out.println(set_bit((byte)0, 0));
		System.out.println(set_bit((byte)0, 1));
		System.out.println(set_bit((byte)0, 2));
		System.out.println(set_bit((byte)0, 3));
		System.out.println(set_bit((byte)0, 4));
		System.out.println(set_bit((byte)0, 5));
		System.out.println(set_bit((byte)0, 6));
		System.out.println(set_bit((byte)0, 7));
		
		System.out.println();
		
		System.out.println(isAlone(set_bit((byte)0, 0)));
		System.out.println(isAlone(set_bit((byte)0, 1)));
		System.out.println(isAlone(set_bit((byte)0, 2)));
		System.out.println(isAlone(set_bit((byte)0, 3)));
		System.out.println(isAlone(set_bit((byte)0, 4)));
		System.out.println(isAlone(set_bit((byte)0, 5)));
		System.out.println(isAlone(set_bit((byte)0, 6)));
		System.out.println(isAlone(set_bit((byte)0, 7)));
	}

	

	
	
		
}
