package org.apache.giraph.edge;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.io.OutputBitStream;

public class CompressionUtils {

	
	private static int MIN_INTERVAL_LENGTH = 4;

	/** Default value of <var>k</var>. */
	public final static int DEFAULT_ZETA_K = 3;
	/**
	 * The value of <var>k</var> for &zeta;<sub><var>k</var></sub> coding (for
	 * residuals).
	 */
	protected static int zetaK = DEFAULT_ZETA_K;

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
		protected static <E extends Writable> int intervalize(final IntArrayList x, final int minInterval, final IntArrayList left, final IntArrayList len, final IntArrayList residuals) {
			int nInterval = 0;
			int vl = x.size();
			int v[] = x.elements();
			int i, j;

			left.clear();
			len.clear();
			residuals.clear();
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
		
		
		
		
	public static <E extends Writable> void diffComp(IntArrayList edges, OutputBitStream obs) throws IOException {
		// Finally, we write the extra list.
		final int residual[], residualCount;
		IntArrayList left = new IntArrayList();
		IntArrayList len = new IntArrayList();
		IntArrayList residuals = new IntArrayList();
		// If we are to produce intervals, we first compute them.
		final int intervalCount = intervalize(edges, MIN_INTERVAL_LENGTH, left, len, residuals);

		// TODO decide on this
		int currNode = left.getInt(0) - 1;
		
		// We write the number of intervals.
		int t = obs.writeGamma(intervalCount);
//		bitsForIntervals += t;

		int currIntLen;

		int prev = 0;
		// We write out the intervals.
		for (int i = 0; i < intervalCount; i++) {
			if (i == 0)
				t = obs.writeLongGamma(Fast.int2nat((long) (prev = left.getInt(i)) - currNode));
			else
				t = obs.writeGamma(left.getInt(i) - prev - 1);
//			bitsForIntervals += t;
			currIntLen = len.getInt(i);
			prev = left.getInt(i) + currIntLen;
//			intervalisedArcs += currIntLen;
			t = obs.writeGamma(currIntLen - MIN_INTERVAL_LENGTH);
//			bitsForIntervals += t;
		}

		residual = residuals.elements();
		residualCount = residuals.size();

		// Now we write out the residuals, if any
		if (residualCount != 0) {
//			residualArcs += residualCount;
//			updateBins(currNode, residual, residualCount, residualGapStats);
			t = writeResidual(obs, Fast.int2nat((long) (prev = residual[0]) - currNode));
//			bitsForResiduals += t;
			for (int i = 1; i < residualCount; i++) {
				if (residual[i] == prev)
					throw new IllegalArgumentException("Repeated successor " + prev + " in successor list of node " + currNode);
				t = writeResidual(obs, residual[i] - prev - 1);
//				bitsForResiduals += t;
				prev = residual[i];
			}

		}
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

	
		
}
