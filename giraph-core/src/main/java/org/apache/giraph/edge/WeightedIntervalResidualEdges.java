package org.apache.giraph.edge;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.webgraph.MergedIntIterator;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.utils.CompressionUtils;
import org.apache.giraph.utils.LazyIntIntIterator;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;


public class WeightedIntervalResidualEdges extends ConfigurableOutEdges<IntWritable, IntWritable>
implements ReuseObjectsOutEdges<IntWritable, IntWritable>, Trimmable {

	/** Serialized Intervals and Residuals */
	byte[] intervalsResiduals;
	/** Number of edges. */
	private int edgeCount;
	
	@Override
	public void initialize(Iterable<Edge<IntWritable, IntWritable>> edges) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			IntArrayList x = new IntArrayList();
			IntArrayList w = new IntArrayList();
			for(Iterator<Edge<IntWritable, IntWritable>> it = edges.iterator(); it.hasNext(); ){
				Edge<IntWritable, IntWritable> next = it.next();
				x.add(next.getTargetVertexId().get());
				w.add(next.getValue().get());
				edgeCount++;
			}
			OutputBitStream obs = new OutputBitStream(baos);
			CompressionUtils.diffComp(x, w, obs);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		intervalsResiduals = baos.toByteArray();
	}

	@Override
	public void initialize(int capacity) {
		// Nothing to do
	}

	@Override
	public void initialize() {
		// Nothing to do
		
	}

	@Override
	public void add(Edge<IntWritable, IntWritable> edge) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove(IntWritable targetVertexId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int size() {
		return edgeCount;
	}

	  /**
	   * Iterator that reuses the same Edge object.
	   */
		private class IntervalResidualEdgeIterator extends
				UnmodifiableIterator<Edge<IntWritable, IntWritable>> {
			/** Wrapped map iterator. */
			LazyIntIntIterator liIter = successors(new InputBitStream(intervalsResiduals));
			/** Representative edge object. */
			  private final Edge<IntWritable, IntWritable> representativeEdge =
			            EdgeFactory.create(new IntWritable(), new IntWritable());
			/** Current edge count */
			private int currentEdge = 0;

	    @Override
	    public boolean hasNext() {
	      return currentEdge < edgeCount;
	    }

	    private LazyIntIntIterator successors(InputBitStream ibs) {
	    	try {
				final int d;
				int extraCount;
				int firstIntervalNode = -1;
				ibs.position(0);
				d = ibs.readInt(32);
				if (d == 0)
					return new LazyIntIntIterator() {
						public int nextInt() { return -1; }
						public int nextWeight() { return -1; }
					};
				extraCount = d;
				int intervalCount = 0; // Number of intervals
				int[] left = null;
				int[] len = null;
				IntArrayList intervalWeights = new IntArrayList();
				if (extraCount > 0) {
					// Prepare to read intervals, if any
					if ((intervalCount = ibs.readGamma()) != 0) {
						int prev = 0; // Holds the last integer in the last
										// interval.
						left = new int[intervalCount];
						len = new int[intervalCount];
						// Now we read intervals
						left[0] = firstIntervalNode = prev = ibs.readInt(32);
						len[0] = ibs.readGamma()
								+ CompressionUtils.MIN_INTERVAL_LENGTH;
						for(int k=0;k<len[0];k++){
							intervalWeights.add(ibs.readGamma());
						}
						prev += len[0];
						extraCount -= len[0];

						for (int i = 1; i < intervalCount; i++) {
							left[i] = prev = ibs.readGamma() + prev + 1;
							len[i] = ibs.readGamma()
									+ CompressionUtils.MIN_INTERVAL_LENGTH;
							for(int k=0;k<len[i];k++){
								intervalWeights.add(ibs.readGamma());
							}
							prev += len[i];
							extraCount -= len[i];
						}
					}
				}

				final int residualCount = extraCount; // Just to be able to use
														// an
														// anonymous class.
				final LazyIntIntIterator residualIterator = residualCount == 0 ? null
						: new ResidualIntIntIterator(ibs, residualCount,
								firstIntervalNode);
				// The extra part is made by the contribution of intervals, if
				// any, and by the residuals iterator.
				final LazyIntIntIterator extraIterator = intervalCount == 0 ? residualIterator
						: (residualCount == 0 ? (LazyIntIntIterator) new IntIntIntervalSequenceIterator(
								left, len, intervalWeights)
								: (LazyIntIntIterator) new MergedIntIntIterator(
										new IntIntIntervalSequenceIterator(left,
												len, intervalWeights), residualIterator));
				return extraIterator;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		@Override
	    public Edge<IntWritable, IntWritable> next() {
	    	representativeEdge.getTargetVertexId().set(liIter.nextInt());
	    	representativeEdge.getValue().set(liIter.nextWeight());
	    	currentEdge++;
	    	return representativeEdge;
	    }
		
		
		/** An iterator returning the residuals of a node. */
		private final class ResidualIntIntIterator implements LazyIntIntIterator {
			/** The input bit stream from which residuals will be read. */
			private final InputBitStream ibs;
			/** The last residual returned. */
			private int next;
			/** The last residual weight returned. */
			private int nextWeight;
			/** The number of remaining residuals. */
			private int remaining;

			private ResidualIntIntIterator(final InputBitStream ibs, final int residualCount, int x) {
				this.remaining = residualCount;
				this.ibs = ibs;
				try {
					if(x > 0){
						long temp = Fast.nat2int(ibs.readLongZeta(CompressionUtils.DEFAULT_ZETA_K));
						this.next = (int) (x + temp);
					}
					else{
						this.next = ibs.readInt(32);
					}
					this.nextWeight = ibs.readGamma();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			public int nextInt() {
				if (remaining == 0)
					return -1;
				try {
					final int result = next;
					if (--remaining != 0)
						next += ibs.readZeta(CompressionUtils.DEFAULT_ZETA_K) + 1;
					return result;
				} catch (IOException cantHappen) {
					throw new RuntimeException(cantHappen);
				}
			}
			
			public int nextWeight(){
				final int result = nextWeight;
				try {
					if (remaining != 0)
						nextWeight = ibs.readGamma();
				} catch (IOException cantHappen) {
					throw new RuntimeException(cantHappen);
				}
				return result;
			}

		}
		
		
		/** An iterator returning the integers contained in a sequence of intervals. */
		public class IntIntIntervalSequenceIterator implements LazyIntIntIterator {
			
			/** The left extremes. */
			private final int left[];
			/** The lengths. */
			private final int len[];
			/** The number of remaining intervals (including the current one). It is zero exactly when the iterator is exhausted. */
			private int remaining;
			/** The index of the current interval. */
			private int currInterval;
			/** The current position in the current interval: the next integer to be output is {@link #currLeft} + {@link #currIndex}. */
			private int currIndex;
			/** The left point of the current interval. */
			private int currLeft;
			/** The current position in the weight list. */
			private int currWeightIndex;
			private IntArrayList weights;

			/** Creates a new interval-sequence iterator by specifying
			 * arrays of left extremes and lengths. Note that the two arrays are <em>not</em> copied, 
			 * so they are supposed not to be changed during the iteration.
			 * 
			 * @param left an array containing the left extremes of the intervals generating this iterator.
			 * @param len an array (of the same length as <code>left</code>) containing the number of integers (greater than zero) in each interval.
			 */

			public IntIntIntervalSequenceIterator( final int left[], final int len[], final IntArrayList weights ) {
				this( left, len, left.length, weights );
			}
			
			/** Creates a new interval-sequence iterator by specifying
			 * arrays of left extremes and lengths, and the number of valid entries. Note that the two arrays are <em>not</em> copied, 
			 * so they are supposed not to be changed during the iteration.
			 * 
			 * @param left an array containing the left extremes of the intervals generating this iterator.
			 * @param len an array (of the same length as <code>left</code>) containing the number of integers (greater than zero) in each interval.
			 * @param n the number of valid entries in <code>left</code> and <code>len</code>.
			 */

			public IntIntIntervalSequenceIterator( final int left[], final int len[], final int n, final IntArrayList weights ) {
				this.left = left;
				this.len = len;
				this.remaining = n;
				this.weights = weights;
				if ( n != 0 ){
					currLeft = left[ 0 ];
				}
			}
			
			private void advance() {
				remaining--;
				if ( remaining != 0 ) currLeft = left[ ++currInterval ];
				currIndex = 0;
			}

			public int nextInt() {
				if ( remaining == 0 ) return -1;

				final int next = currLeft + currIndex++;
				if ( currIndex == len[ currInterval ] ) advance();
				return next;
			}
			
			public int nextWeight() {
				final int nextWeight = weights.get(currWeightIndex);
				currWeightIndex++;
				return nextWeight;
			}

		}
		
		/** An iterator returning the union of the integers returned by two {@link IntIterator}s.
		 *  The two iterators must return integers in an increasing fashion; the resulting
		 *  {@link MergedIntIterator} will do the same. Duplicates will be eliminated.
		 */

		public class MergedIntIntIterator implements LazyIntIntIterator {
			/** The first component iterator. */
			private final ResidualIntIntIterator it0;
			/** The second component iterator. */
			private final IntIntIntervalSequenceIterator it1;
			/** The last integer returned by {@link #it0}. */
			private int curr0;
			/** The last integer returned by {@link #it1}. */
			private int curr1;
			/** The last weight returned by {@link #it0}. */
			private int currWeight0;
			/** The last weight returned by {@link #it1}. */
			private int currWeight1;
			/** The current weight to be returned. */
			private int currWeight;

			/** Creates a new merged iterator by merging two given iterators; the resulting iterator will not emit more than <code>n</code> integers.
			 * 
			 * @param it0 the first (monotonically nondecreasing) component iterator.
			 * @param it1 the second (monotonically nondecreasing) component iterator.
			 */
			public MergedIntIntIterator( final LazyIntIntIterator it0, final LazyIntIntIterator it1 ) {
				this.it0 = (ResidualIntIntIterator)it0;
				this.it1 = (IntIntIntervalSequenceIterator)it1;
				curr0 = it0.nextInt();
				curr1 = it1.nextInt();
				currWeight0 = this.it0.nextWeight();
				currWeight1 = this.it1.nextWeight();
			}

			public int nextInt() {
				if ( curr0 < curr1 ) {
					if ( curr0 == -1 ) {
						final int result = curr1;
						currWeight = currWeight1;
						curr1 = it1.nextInt();
						currWeight1 = it1.nextWeight();
						return result;
					}
					
					final int result = curr0;
					currWeight = currWeight0;
					curr0 = it0.nextInt();
					currWeight0 = it0.nextWeight();
					return result;
				} 
				else {
					if ( curr1 == -1 ) {
						final int result = curr0;
						currWeight = currWeight0;
						curr0 = it0.nextInt();
						currWeight0 = it0.nextWeight();
						return result;
					}
					
					final int result = curr1;
					currWeight = currWeight1;
					if ( curr0 == curr1 ){
						curr0 = it0.nextInt();
						currWeight0 = it0.nextWeight();
					}
					curr1 = it1.nextInt();
					currWeight1 = it1.nextWeight();
					return result;
				}
			}

			@Override
			public int nextWeight() {
				return currWeight;
			}
		}


	  }

	  @Override
	  public Iterator<Edge<IntWritable, IntWritable>> iterator() {
	    if (edgeCount == 0) {
	      return Iterators.emptyIterator();
	    } else {
	      return new IntervalResidualEdgeIterator();
	    }
	  }



	  @Override
	  public void readFields(DataInput in) throws IOException {
	    edgeCount = in.readInt();
	    int intervalResidualEdgesBytesUsed = in.readInt();
	    if (intervalResidualEdgesBytesUsed > 0) {
	      // Only create a new buffer if the old one isn't big enough
	      if (intervalsResiduals == null ||
	          intervalResidualEdgesBytesUsed > intervalsResiduals.length) {
	        intervalsResiduals = new byte[intervalResidualEdgesBytesUsed];
	      }
	      in.readFully(intervalsResiduals, 0, intervalResidualEdgesBytesUsed);
	    }
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeInt(edgeCount);
	    out.writeInt(intervalsResiduals.length);
	    if (intervalsResiduals.length > 0) {
	      out.write(intervalsResiduals, 0, intervalsResiduals.length);
	    }
	  }

	@Override
	public void trim() {
		// Nothing to do
	}
	
	
}
