package org.apache.giraph.edge;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.webgraph.AbstractLazyIntIterator;
import it.unimi.dsi.webgraph.IntIntervalSequenceIterator;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.LazyIntIterators;
import it.unimi.dsi.webgraph.MergedIntIterator;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.utils.CompressionUtils;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;


public class BitIntervalResidualEdges extends ConfigurableOutEdges<IntWritable, NullWritable>
implements ReuseObjectsOutEdges<IntWritable, NullWritable>, Trimmable {

	/** Serialized Intervals and Residuals */
	byte[] intervalsResiduals;
	/** Number of edges. */
	private int edgeCount;
	
	@Override
	public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			IntArrayList x = new IntArrayList();
			for(Iterator<Edge<IntWritable, NullWritable>> it = edges.iterator(); it.hasNext(); ){
				Edge<IntWritable, NullWritable> next = it.next();
				x.add(next.getTargetVertexId().get());
				edgeCount++;
			}
			OutputBitStream obs = new OutputBitStream(baos);
			CompressionUtils.diffComp(x, obs);
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
	public void add(Edge<IntWritable, NullWritable> edge) {
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
				UnmodifiableIterator<Edge<IntWritable, NullWritable>> {
			/** Wrapped map iterator. */
			LazyIntIterator liIter = successors(new InputBitStream(intervalsResiduals));
			/** Representative edge object. */
			  private final Edge<IntWritable, NullWritable> representativeEdge =
			            EdgeFactory.create(new IntWritable());
			/** Current edge count */
			private int currentEdge = 0;

	    @Override
	    public boolean hasNext() {
	      return currentEdge < edgeCount;
	    }

	    private LazyIntIterator successors(InputBitStream ibs) {
	    	try {
				final int d;
				int extraCount;
				int firstIntervalNode = -1;
				ibs.position(0);
				d = ibs.readInt(32);
				if (d == 0)
					return LazyIntIterators.EMPTY_ITERATOR;
				extraCount = d;
				int intervalCount = 0; // Number of intervals
				int[] left = null;
				int[] len = null;
				if (extraCount > 0) {
					// Prepare to read intervals, if any
					if ((intervalCount = ibs.readGamma()) != 0) {
						int prev = 0; // Holds the last integer in the last
										// interval.
						left = new int[intervalCount];
						len = new int[intervalCount];
						// Now we read intervals
						// left[0] = prev = (int)
						// (Fast.nat2int(ibs.readLongGamma()) + x);
						left[0] = firstIntervalNode = prev = ibs.readInt(32);
						len[0] = ibs.readGamma()
								+ CompressionUtils.MIN_INTERVAL_LENGTH;

						prev += len[0];
						extraCount -= len[0];
						for (int i = 1; i < intervalCount; i++) {
							left[i] = prev = ibs.readGamma() + prev + 1;
							len[i] = ibs.readGamma()
									+ CompressionUtils.MIN_INTERVAL_LENGTH;
							prev += len[i];
							extraCount -= len[i];
						}
					}
				}

				final int residualCount = extraCount; // Just to be able to use
														// an
														// anonymous class.
				final LazyIntIterator residualIterator = residualCount == 0 ? null
						: new ResidualIntIterator(ibs, residualCount,
								firstIntervalNode);
				// The extra part is made by the contribution of intervals, if
				// any, and by the residuals iterator.
				final LazyIntIterator extraIterator = intervalCount == 0 ? residualIterator
						: (residualCount == 0 ? (LazyIntIterator) new IntIntervalSequenceIterator(
								left, len)
								: (LazyIntIterator) new MergedIntIterator(
										new IntIntervalSequenceIterator(left,
												len), residualIterator));
				return extraIterator;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			// return null;
		}

		@Override
	    public Edge<IntWritable, NullWritable> next() {
	    	representativeEdge.getTargetVertexId().set(liIter.nextInt());
	    	currentEdge++;
	    	return representativeEdge;
	    }
		
		/** An iterator returning the residuals of a node. */
		private final class ResidualIntIterator extends AbstractLazyIntIterator {
			/** The input bit stream from which residuals will be read. */
			private final InputBitStream ibs;
			/** The last residual returned. */
			private int next;
			/** The number of remaining residuals. */
			private int remaining;

			private ResidualIntIterator(final InputBitStream ibs, final int residualCount, int x) {
				this.remaining = residualCount;
				this.ibs = ibs;
				try {
					if(x >= 0){
						long temp = Fast.nat2int(ibs.readLongZeta(CompressionUtils.DEFAULT_ZETA_K));
						this.next = (int) (x + temp);
					}
					else{
						this.next = ibs.readInt(32);
					}
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

			@Override
			public int skip(int n) {
				if (n >= remaining) {
					n = remaining;
					remaining = 0;
					return n;
				}
				try {
					for (int i = n; i-- != 0;)
						next += ibs.readZeta(CompressionUtils.DEFAULT_ZETA_K) + 1;
					remaining -= n;
					return n;
				} catch (IOException cantHappen) {
					throw new RuntimeException(cantHappen);
				}
			}

		}

	  }

	  @Override
	  public Iterator<Edge<IntWritable, NullWritable>> iterator() {
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
