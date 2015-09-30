package org.apache.giraph.edge;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.webgraph.AbstractLazyIntIterator;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.LazyIntIterators;

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


public class ResidualEdges extends ConfigurableOutEdges<IntWritable, NullWritable>
implements ReuseObjectsOutEdges<IntWritable, NullWritable>, Trimmable {

	/** Serialized Intervals and Residuals */
	byte[] residuals;
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
			CompressionUtils.resComp(x, obs);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		residuals = baos.toByteArray();
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
			LazyIntIterator liIter = successors(new InputBitStream(residuals));
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
				ibs.position(0);
				d = ibs.readInt(32);
				if (d == 0)
					return LazyIntIterators.EMPTY_ITERATOR;
				return new ResidualIntIterator(ibs, d);
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
			/** The last residual returned. */
			private int next;
			private int[] residuals;

			private ResidualIntIterator(final InputBitStream ibs, final int residualCount) {
				this.residuals = new int[residualCount];
				try {
					residuals[0] = ibs.readInt(32);
					for(int i=1; i<residualCount; i++ )
						residuals[i] = ibs.readZeta(CompressionUtils.DEFAULT_ZETA_K);
					this.next = residuals[0];
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			public int nextInt() {
				if (currentEdge == residuals.length)
					return -1;
				final int result = next;
				if (currentEdge < residuals.length - 1)
					next += residuals[currentEdge] + 1;
				return result;
			}

			@Override
			public int skip(int n) {
				return n;
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
	    int residualEdgesBytesUsed = in.readInt();
	    if (residualEdgesBytesUsed > 0) {
	      // Only create a new buffer if the old one isn't big enough
	      if (residuals == null ||
	          residualEdgesBytesUsed > residuals.length) {
	        residuals = new byte[residualEdgesBytesUsed];
	      }
	      in.readFully(residuals, 0, residualEdgesBytesUsed);
	    }
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeInt(edgeCount);
	    out.writeInt(residuals.length);
	    if (residuals.length > 0) {
	      out.write(residuals, 0, residuals.length);
	    }
	  }

	@Override
	public void trim() {
		// Nothing to do
	}
	
	
}
