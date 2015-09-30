package org.apache.giraph.edge;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;


public class IntervalResidualEdges extends ConfigurableOutEdges<IntWritable, NullWritable>
implements ReuseObjectsOutEdges<IntWritable, NullWritable>, Trimmable {

	/** Serialized Intervals and Residuals */
	byte[] intervalsResiduals;
	/** Number of edges. */
	private int edgeCount;
	
	@Override
	public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
		ExtendedDataOutput eos =
		        getConf().createExtendedDataOutput();
		try {
			IntArrayList x = new IntArrayList();
			for(Iterator<Edge<IntWritable, NullWritable>> it = edges.iterator(); it.hasNext(); ){
				Edge<IntWritable, NullWritable> next = it.next();
				x.add(next.getTargetVertexId().get());
				edgeCount++;
			}
			intervalResidualCompression(x, eos);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		intervalsResiduals = eos.toByteArray();
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
			private ExtendedDataInput extendedDataInput =
			        getConf().createExtendedDataInput(
			            intervalsResiduals, 0, intervalsResiduals.length);
			/** Representative edge object. */
			  private final Edge<IntWritable, NullWritable> representativeEdge =
			            EdgeFactory.create(new IntWritable());
			/** Current edge count */
			private int currentEdge = 0;
			private int currentLeft;
			private int currentLen = 0;
			private int intervalCount;
			public IntervalResidualEdgeIterator() {
				try {
					intervalCount = extendedDataInput.readInt();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			

	    @Override
	    public boolean hasNext() {
	      return currentEdge < edgeCount;
	    }

		@Override
	    public Edge<IntWritable, NullWritable> next() {
			this.currentEdge++;
			switch(this.currentLen){
			case 0:
				switch(this.intervalCount){
				case 0:
					try {
						representativeEdge.getTargetVertexId().set(extendedDataInput.readInt());
					} catch (IOException canthappen) {
						canthappen.printStackTrace();
					}
					return representativeEdge;
				default:
					try {
						this.currentLeft = extendedDataInput.readInt();
						this.currentLen = extendedDataInput.readByte()  & 0xff;
						intervalCount--;
					} catch (IOException canthappen) {
						canthappen.printStackTrace();
					}
					final int result = this.currentLeft;
					this.currentLen--;
					representativeEdge.getTargetVertexId().set(result);
					return representativeEdge;
					
				}
				default:
					final int result = ++this.currentLeft;
					this.currentLen--;
					representativeEdge.getTargetVertexId().set(result);
					return representativeEdge;
			}
			
//			if(intervalCount<0){
//				try {
//					representativeEdge.getTargetVertexId().set(extendedDataInput.readInt());
//				} catch (IOException canthappen) {
//					canthappen.printStackTrace();
//				}
//				return representativeEdge;
//			}
			
//			if (this.currentLen > 0) {
//				final int result = ++this.currentLeft;
//				this.currentLen--;
//				representativeEdge.getTargetVertexId().set(result);
//				return representativeEdge;
//			} else {
//				if(intervalCount>0){
//					try {
//						this.currentLeft = extendedDataInput.readInt();
////						this.currentLen = extendedDataInputInt.readByte();
//						this.currentLen = extendedDataInput.readByte()  & 0xff;
//						intervalCount--;
//					} catch (IOException canthappen) {
//						canthappen.printStackTrace();
//					}
//					final int result = this.currentLeft;
//					this.currentLen--;
//					representativeEdge.getTargetVertexId().set(result);
//					return representativeEdge;
//				}
//				else{
//					try {
//						representativeEdge.getTargetVertexId().set(extendedDataInput.readInt());
//					} catch (IOException canthappen) {
//						canthappen.printStackTrace();
//					}
//					return representativeEdge;
//				}
//			}
	    }
		
		
		  public void readAll(){
		
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
	
	
	private static <E extends Writable> int intervalize(final IntArrayList edges, final int minInterval, final int maxInterval, final IntArrayList left, final IntArrayList len, final IntArrayList residuals) {
		int nInterval = 0;
		int vl = edges.size();
		int v[] = edges.elements();
		int i, j;

		for (i = 0; i < vl; i++) {
			j = 0;
			if (i < vl - 1 && v[i] + 1 == v[i + 1]) {
				do
					j++;
				while (i + j < vl - 1 && j < maxInterval && v[i + j] + 1 == v[i + j + 1]);
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
	
	
	private static <E extends Writable> void intervalResidualCompression(IntArrayList edges, ExtendedDataOutput eos) throws IOException {
		final int minIntervalLength = 2;
		final int residual[], residualCount;
		IntArrayList left = new IntArrayList();
		IntArrayList len = new IntArrayList();
		IntArrayList residuals = new IntArrayList();
		// If we are to produce intervals, we first compute them.
		final int intervalCount = intervalize(edges, minIntervalLength, 254, left, len, residuals);
		// We write out the intervals.
		eos.writeInt(intervalCount);
		for (int i = 0; i < intervalCount; i++) {
			eos.writeInt(left.getInt(i));
			eos.write(len.getInt(i));
		}
		residual = residuals.elements();
		residualCount = residuals.size();
		for (int i = 0; i< residualCount; i++){
			eos.writeInt(residual[i]);
		}
	}
	
	
}
