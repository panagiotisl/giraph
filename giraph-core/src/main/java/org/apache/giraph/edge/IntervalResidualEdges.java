package org.apache.giraph.edge;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.io.OutputBitStream;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class IntervalResidualEdges <I extends Writable> extends ConfigurableOutEdges<IntWritable, I>
implements ReuseObjectsOutEdges<IntWritable, I>, Trimmable {

	/** Serialized Intervals and Residuals */
	byte[] intervalsResiduals;
	/** Number of edges. */
	private int edgeCount;
	
	@Override
	public void initialize(Iterable<Edge<IntWritable, I>> edges) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			IntArrayList x = new IntArrayList();
//			IntArrayList w = new IntArrayList();
			for(Iterator<Edge<IntWritable, I>> it = edges.iterator(); it.hasNext(); ){
				Edge<IntWritable, I> next = it.next();
				x.add(next.getTargetVertexId().get());
				edgeCount++;
			}
			CompressionUtils.diffComp(x, new OutputBitStream(baos));
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
	public void add(Edge<IntWritable, I> edge) {
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

	@Override
	public Iterator<Edge<IntWritable, I>> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trim() {
		// Nothing to do
	}
	
	
}
