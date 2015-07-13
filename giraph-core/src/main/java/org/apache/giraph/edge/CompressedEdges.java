package org.apache.giraph.edge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CompressedEdges<I extends WritableComparable, E extends Writable>
extends ConfigurableOutEdges<I, E>
implements ReuseObjectsOutEdges<I, E>, Trimmable {

	@Override
	public void initialize(Iterable<Edge<I, E>> edges) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initialize(int capacity) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void add(Edge<I, E> edge) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove(I targetVertexId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Iterator<Edge<I, E>> iterator() {
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
		// TODO Auto-generated method stub
		
	}

}
