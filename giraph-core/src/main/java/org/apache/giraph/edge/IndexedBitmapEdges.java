/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.edge;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * {@link OutEdges} implementation backed by a byte array.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but edge removals are expensive.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class IndexedBitmapEdges
	extends ConfigurableOutEdges<IntWritable, NullWritable>
	implements ReuseObjectsOutEdges<IntWritable, NullWritable>, Trimmable {
  /** Serialized edges. */
  private byte[] serializedEdges;
  /** Number of edges. */
  private int edgeCount;

  
	@Override
	public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
		initialize();
	    HashMap<Integer, Byte> map = new HashMap<Integer, Byte>();
	    for (Edge<IntWritable, NullWritable> edge : edges) {
	    	int id = ((IntWritable) edge.getTargetVertexId()).get();
	    	int bucket = id / 8 ;
	    	int pos = id % 8;
	    	if(map.containsKey(bucket)){
	    		map.put(bucket, CompressionUtils.set_bit(map.get(bucket), pos));
	    	}
	    	else{
	    		map.put(bucket, CompressionUtils.set_bit((byte)0, pos));
	    	}
	      ++edgeCount;
	    }
	    for(Integer bucket : map.keySet()){
	    	serializedEdges = CompressionUtils.addBytes(serializedEdges, CompressionUtils.toByteArray(bucket), map.get(bucket));
	    }
		
	}

	@Override
	public void add(Edge<IntWritable, NullWritable> edge) {
		int intEdge = ((IntWritable) edge.getTargetVertexId()).get();
		int bucket = intEdge / 8;
		int pos = intEdge % 8;
		int i = 0;
		byte[] index = new byte[4];
		boolean done = false;
		while (i < serializedEdges.length) { // if bucket is already there, simply set the appropriate bit
			System.arraycopy(serializedEdges, i, index, 0, 4);
			if (CompressionUtils.fromByteArray(index) == bucket) {
				CompressionUtils.set_bit(serializedEdges[i + 4], pos);
				done = true;
				break;
			}
			i += 5;
		}
		if (!done) { // we need to add a bucket
			serializedEdges = CompressionUtils.addBytes(serializedEdges, CompressionUtils.toByteArray(bucket),
					CompressionUtils.set_bit((byte) 0, pos));
		}
		++edgeCount;
	}

	@Override
	public void remove(IntWritable targetVertexId) {
		int edge = ((IntWritable) targetVertexId).get();
		int bucket = edge / 8;
		int pos = edge % 8;
		int i = 0;
		byte[] index = new byte[4];
		System.out.println("Removing edge " + edge + " with (bucket,pos) = " + bucket + " " + pos);
		while (i < serializedEdges.length) {
			System.arraycopy(serializedEdges, i, index, 0, 4);
			if (CompressionUtils.fromByteArray(index) == bucket) {
				CompressionUtils.unset_bit(serializedEdges[i + 4], pos);
				--edgeCount;
				break;
			}
			i += 5;
		}
	}

  

  @Override
  public void initialize(int capacity) {
    // We have no way to know the size in bytes used by a certain
    // number of edges.
    initialize();
  }

  @Override
  public void initialize() {
	  serializedEdges = new byte[0];
  }


  @Override
  public int size() {
    return edgeCount;
  }

  @Override
  public void trim() {
	  // Nothing to do
  }

  /** Iterator that reuses the same Edge object. */
  private class IndexedBitmapEdgeIterator
      extends UnmodifiableIterator<Edge<IntWritable, NullWritable>> {
    /** Representative edge object. */
    private final Edge<IntWritable, NullWritable> representativeEdge =
            EdgeFactory.create(new IntWritable());
    /** Current edge count */
    private int currentEdge = 0;
    /** Current position */
    private int currentPosition = 0;
    /** Index int */
    private int indexInt;
    /** Current byte */
    private Byte my_byte;
    /** Input for processing the bytes */
    private ExtendedDataInput extendedDataInput =
        getConf().createExtendedDataInput(
            serializedEdges, 0, serializedEdges.length);


    @Override
    public boolean hasNext() {
      return currentEdge < edgeCount;
    }

//    @Override
//    public Edge<IntWritable, NullWritable>  next() {
//    	int bucket = currentPosition / 8;
//    	int pos = currentPosition % 8;
////    	byte[] index = new byte[4];
//    	int nextIndex = 0, nextPos = 0;
//    	boolean done = false;
//    	while(!done){
//    		for(int i=pos;i<8;i++){
//        		if(CompressionUtils.isSet(serializedEdges[bucket*5+4], i)){
////    			if(true){
//        			done = true;
//        			nextPos = i;
////        			System.arraycopy(serializedEdges, bucket*5, index, 0, 4);
////        			nextIndex = CompressionUtils.fromByteArray(index);
//        			nextIndex = 3;
//        			currentEdge++;
//        			currentPosition = bucket * 8 + i + 1;
//        			break;
//        		}
//        	}
//        	bucket++;
//        	pos = 0;
//    	}
//    	representativeEdge.getTargetVertexId().set(nextIndex * 8 + nextPos);
//    	return representativeEdge;
//    }
    
//    @Override
//    public Edge<IntWritable, NullWritable> next() {
//    	if(currentPosition==8){
//    		bucket++;
//    		currentPosition=0;
//    	}
//    	if(currentPosition == 0){
//    		System.arraycopy(serializedEdges, bucket*5, index, 0, 4);
////    		indexInt = java.nio.ByteBuffer.wrap(index).getInt();
//			indexInt = CompressionUtils.fromByteArray(index);
//			my_byte = serializedEdges[bucket*5+4];
//    	}
//    	int pos = currentPosition;
//    	int nextPos = 0;
//    	boolean done = false;
//    	while(!done){
//    		for(int i=pos;i<8;i++){
//        		if(CompressionUtils.isSet(my_byte, i)){
//        			done = true;
//        			nextPos = i;
//        			currentEdge++;
//        			currentPosition = i + 1;
//        			break;
//        		}
//        	}
//    		// TODO check the commented out code
//    		if(!done /*&& mapIterator.hasNext()*/){
//    			bucket++;
//    			System.arraycopy(serializedEdges, bucket*5, index, 0, 4);
//    			indexInt = CompressionUtils.fromByteArray(index);
//    			my_byte = serializedEdges[bucket*5+4];
//        		pos = 0;
//    		}
//    	}
//    	representativeEdge.getTargetVertexId().set(indexInt * 8 + nextPos);
//    	return representativeEdge;
//    }

    @Override
    public Edge<IntWritable, NullWritable> next() {
    	if(currentPosition==8){
    		currentPosition=0;
    	}
    	if(currentPosition == 0){
    		try {
				indexInt = extendedDataInput.readInt();
				my_byte = extendedDataInput.readByte();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	int pos = currentPosition;
    	int nextPos = 0;
    	boolean done = false;
    	while(!done){
    		for(int i=pos;i<8;i++){
        		if(CompressionUtils.isSet(my_byte, i)){
        			done = true;
        			nextPos = i;
        			currentEdge++;
        			currentPosition = i + 1;
        			break;
        		}
        	}
    		// TODO check the commented out code
    		if(!done /*&& mapIterator.hasNext()*/){
        		try {
    				indexInt = extendedDataInput.readInt();
    				my_byte = extendedDataInput.readByte();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
        		pos = 0;
    		}
    	}
    	representativeEdge.getTargetVertexId().set(indexInt * 8 + nextPos);
    	return representativeEdge;
    }
    
  }

  @Override
  public Iterator<Edge<IntWritable, NullWritable>> iterator() {
    if (edgeCount == 0) {
      return Iterators.emptyIterator();
    } else {
      return new IndexedBitmapEdgeIterator();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    edgeCount = in.readInt();
    int serializedEdgesBytesUsed = in.readInt();
    if (serializedEdgesBytesUsed > 0) {
      // Only create a new buffer if the old one isn't big enough
      if (serializedEdges == null ||
          serializedEdgesBytesUsed > serializedEdges.length) {
        serializedEdges = new byte[serializedEdgesBytesUsed];
      }
      in.readFully(serializedEdges, 0, serializedEdgesBytesUsed);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(edgeCount);
    out.writeInt(serializedEdges.length);
    if (serializedEdges.length > 0) {
      out.write(serializedEdges, 0, serializedEdges.length);
    }
  }
  
  
}
