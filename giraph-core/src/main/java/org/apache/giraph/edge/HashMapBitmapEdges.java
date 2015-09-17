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

import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * {@link OutEdges} implementation backed by a byte array.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but edge removals are expensive.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class HashMapBitmapEdges
    implements ReuseObjectsOutEdges<IntWritable, NullWritable>, Trimmable {
  /** Serialized edges. */
  private HashMap<Integer, Byte> edgeMap;
  /** Number of edges. */
  private int edgeCount;

  @Override
  public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
	initialize();
    for (Edge<IntWritable, NullWritable> edge : edges) {
    	int id = ((IntWritable) edge.getTargetVertexId()).get();
    	int bucket = id / 8 ;
    	int pos = id % 8;
    	if(edgeMap.containsKey(bucket)){
    		edgeMap.put(bucket, CompressionUtils.set_bit(edgeMap.get(bucket), pos));
    	}
    	else{
    		edgeMap.put(bucket, CompressionUtils.set_bit((byte)0, pos));
    	}
      ++edgeCount;
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
	  edgeMap = new HashMap<Integer, Byte>();
  }

  @Override
  public void add(Edge<IntWritable, NullWritable> edge) {
	int id = ((IntWritable) edge.getTargetVertexId()).get();
  	int bucket = id / 8 ;
  	int pos = id % 8;
  	if(edgeMap.containsKey(bucket)){
  		edgeMap.put(bucket, CompressionUtils.set_bit(edgeMap.get(bucket), pos));
  	}
  	else{
  		edgeMap.put(bucket, CompressionUtils.set_bit((byte)0, pos));
  	}
    ++edgeCount;
  }

  @Override
  public void remove(IntWritable targetVertexId) {
	int id = ((IntWritable) targetVertexId).get();
  	int bucket = id / 8 ;
  	int pos = id % 8;
  	if(edgeMap.containsKey(bucket)){
  		edgeMap.put(bucket, CompressionUtils.unset_bit(edgeMap.get(bucket), pos));
  		if(edgeMap.get(bucket) == 0){
  			edgeMap.remove(bucket);
  		}
  		--edgeCount;	
  	}
  	else{
  		// TODO
  		// consider throwing IllegalOperation exception
  	}
  }

  @Override
  public int size() {
    return edgeCount;
  }

  @Override
  public void trim() {
	  // Nothing to do
  }

  /**
   * Iterator that reuses the same Edge object.
   */
	private class HashMapBitmapEdgeIterator extends
			UnmodifiableIterator<Edge<IntWritable, NullWritable>> {
		/** Wrapped map iterator. */
		private Iterator<Entry<Integer, Byte>> mapIterator = edgeMap.entrySet()
				.iterator();
		/** Representative edge object. */
		private final Edge<IntWritable, NullWritable> representativeEdge =
	            EdgeFactory.create(new IntWritable(), NullWritable.get());
		/** Current bucket */
		private Entry<Integer, Byte> entry = mapIterator.next(); // safe because emptyIterator is created when there are no buckets
		/** Current edge count */
		private int currentEdge = 0;
		/** Current position */
		private int currentPosition = 0;

    @Override
    public boolean hasNext() {
      return currentEdge < edgeCount;
    }

    @Override
    public Edge<IntWritable, NullWritable> next() {
    	if(currentPosition==8){
    		entry = mapIterator.next();
    		currentPosition=0;
    	}
    	int pos = currentPosition;
    	int nextIndex = 0, nextPos = 0;
    	boolean done = false;
    	Byte my_byte = entry.getValue();
    	while(!done){
    		for(int i=pos;i<8;i++){
        		if(CompressionUtils.isSet(my_byte, i)){
        			done = true;
        			nextPos = i;
        			nextIndex = entry.getKey();
        			currentEdge++;
        			currentPosition = i + 1;
        			break;
        		}
        	}
    		if(!done && mapIterator.hasNext()){
    			entry = mapIterator.next();
    			my_byte = entry.getValue();
        		pos = 0;
    		}
    	}
    	representativeEdge.getTargetVertexId().set(nextIndex * 8 + nextPos);
    	return representativeEdge;
    }
  }

  @Override
  public Iterator<Edge<IntWritable, NullWritable>> iterator() {
    if (edgeCount == 0) {
      return Iterators.emptyIterator();
    } else {
      return new HashMapBitmapEdgeIterator();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
	  edgeCount = in.readInt();
	  int hashMapSize = in.readInt();
	  for (int i = 0; i < hashMapSize; ++i) {
		  Integer key = in.readInt();
		  Byte value = in.readByte();
	      edgeMap.put(key, value);
	    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
	  out.writeInt(edgeCount);
	  out.writeInt(edgeMap.size());
	  for (Map.Entry<Integer, Byte> entry : edgeMap.entrySet()) {
		  out.write(entry.getKey());
		  out.write(entry.getValue());
	  }
  }
	
	
}
