package org.apache.giraph.edge;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TestVertexAndEdges.TestComputation;
import org.apache.giraph.io.formats.IntDoubleNullTextInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;
import org.weakref.jmx.com.google.common.io.Resources;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;


/**
 * Tests {@link TestSizeEdges} implementations.
 */
public class TestSizeEdges {

	
	@Test
	public void testNumberEdges() {

	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    // Needed to extract type arguments in ReflectionUtils.
	    giraphConfiguration.setComputationClass(TestComputation.class);
//	    giraphConfiguration.setOutEdgesClass(ByteArrayEdges.class);
	    giraphConfiguration.setOutEdgesClass(IntNullArrayEdges.class);
//	    giraphConfiguration.setOutEdgesClass(HashMapEdges.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
//	    ByteArrayEdges<IntWritable, IntWritable> edges = 
//	    		(ByteArrayEdges<IntWritable, IntWritable>) immutableClassesGiraphConfiguration.createOutEdges();
	    IntNullArrayEdges edges = 
	    		(IntNullArrayEdges) immutableClassesGiraphConfiguration.createOutEdges();
//	    HashMapEdges<IntWritable, IntWritable> edges = 
//	    		(HashMapEdges<IntWritable, IntWritable>) immutableClassesGiraphConfiguration.createOutEdges();
	    
	    
	    // Initial edges list contains parallel edges.
	    List<Edge<IntWritable, DoubleWritable>> initialEdges = Lists.newArrayList(
	        EdgeFactory.create(new IntWritable(0), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(1), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(4), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(5), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(6), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(7), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(3), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(8), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(9), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(10), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(11), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(12), new DoubleWritable(0)),
	        EdgeFactory.create(new IntWritable(2), new DoubleWritable(0)));

	    edges.initialize((Iterable)initialEdges);
	    
		assertEquals(3, edges.size());
		
	}
	
	
	@Test
	public void testIndexedBitmapEdges() {
		long startTime = System.nanoTime();
	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    giraphConfiguration.setComputationClass(TestComputation.class);
	    giraphConfiguration.setOutEdgesClass(IndexedBitmapEdges.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
//    	int size = 1000000;
//    	int size = 2180759;
	    int size = 7414866;
    	List<IndexedBitmapEdges> allEdges = Lists.newArrayListWithCapacity(size);
	    try {
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("uk-2007-05@1000000.txt").getFile()), "UTF-8");
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("hollywood-2011.txt").getFile()), "UTF-8");
	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("indochina-2004.txt").getFile()), "UTF-8");
	    	   while (it.hasNext()) {
	    	     String line = it.nextLine();
	    	     String[] splits = line.split("\t");
					IndexedBitmapEdges edges = 
				    		(IndexedBitmapEdges) immutableClassesGiraphConfiguration.createOutEdges();
					List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayListWithCapacity(splits.length-2);
					for(int i=2;i<splits.length;i++){
						initialEdges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(splits[i]))));
					}
					edges.initialize((Iterable)initialEdges);
//					int count = 0;
//					for(Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
//			    		edgeIter.next();
//			    		count++;
//			    	}		
//					assertEquals(count, initialEdges.size());
					allEdges.add(edges);
	    	   }
	    	   long endTime = System.nanoTime();
	    	   System.out.println("Initialization:" + (endTime - startTime) + "ns");
	    	   System.out.println("Size:"+ allEdges.size());
	    	   it.close();
		} catch (IOException e) {
		}
	    startTime = System.nanoTime();
	    for (IndexedBitmapEdges edges : allEdges){
	    	for(Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
	    		edgeIter.next();
	    	}
	    }
	    long endTime = System.nanoTime();
	    System.out.println("Access:" + (endTime - startTime) + "ns");
	}
	
	
	@Test
	public void testHashMapBitmapEdges() {
		long startTime = System.nanoTime();
	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    giraphConfiguration.setComputationClass(TestComputation.class);
	    giraphConfiguration.setOutEdgesClass(HashMapBitmapEdges.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
//	    int size = 1000000;
	    int size = 2180759;
    	List<HashMapBitmapEdges> allEdges = Lists.newArrayListWithCapacity(size);
	    try {
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("uk-2007-05@1000000.txt").getFile()), "UTF-8");
	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("hollywood-2011.txt").getFile()), "UTF-8");
	    	   while (it.hasNext()) {
	    	     String line = it.nextLine();
	    	     String[] splits = line.split("\t");
	    	     HashMapBitmapEdges edges = 
				    		(HashMapBitmapEdges) immutableClassesGiraphConfiguration.createOutEdges();
					List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayListWithCapacity(splits.length-2);
					for(int i=2;i<splits.length;i++){
						initialEdges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(splits[i]))));
					}
					edges.initialize((Iterable<Edge<IntWritable, NullWritable>>)initialEdges);
//					int count = 0;
//					for(Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
//			    		edgeIter.next();
//			    		count++;
//			    	}		
//					assertEquals(count, initialEdges.size());
					allEdges.add(edges);
	    	   }
	    	   long endTime = System.nanoTime();
	    	   System.out.println("Initialization:" + (endTime - startTime) + "ns");
	    	   System.out.println("Size:"+ allEdges.size());
	    	   it.close();
		} catch (IOException e) {
		}
	    startTime = System.nanoTime();
	    for (HashMapBitmapEdges edges : allEdges){
	    	for(Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
	    		edgeIter.next();
	    	}
	    }
	    long endTime = System.nanoTime();
	    System.out.println("Access:" + (endTime - startTime) + "ns");
	}
	
	@Test
	public void testIntervalResidualEdges() {
		long startTime = System.nanoTime();
	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    giraphConfiguration.setComputationClass(TestComputation.class);
	    giraphConfiguration.setOutEdgesClass(IntervalResidualEdges.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
	    int size = 100000;
//    	int size = 2180759;
//    	int size = 7414866;
    	List<IntervalResidualEdges> allEdges = Lists.newArrayListWithCapacity(size);
	    try {
	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("uk-2007-05@100000.txt").getFile()), "UTF-8");
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("hollywood-2011.txt").getFile()), "UTF-8");
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("indochina-2004.txt").getFile()), "UTF-8");
	    	   while (it.hasNext()) {
	    	     String line = it.nextLine();
	    	     String[] splits = line.split("\t");
					IntervalResidualEdges edges = 
				    		(IntervalResidualEdges) immutableClassesGiraphConfiguration.createOutEdges();
					List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayListWithCapacity(splits.length-2);
					for(int i=2;i<splits.length;i++){
						initialEdges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(splits[i]))));
					}
					edges.initialize((Iterable)initialEdges);
//					int count = 0;
//					for(Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
//			    		edgeIter.next();
//			    		count++;
//			    	}		
//					assertEquals(count, initialEdges.size());
					allEdges.add(edges);
	    	   }
	    	   long endTime = System.nanoTime();
	    	   System.out.println("Initialization:" + (endTime - startTime) + "ns");
	    	   System.out.println("Size:"+ allEdges.size());
	    	   it.close();
		} catch (IOException e) {
		}
	    startTime = System.nanoTime();
		for (IntervalResidualEdges edges : allEdges) {
			for (Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();) {
				edgeIter.next();
			}
		}
	    long endTime = System.nanoTime();
	    System.out.println("Access:" + (endTime - startTime) + "ns");
	}	
	

	@Test
	public void testResidualEdges() {
		long startTime = System.nanoTime();
	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    giraphConfiguration.setComputationClass(TestComputation.class);
	    giraphConfiguration.setOutEdgesClass(ResidualEdges.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
	    int size = 1000000;
//    	int size = 2180759;
//    	int size = 7414866;
    	List<ResidualEdges> allEdges = Lists.newArrayListWithCapacity(size);
	    try {
	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("uk-2007-05@1000000.txt").getFile()), "UTF-8");
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("hollywood-2011.txt").getFile()), "UTF-8");
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("indochina-2004.txt").getFile()), "UTF-8");
	    	   while (it.hasNext()) {
	    	     String line = it.nextLine();
	    	     String[] splits = line.split("\t");
					ResidualEdges edges = 
				    		(ResidualEdges) immutableClassesGiraphConfiguration.createOutEdges();
					List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayListWithCapacity(splits.length-2);
					for(int i=2;i<splits.length;i++){
						initialEdges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(splits[i]))));
					}
					edges.initialize((Iterable)initialEdges);
					allEdges.add(edges);
	    	   }
	    	   long endTime = System.nanoTime();
	    	   System.out.println("Initialization:" + (endTime - startTime) + "ns");
	    	   System.out.println("Size:"+ allEdges.size());
	    	   it.close();
		} catch (IOException e) {
		}
	    startTime = System.nanoTime();
		for (ResidualEdges edges : allEdges) {
			for (Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges
					.iterator(); edgeIter.hasNext();) {
				Edge<IntWritable, NullWritable> a = edgeIter.next();
			}
		}
	    long endTime = System.nanoTime();
	    System.out.println("Access:" + (endTime - startTime) + "ns");
	}	

	
	@Test
	public void testWeightedIntervalResidualEdges() {
		long startTime = System.nanoTime();
	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    giraphConfiguration.setComputationClass(TestComputation.class);
	    giraphConfiguration.setOutEdgesClass(WeightedIntervalResidualEdges.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
    	List<WeightedIntervalResidualEdges> allEdges = Lists.newArrayList();
    	int prev = -1;
	    try {
	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("higgs-retweet_network.gray.ordered.txt").getFile()), "UTF-8");
	    	List<Edge<IntWritable, IntWritable>> initialEdges = null;
	    	WeightedIntervalResidualEdges edges = 
	 		    		(WeightedIntervalResidualEdges) immutableClassesGiraphConfiguration.createOutEdges();
	    	   while (it.hasNext()) {
	    	     String line = it.nextLine();
	    	     String[] splits = line.split(" ");
	    	     if(prev!=Integer.parseInt(splits[0])){
					if (prev != -1) {
						edges.initialize((Iterable) initialEdges);
//						int count = 0;
//						for (Iterator<Edge<IntWritable, IntWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();) {
//							edgeIter.next();
//							count++;
//						}
//						assertEquals(count, initialEdges.size());
						allEdges.add(edges);
						edges = (WeightedIntervalResidualEdges) immutableClassesGiraphConfiguration.createOutEdges();
					}
	    	    	 initialEdges = Lists.newArrayList();	    	    	 
	    	     }
	    	     prev = Integer.parseInt(splits[0]);
//	    	     System.out.println("Adding: "+Integer.parseInt(splits[0])+ " "+Integer.parseInt(splits[1])+ "  " + Integer.parseInt(splits[2]));
	    	     initialEdges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(splits[1])), new IntWritable(Integer.parseInt(splits[2]))));
	    	   }
	    	   edges.initialize((Iterable)initialEdges);
	    	   allEdges.add(edges);
	    	   long endTime = System.nanoTime();
	    	   System.out.println("Initialization:" + (endTime - startTime) + "ns");
	    	   System.out.println("Size:"+ allEdges.size());
	    	   it.close();
		} catch (IOException e) {
		}
	    startTime = System.nanoTime();
	    for (WeightedIntervalResidualEdges edges : allEdges){
	    	for(Iterator<Edge<IntWritable, IntWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
	    		edgeIter.next();
	    	}
	    }
	    long endTime = System.nanoTime();
	    System.out.println("Access:" + (endTime - startTime) + "ns");
	}	

	
	@Test
	public void testByteArrayEdges() {
		long startTime = System.nanoTime();
	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    giraphConfiguration.setComputationClass(TestComputation.class);
	    giraphConfiguration.setOutEdgesClass(ByteArrayEdges.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
//    	int size = 1000000;
//    	int size = 2180759;
//    	int size = 7414866;
    	int size = 18520486;
    	List<ByteArrayEdges<IntWritable, NullWritable>> allEdges = Lists.newArrayListWithCapacity(size);
	    try {
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("uk-2007-05@1000000.txt").getFile()), "UTF-8");
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("hollywood-2011.txt").getFile()), "UTF-8");
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("indochina-2004.txt").getFile()), "UTF-8");
	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("uk-2002.txt").getFile()), "UTF-8");
	    	   while (it.hasNext()) {
	    	     String line = it.nextLine();
	    	     String[] splits = line.split("\t");
	    	     ByteArrayEdges<IntWritable, NullWritable> edges = 
				    		(ByteArrayEdges<IntWritable, NullWritable>) immutableClassesGiraphConfiguration.createOutEdges();
					List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayListWithCapacity(splits.length-2);
					for(int i=2;i<splits.length;i++){
						initialEdges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(splits[i]))));
					}
//					System.out.println("Edges read: " + initialEdges.size());
					edges.initialize((Iterable)initialEdges);
//					int count = 0;
//					for(Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
//			    		edgeIter.next();
//			    		count++;
//			    	}		
//					System.out.println("Edges in: " + count + " " + edges.size());
					allEdges.add(edges);
	    	   }
	    	   long endTime = System.nanoTime();
	    	   System.out.println("Initialization:" + (endTime - startTime) + "ns");
	    	   System.out.println("Size:"+ allEdges.size());
	    	   it.close();
		} catch (IOException e) {
		}
	    startTime = System.nanoTime();
	    for (ByteArrayEdges<IntWritable, NullWritable> edges : allEdges){
	    	for(Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
	    		edgeIter.next();
	    	}
	    }
	    long endTime = System.nanoTime();
	    System.out.println("Access:" + (endTime - startTime) + "ns");
	}
	
	@Test
	public void testHashMapEdges() {
		long startTime = System.nanoTime();
	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    giraphConfiguration.setComputationClass(TestComputation.class);
	    giraphConfiguration.setOutEdgesClass(HashMapEdges.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
//    	int size = 1000000;
    	int size = 2180759;
    	List<HashMapEdges<IntWritable, NullWritable>> allEdges = Lists.newArrayListWithCapacity(size);
	    try {
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("uk-2007-05@1000000.txt").getFile()), "UTF-8");
	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("hollywood-2011.txt").getFile()), "UTF-8");
	    	   while (it.hasNext()) {
	    	     String line = it.nextLine();
	    	     String[] splits = line.split("\t");
	    	     HashMapEdges<IntWritable, NullWritable> edges = 
				    		(HashMapEdges<IntWritable, NullWritable>) immutableClassesGiraphConfiguration.createOutEdges();
					List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayListWithCapacity(splits.length-2);
					for(int i=2;i<splits.length;i++){
						initialEdges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(splits[i]))));
					}
					edges.initialize((Iterable)initialEdges);
					allEdges.add(edges);
	    	   }
	    	   long endTime = System.nanoTime();
	    	   System.out.println("Initialization:" + (endTime - startTime) + "ns");
	    	   System.out.println("Size:"+ allEdges.size());
	    	   it.close();
		} catch (IOException e) {
		}
	    startTime = System.nanoTime();
	    for (HashMapEdges<IntWritable, NullWritable> edges : allEdges){
	    	for(Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
	    		edgeIter.next();
	    	}
	    }
	    long endTime = System.nanoTime();
	    System.out.println("Access:" + (endTime - startTime) + "ns");
	}
	
	
	
	@Test
	public void error(){
	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    giraphConfiguration.setComputationClass(TestComputation.class);
	    giraphConfiguration.setOutEdgesClass(ByteArrayEdges.class);
	    giraphConfiguration.setVertexInputFormatClass(IntDoubleNullTextInputFormat.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
		ByteArrayEdges<IntWritable, NullWritable> edges = 
	    		(ByteArrayEdges<IntWritable, NullWritable>) immutableClassesGiraphConfiguration.createOutEdges();
	    List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayList(
	        EdgeFactory.create(new IntWritable(1)),
	        EdgeFactory.create(new IntWritable(2)));

		edges.initialize((Iterable)initialEdges);
		
		for(Iterator<Edge<IntWritable, NullWritable>> edgeIter = edges.iterator(); edgeIter.hasNext();){
    		edgeIter.next();
    	}
	}
	
}
