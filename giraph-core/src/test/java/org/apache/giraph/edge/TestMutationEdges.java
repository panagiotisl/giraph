package org.apache.giraph.edge;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TestVertexAndEdges.TestComputation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;
import org.weakref.jmx.com.google.common.io.Resources;

import com.google.common.collect.Lists;

public class TestMutationEdges {

	
	
	@Test
	public void testByteArrayRemoval(){
		Random generator = new Random(23);
		long startTime = System.nanoTime();
	    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
	    giraphConfiguration.setComputationClass(TestComputation.class);
	    giraphConfiguration.setOutEdgesClass(ByteArrayEdges.class);
	    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
	    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
//    	int size = 1000000;
    	int size = 2180759;
//    	int size = 7414866;
    	List<ByteArrayEdges<IntWritable, NullWritable>> allEdges = Lists.newArrayListWithCapacity(size);
    	List<Edge<IntWritable, NullWritable>> allRemovals = Lists.newArrayListWithCapacity(size);
	    try {
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("uk-2007-05@1000000.txt").getFile()), "UTF-8");
	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("hollywood-2011.txt").getFile()), "UTF-8");
//	    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("indochina-2004.txt").getFile()), "UTF-8");
	    	   while (it.hasNext()) {
	    	     String line = it.nextLine();
	    	     String[] splits = line.split("\t");
	    	     ByteArrayEdges<IntWritable, NullWritable> edges = 
				    		(ByteArrayEdges<IntWritable, NullWritable>) immutableClassesGiraphConfiguration.createOutEdges();
					List<Edge<IntWritable, NullWritable>> initialEdges = Lists.newArrayListWithCapacity(splits.length-2);
					for(int i=2;i<splits.length;i++){
						initialEdges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(splits[i]))));
					}
					edges.initialize((Iterable)initialEdges);
					allEdges.add(edges);
					if(initialEdges.size()>0)
						allRemovals.add(initialEdges.get(generator.nextInt(initialEdges.size())));
//						System.out.println(generator.nextInt(initialEdges.size()));
	    	   }
	    	   long endTime = System.nanoTime();
	    	   System.out.println("Initialization:" + (endTime - startTime) + "ns");
	    	   System.out.println("Size:"+ allEdges.size());
	    	   it.close();
		} catch (IOException e) {
		}
	    startTime = System.nanoTime();
	    Iterator<Edge<IntWritable, NullWritable>> removalsIt = allRemovals.iterator();
	    for (ByteArrayEdges<IntWritable, NullWritable> edges : allEdges){
	    	if(edges.size()>0)
	    		edges.remove(removalsIt.next().getTargetVertexId());
	    }
	    long endTime = System.nanoTime();
	    System.out.println("Removals:" + (endTime - startTime) + "ns");

	}



@Test
public void testIndexedBitmapRemoval(){
	Random generator = new Random(23);
	long startTime = System.nanoTime();
    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    giraphConfiguration.setComputationClass(TestComputation.class);
    giraphConfiguration.setOutEdgesClass(IndexedBitmapEdges.class);
    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration = 
    		new ImmutableClassesGiraphConfiguration(giraphConfiguration);
//	int size = 1000000;
	int size = 2180759;
//	int size = 7414866;
	List<IndexedBitmapEdges> allEdges = Lists.newArrayListWithCapacity(size);
	List<Edge<IntWritable, NullWritable>> allRemovals = Lists.newArrayListWithCapacity(size);
    try {
//    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("uk-2007-05@1000000.txt").getFile()), "UTF-8");
    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("hollywood-2011.txt").getFile()), "UTF-8");
//    	LineIterator it = FileUtils.lineIterator(new File(Resources.getResource("indochina-2004.txt").getFile()), "UTF-8");
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
				allEdges.add(edges);
				if(initialEdges.size()>0)
					allRemovals.add(initialEdges.get(generator.nextInt(initialEdges.size())));
//					System.out.println(generator.nextInt(initialEdges.size()));
    	   }
    	   long endTime = System.nanoTime();
    	   System.out.println("Initialization:" + (endTime - startTime) + "ns");
    	   System.out.println("Size:"+ allEdges.size());
    	   it.close();
	} catch (IOException e) {
	}
    startTime = System.nanoTime();
    Iterator<Edge<IntWritable, NullWritable>> removalsIt = allRemovals.iterator();
    for (IndexedBitmapEdges edges : allEdges){
    	if(edges.size()>0)
    		edges.remove(removalsIt.next().getTargetVertexId());
    }
    long endTime = System.nanoTime();
    System.out.println("Removals:" + (endTime - startTime) + "ns");

}
}

