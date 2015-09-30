package org.apache.giraph.utils;

/** A lazy iterator over the integers.
 * 
 * <p>An instance of this class represent an iterator over the integers and weights.
 * The iterator is exhausted when an implementation-dependent special marker is
 * returned. This fully lazy architecture halves the number of method
 * calls w.r.t. Java's eager iterators.
 */

public interface LazyIntIntIterator {
	/** The next integer returned by this iterator, or the special
	 * marker if this iterator is exhausted.
	 * 
	 * @return next integer returned by this iterator, or the special
	 * marker if this iterator is exhausted.
	 */
	public int nextInt();
	
	/** The next weight returned by this iterator.
	 * 
	 * @return next weight returned by this iterator.
	 */
	public int nextWeight();

}