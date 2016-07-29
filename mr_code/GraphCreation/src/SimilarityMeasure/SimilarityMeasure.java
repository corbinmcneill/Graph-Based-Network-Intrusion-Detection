package SimilarityMeasure;

/**
 * The Similarity Measure interface that defines the behavior of similarity functions for
 * building similarity graphs. 
 * @author mcneill
 */
public interface  SimilarityMeasure {
	/**
	 * Get the distance between two vertices based on their data vectors
	 * @param a A vector describing one input vector.
	 * @param b A vector describing another input vector. 
	 * @return The distance between the two datums
	 * @throws MaximumsNotSetException if the maximums must be set prior to a getDistance call 
	 * and has not yet been set.
	 */
	double getDistance(String[] a, String[] b) throws MaximumsNotSetException;
	
	/**
	 * Get the maximum possible distance between any two graph vertices.
	 */
	double maxDistance();
	
	/**
	 * Set the maximums of the current graph.
	 */
	void setMaximums(double[] a);
}
