package SimilarityMeasure;


/**
 * An implementation of the Sorenson Similairty Measure
 * @author mcneill
 */
public class Sorenson implements SimilarityMeasure {
	
	/**
	 * The Dimension of input data.
	 */
	private static final int DIMENSION = 12;
	
	public double getDistance(String[] a, String[] b) {
		double distance = 0;
		for (int i=0; i<DIMENSION; i++) {
			try {
				distance += Math.abs(Double.parseDouble(a[i]) - Double.parseDouble(b[i]))/(Double.parseDouble(a[i]) + Double.parseDouble(b[i]));
			} catch (NumberFormatException e) {
				distance += 1;
			}
		}
		return distance;
	}
	
	public double maxDistance() {
		return DIMENSION;
	}

	/**
	 * Set maximums does nothing as maximums are not required for the Sorenson Similarity Measure.
	 */
	public void setMaximums(double[] a) {
		return;
	}
}
