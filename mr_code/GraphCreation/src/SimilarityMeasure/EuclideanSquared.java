package SimilarityMeasure;


/**
 * A similarity measure based off of the euclidean difference between two vectors squared.
 * @author mcneill
 */
public class EuclideanSquared implements SimilarityMeasure {
	private static double MAX_VALUES[] = new double[] {54451, 0, 0, 101764, 5203179, 30, 3, 381, 1, 1, 401, 30, 5, 4, 1, 1, 1};
	private static double MAX_DISTANCE = 0;
	
	public double getDistance(String[] a, String[] b) throws MaximumsNotSetException {
		if (MAX_VALUES == null) {
			throw new MaximumsNotSetException();
		}
		double distance = 0;
		for (int i=0; i<MAX_VALUES.length; i++) {
			if (MAX_VALUES[i] == 0) {
				if (!a[i].equals(b[i])) {
					distance += 1;
				}
			} else {
				distance += Math.pow((Double.parseDouble(a[i])-Double.parseDouble(b[i]))/MAX_VALUES[i], 2);
			}
		}
		return distance;
	}
	
	public double maxDistance() {
		if (MAX_DISTANCE == 0) {
			MAX_DISTANCE = Math.sqrt(MAX_VALUES.length);
		}
		return MAX_DISTANCE;
	}
	
	public void setMaximums(double[] a) {
		MAX_VALUES = a;
	}
}
