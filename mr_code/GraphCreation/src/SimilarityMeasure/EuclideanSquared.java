package SimilarityMeasure;

public class EuclideanSquared implements SimilarityMeasure {
<<<<<<< HEAD
	private static final int MAX_VALUES[]= {47114, 0, 0, 3131464, 1888525, 24, 1, 165, 1, 1, 173, 4, 2, 2, 1, 1, 1}; 
=======
	private static double MAX_VALUES[];
>>>>>>> 86ddb9bca0d743b8ceeb7a81c894bd3c77452168
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
<<<<<<< HEAD
=======

	public void setMaximums(double[] a) {
		MAX_VALUES = a;
	}
>>>>>>> 86ddb9bca0d743b8ceeb7a81c894bd3c77452168
}
