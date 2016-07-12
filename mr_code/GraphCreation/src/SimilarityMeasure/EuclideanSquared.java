package SimilarityMeasure;

public class EuclideanSquared implements SimilarityMeasure {
	private static final int MAX_VALUES[]= {12112, 0, 0, 93184, 5203179, 19, 3, 8, 1, 1, 7, 4, 2, 1, 1, 1, 1}; 
	private static double MAX_DISTANCE = 0;
	
	public double getDistance(String[] a, String[] b) {
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

}
