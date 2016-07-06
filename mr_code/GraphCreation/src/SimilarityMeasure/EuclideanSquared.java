package SimilarityMeasure;

public class EuclideanSquared implements SimilarityMeasure {
	private static final int MAX_VALUES[]= {902, 0, 0, 56504, 15279, 5, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1}; 
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