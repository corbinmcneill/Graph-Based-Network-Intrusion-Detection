package SimilarityMeasure;

public interface  SimilarityMeasure {
	double getDistance(String[] a, String[] b) throws MaximumsNotSetException;
	double maxDistance();
	void setMaximums(double[] a);
}
