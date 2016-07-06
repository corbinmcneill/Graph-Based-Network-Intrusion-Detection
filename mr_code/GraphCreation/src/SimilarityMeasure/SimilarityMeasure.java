package SimilarityMeasure;

public interface  SimilarityMeasure {
	double getDistance(String[] a, String[] b);
	double maxDistance();
}
