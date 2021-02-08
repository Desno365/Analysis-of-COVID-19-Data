package it.polimi.middleware.spark;

public class Main {

	public static void main(String[] args) {
		final String master = args.length > 0 ? args[0] : "local[4]";
		final String filePath = args.length > 1 ? args[1] : "./";
		final String inputDatasetPath = filePath + "files/datasets/ecdc-data.csv";
		final String outputDirectoryPath = filePath + "files/outputs/";

		// Perform analysis.
		final CovidReport covidReport = new CovidReport(master, inputDatasetPath, outputDirectoryPath, true);
		covidReport.performAnalysis();
	}
}
