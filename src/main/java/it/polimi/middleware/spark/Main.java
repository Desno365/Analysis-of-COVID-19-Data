package it.polimi.middleware.spark;

public class Main {

	public static void main(String[] args) {
		final String master = args.length > 0 ? args[0] : "local[4]";
		final String filePath = args.length > 1 ? args[1] : "./";
		final String inputDatasetPath = filePath + "files/datasets/ecdc-data.csv";
		final String outputDirectoryPath = filePath + "files/outputs/";
		final boolean showResultsInTerminal = args.length > 2 ? Boolean.parseBoolean(args[2]) : true;

		// Perform analysis.
		final CovidReport covidReport = new CovidReport(master, inputDatasetPath, outputDirectoryPath, showResultsInTerminal);
		covidReport.performAnalysis();
	}
}
