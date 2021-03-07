package it.polimi.middleware.spark;

public class Main {

	public static void main(String[] args) {
		final String master = args.length > 0 ? args[0] : "local[4]";
		final String filePath = args.length > 1 ? args[1] : "./";
		final String whichDataset = args.length > 2 ? args[2].toLowerCase() : "all";
		final String datasetsDirectoryPath = filePath + "files/datasets/";
		final String outputsDirectoryPath = filePath + "files/outputs/";
		final boolean showResultsInTerminal = args.length > 3 ? Boolean.parseBoolean(args[3]) : true;

		// Perform analysis.
		final CovidReport covidReport = new CovidReport(master, datasetsDirectoryPath, outputsDirectoryPath, whichDataset, showResultsInTerminal);
		covidReport.performAnalysis();
	}
}
