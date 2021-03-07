package it.polimi.middleware.spark;

import it.polimi.middleware.spark.loaders.DatasetLoader;
import it.polimi.middleware.spark.loaders.EcdcDataLoader;
import it.polimi.middleware.spark.loaders.SimulationDataLoader;
import it.polimi.middleware.spark.operators.DatasetOperator;
import it.polimi.middleware.spark.operators.preprocessors.EcdcDataPreprocessor;
import it.polimi.middleware.spark.operators.preprocessors.SimulationDataPreprocessor;
import it.polimi.middleware.spark.operators.queries.PercentageIncrease7DaysMA;
import it.polimi.middleware.spark.operators.queries.SevenDaysMovingAverageOperator;
import it.polimi.middleware.spark.operators.queries.Top10CountriesWithHighestPercentageIncrease;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class CovidReport {

	private static final String SPARK_APP_NAME = "CovidReport";

	private final String sparkMaster;
	private final String datasetsDirectoryPath;
	private final String outputsDirectoryPath;
	private final String whichDataset;
	private final boolean showResultsInTerminal;

	/**
	 * Performs a covid report.
	 * @param sparkMaster the Spark master URL to connect to.
	 * @param datasetsDirectoryPath path to the CSV file containing the raw covid data.
	 * @param outputsDirectoryPath path to the directory that should be used to save the outputs.
	 * @param showResultsInTerminal set to true to show a part of the result also in the terminal.
	 */
	public CovidReport(String sparkMaster, String datasetsDirectoryPath, String outputsDirectoryPath, String whichDataset, boolean showResultsInTerminal) {
		this.sparkMaster = sparkMaster;
		this.datasetsDirectoryPath = datasetsDirectoryPath;
		this.outputsDirectoryPath = outputsDirectoryPath;
		this.whichDataset = whichDataset;
		this.showResultsInTerminal = showResultsInTerminal;
	}

	/**
	 * Performs the covid report analysis:
	 * 0) Preprocess input dataset;
	 * 1) Seven days moving average of new reported cases, for each country and for each day;
	 * 2) Percentage increase (with respect to the day before) of the seven days moving average, for each country and for each day;
	 * 3) Top 10 countries with the highest percentage increase of the seven days moving average, for each day.
	 */
	public void performAnalysis() {
		// Disable logs of Spark.
		Logger.getLogger("org").setLevel(Level.OFF);

		// Initialize SparkSession.
		final SparkSession sparkSession = SparkSession
				.builder()
				.master(sparkMaster)
				.appName(SPARK_APP_NAME)
				.getOrCreate();

		final List<DatasetOperator> preprocessors = new ArrayList<>();

		if(whichDataset.equals("ecdc") || whichDataset.equals("all")) {
			// Load ECDC data.
			final DatasetLoader ecdcDatasetLoader = new EcdcDataLoader(sparkSession, datasetsDirectoryPath);
			final Dataset<Row> ecdcDataset = ecdcDatasetLoader.load();

			// Set up preprocessor.
			final DatasetOperator preprocessOperator = new EcdcDataPreprocessor(ecdcDataset, showResultsInTerminal);
			preprocessors.add(preprocessOperator);
		}

		if(whichDataset.equals("simulation") || whichDataset.equals("all")) {
			// Load Simulation data.
			final DatasetLoader simulationDatasetLoader = new SimulationDataLoader(sparkSession, datasetsDirectoryPath);
			final Dataset<Row> simulationDataset = simulationDatasetLoader.load();

			// Set up preprocessor.
			final DatasetOperator preprocessOperator = new SimulationDataPreprocessor(simulationDataset, showResultsInTerminal);
			preprocessors.add(preprocessOperator);
		}

		for(DatasetOperator preprocessor : preprocessors) {
			// Preprocess data.
			final Dataset<Row> preprocessedCovidDataset = preprocessor.performOperation();

			// Step 1: Seven days moving average of new reported cases, for each country and for each day.
			final DatasetOperator query1Operator = new SevenDaysMovingAverageOperator(preprocessedCovidDataset);
			final Dataset<Row> covidDatasetQuery1 = query1Operator.performOperation();
			SparkUtils.saveDatasetAsSingleCSV(covidDatasetQuery1, outputsDirectoryPath + "/" + preprocessor.getDatasetName() + "/" + query1Operator.getDatasetName());

			// Step 2: Percentage increase (with respect to the day before) of the seven days moving average, for each country and for each day.
			final DatasetOperator query2Operator = new PercentageIncrease7DaysMA(covidDatasetQuery1);
			final Dataset<Row> covidDatasetQuery2 = query2Operator.performOperation();
			SparkUtils.saveDatasetAsSingleCSV(covidDatasetQuery2, outputsDirectoryPath + "/" + preprocessor.getDatasetName() + "/" + query2Operator.getDatasetName());

			// Show moving average and percentage increase.
			if(showResultsInTerminal) {
				System.out.println("################################################################################");
				System.out.println("############## " + preprocessor.getDatasetName() + ": Moving average + Percentage increase ##############");
				System.out.println("################################################################################");
				covidDatasetQuery2.show(750);
			} else {
				System.out.println("Finished computing: " + preprocessor.getDatasetName() + ": Moving average + Percentage increase.");
			}

			// Step 3: Top 10 countries with the highest percentage increase of the seven days moving average, for each day.
			final DatasetOperator query3Operator = new Top10CountriesWithHighestPercentageIncrease(covidDatasetQuery2);
			final Dataset<Row> covidDatasetQuery3 = query3Operator.performOperation();
			SparkUtils.saveDatasetAsSingleCSV(covidDatasetQuery3, outputsDirectoryPath + "/" + preprocessor.getDatasetName() + "/" + query3Operator.getDatasetName());

			// Show top 10 countries.
			if(showResultsInTerminal) {
				System.out.println("###################################################################################");
				System.out.println("############## " + preprocessor.getDatasetName() + ": Top 10 countries by percentage increase ##############");
				System.out.println("###################################################################################");
				covidDatasetQuery3.show(100);
			} else {
				System.out.println("Finished computing: " + preprocessor.getDatasetName() + ": Top 10 countries by percentage increase.");
			}
		}
	}
}
