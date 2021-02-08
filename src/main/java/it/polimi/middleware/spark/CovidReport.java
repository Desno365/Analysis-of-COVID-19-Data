package it.polimi.middleware.spark;

import it.polimi.middleware.spark.loaders.DatasetLoader;
import it.polimi.middleware.spark.loaders.EcdcDataLoader;
import it.polimi.middleware.spark.operators.DatasetOperator;
import it.polimi.middleware.spark.operators.preprocessors.EcdcDataPreprocessor;
import it.polimi.middleware.spark.operators.queries.PercentageIncrease7DaysMA;
import it.polimi.middleware.spark.operators.queries.SevenDaysMovingAverageOperator;
import it.polimi.middleware.spark.operators.queries.Top10CountriesWithHighestPercentageIncrease;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CovidReport {

	private static final String SPARK_APP_NAME = "CovidReport";

	private final String sparkMaster;
	private final String inputDatasetPath;
	private final String outputDirectoryPath;
	private final boolean showResultsInTerminal;

	public CovidReport(String sparkMaster, String inputDatasetPath, String outputDirectoryPath, boolean showResultsInTerminal) {
		this.sparkMaster = sparkMaster;
		this.inputDatasetPath = inputDatasetPath;
		this.outputDirectoryPath = outputDirectoryPath;
		this.showResultsInTerminal = showResultsInTerminal;
	}

	public void performAnalysis() {
		// Disable logs of Spark.
		Logger.getLogger("org").setLevel(Level.OFF);

		// Initialize SparkSession.
		final SparkSession sparkSession = SparkSession
				.builder()
				.master(sparkMaster)
				.appName(SPARK_APP_NAME)
				.getOrCreate();

		// Load ECDC data.
		final DatasetLoader datasetLoader = new EcdcDataLoader(sparkSession, inputDatasetPath);
		final Dataset<Row> covidDataset = datasetLoader.load();

		// Preprocess data.
		final DatasetOperator preprocessOperator = new EcdcDataPreprocessor(covidDataset);
		final Dataset<Row> preprocessedCovidDataset = preprocessOperator.performOperation();

		// Step 1: Seven days moving average of new reported cases, for each country and for each day.
		final DatasetOperator query1Operator = new SevenDaysMovingAverageOperator(preprocessedCovidDataset);
		final Dataset<Row> covidDatasetQuery1 = query1Operator.performOperation();
		SparkUtils.saveDatasetAsSingleCSV(covidDatasetQuery1, outputDirectoryPath + "seven-days-moving-average-per-country");

		// Step 2: Percentage increase (with respect to the day before) of the seven days moving average, for each country and for each day.
		final DatasetOperator query2Operator = new PercentageIncrease7DaysMA(covidDatasetQuery1);
		final Dataset<Row> covidDatasetQuery2 = query2Operator.performOperation();
		SparkUtils.saveDatasetAsSingleCSV(covidDatasetQuery2, outputDirectoryPath +  "percentage-increase-seven-days-moving-average-per-country");

		// Show moving average and percentage increase.
		if(showResultsInTerminal) {
			System.out.println("##################################################################");
			System.out.println("############## Moving average + Percentage increase ##############");
			System.out.println("##################################################################");
			covidDatasetQuery2.show(750);
		}

		// Step 3: Top 10 countries with the highest percentage increase of the seven days moving average, for each day.
		final DatasetOperator query3Operator = new Top10CountriesWithHighestPercentageIncrease(covidDatasetQuery2);
		final Dataset<Row> covidDatasetQuery3 = query3Operator.performOperation();
		SparkUtils.saveDatasetAsSingleCSV(covidDatasetQuery3, outputDirectoryPath + "top-ten-countries-with-highest-percentage-increase");

		// Show top 10 countries. Shown from date 04/01/2021 so that every country has some cases.
		if(showResultsInTerminal) {
			System.out.println("#####################################################################");
			System.out.println("############## Top 10 countries by percentage increase ##############");
			System.out.println("#####################################################################");
			covidDatasetQuery3
					.where(col("date").geq(to_date(lit("04/01/2021"), "dd/MM/yyyy")))
					.show(100);
		}
	}
}
