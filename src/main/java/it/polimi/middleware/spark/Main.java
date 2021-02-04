package it.polimi.middleware.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Main {
	public static void main(String[] args) {
		final String master = args.length > 0 ? args[0] : "local[4]";
		final String filePath = args.length > 1 ? args[1] : "./";
		Logger.getLogger("org").setLevel(Level.OFF);

		final SparkSession spark = SparkSession
				.builder()
				.master(master)
				.appName("CovidReport")
				.getOrCreate();

		// Create initial schema.
		final List<StructField> mySchemaFields = new ArrayList<>();
		mySchemaFields.add(DataTypes.createStructField("date", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("yearAndWeek", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("casesWeekly", DataTypes.IntegerType, false));
		mySchemaFields.add(DataTypes.createStructField("deathsWeekly", DataTypes.IntegerType, false));
		mySchemaFields.add(DataTypes.createStructField("country", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("geoId", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("countryCode", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("population2019", DataTypes.IntegerType, false));
		mySchemaFields.add(DataTypes.createStructField("continent", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("notificationRate", DataTypes.FloatType, false)); // It is calculated as: (New cases over 14 day period)/Population)*100'000
		final StructType mySchema = DataTypes.createStructType(mySchemaFields);

		// Load dataset.
		final Dataset<Row> covidDataset = spark
				.read()
				.option("header", "true")
				.option("delimiter", ",")
				.schema(mySchema)
				.csv(filePath + "files/datasets/ecdc-data.csv");

		// Preprocess: convert "date" from StringType to DateType.
		final Dataset<Row> covidDatasetWithDate = covidDataset.withColumn("date", to_date(col("date"), "dd/MM/yyyy"));
		covidDatasetWithDate.show();

		// Preprocess: add a "dayOfTheWeek" column, for each date it will create 7 days of the week and it will update the date accordingly.
		// How is it done:
		// 1) Add 7 days of the week for each date, but then date will be equal to all of them. So now we need to update the date.
		// 2) Update the date by adding to each date a number of days equal to the day of the week - 1.
		final Column daysOfTheWeekForEachDate = array(lit(7), lit(6), lit(5), lit(4), lit(3), lit(2), lit(1));
		final Dataset<Row> covidDatasetWithDaysOfTheWeek = covidDatasetWithDate
				.withColumn("dayOfTheWeek", explode(daysOfTheWeekForEachDate))
				.withColumn("date", date_add(col("date"), col("dayOfTheWeek").minus(1)));
		covidDatasetWithDaysOfTheWeek.show(35);

		final Dataset<Row> covidDatasetWithEvenlySpreadDailyCases = covidDatasetWithDaysOfTheWeek
				.withColumn("casesDaily", col("casesWeekly").divide(7).cast(DataTypes.IntegerType));
		covidDatasetWithEvenlySpreadDailyCases.show(35);

		// Example query: total amount of cases for each country.
		/*final Dataset<Row> totalCasesPerCountry = covidDatasetWithDate
				.groupBy("country")
				.sum("casesWeekly")
				.select("country", "sum(casesWeekly)");
		saveDatasetAsCSV(totalCasesPerCountry, filePath + "files/outputs/total-cases-per-country");*/
	}

	private static void saveDatasetAsCSV(Dataset<Row> dataset, String path) {
		dataset.coalesce(1)
				.write()
				.mode(SaveMode.Overwrite)
				.format("csv")
				.option("header", "true")
				.save(path);
	}
}
