package it.polimi.middleware.spark.operators.preprocessors;

import it.polimi.middleware.spark.SparkUtils;
import it.polimi.middleware.spark.operators.DatasetOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.Calendar;
import java.util.TimeZone;

import static org.apache.spark.sql.functions.*;

public class SimulationDataPreprocessor extends DatasetOperator {

	private final boolean showResultsInTerminal;

	public SimulationDataPreprocessor(Dataset<Row> dataset, boolean showResultsInTerminal) {
		super(dataset, "simulation-dataset");
		this.showResultsInTerminal = showResultsInTerminal;
	}

	@Override
	public Dataset<Row> performOperation() {
		// Initialize the data.
		// How is it done:
		// 1) Select only interesting columns.
		final Dataset<Row> rawDataset = getDataset()
				.select("country", "day", "currentInfected");

		if(showResultsInTerminal) {
			System.out.println("############################################################");
			System.out.println("###### Raw Simulation data (only interesting columns) ######");
			System.out.println("############################################################");
			rawDataset.show();
		} else {
			System.out.println("Finished loading: " + getDatasetName() + ".");
		}

		// Preprocessing of the data.
		// How is it done:
		// 1) Add a "date" column by adding the number of days specified in the "day" column to the current date.
		// 2) Add "casesDaily" column computed from the "currentInfected" column by doing the difference relative to the day before.
		// 3) Update "casesDaily" column by setting the initial value for days that do not have a previous day (so their result of previous difference computation is null).
		// 4) Reorder columns with a select and keep only interesting columns.
		final WindowSpec windowInEachCountryByDay = Window
				.partitionBy("country")
				.orderBy("day");
		final Column casesDailyComputationColumn = col("currentInfected")
				.minus(lag("currentInfected", 1).over(windowInEachCountryByDay));
		return rawDataset
				.withColumn("date", date_add(getInitialDateColumnLiteral(2019, 12, 31), col("day")))
				.withColumn("casesDaily", casesDailyComputationColumn)
				.withColumn("casesDaily", when(col("casesDaily").isNull(), col("currentInfected")).otherwise(col("casesDaily")))
				.select("country", "date", "currentInfected", "casesDaily");
	}

	private Column getInitialDateColumnLiteral(int year, int month, int day) {
		final Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
		calendar.set(year, month - 1, day, 12, 0);
		return SparkUtils.getDateColumnLiteralFromJavaDate(calendar.getTime());
	}
}

