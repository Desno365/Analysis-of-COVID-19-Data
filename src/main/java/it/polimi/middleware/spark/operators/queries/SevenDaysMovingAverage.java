package it.polimi.middleware.spark.operators.queries;

import it.polimi.middleware.spark.operators.DatasetTransformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

/**
 * SevenDaysMovingAverage performs the seven days moving average of new reported cases, for each country and for each day.
 */
public class SevenDaysMovingAverage extends DatasetTransformation {

	public SevenDaysMovingAverage(Dataset<Row> dataset) {
		super(dataset, "seven-days-moving-average-per-country");
	}

	@Override
	public Dataset<Row> getDatasetAfterTransformation() {
		final WindowSpec window7DaysInEachCountryByDate = Window
				.partitionBy("country")
				.orderBy("date")
				.rowsBetween(-6, 0);
		return getDataset()
				.withColumn("movingAverage7Days", avg(col("casesDaily")).over(window7DaysInEachCountryByDate));
	}
}
