package it.polimi.middleware.spark.queries;

import it.polimi.middleware.spark.DatasetOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

/**
 * SevenDaysMovingAverageOperator performs the seven days moving average of new reported cases, for each country and for each day.
 */
public class SevenDaysMovingAverageOperator extends DatasetOperator {

	public SevenDaysMovingAverageOperator(Dataset<Row> dataset) {
		super(dataset);
	}

	@Override
	public Dataset<Row> performOperation() {
		final WindowSpec window7DaysInEachCountryByDate = Window
				.partitionBy("country")
				.orderBy("date")
				.rowsBetween(-6, 0);
		return getDataset()
				.withColumn("movingAverage7Days", avg(col("casesDaily")).over(window7DaysInEachCountryByDate));
	}
}
