package it.polimi.middleware.spark.queries;

import it.polimi.middleware.spark.DatasetOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.when;

/**
 * PercentageIncrease7DaysMA computes the percentage increase (with respect to the day before) of the seven days moving average, for each country and for each day.
 */
public class PercentageIncrease7DaysMA extends DatasetOperator {

	public PercentageIncrease7DaysMA(Dataset<Row> dataset) {
		super(dataset);
	}

	@Override
	public Dataset<Row> performOperation() {
		// How it is computed: percentage increase = ((x2-x1)*100)/x1. This can be simplified to (x2/x1 - 1)*100.
		final WindowSpec windowInEachCountryByDate = Window
				.partitionBy("country")
				.orderBy("date");
		final Column percentageIncreaseComputationColumn = col("movingAverage7Days")
				.divide(lag("movingAverage7Days", 1).over(windowInEachCountryByDate))
				.minus(1.0)
				.multiply(100.0);
		return getDataset()
				.withColumn("percentageIncreaseOfMA7Days", percentageIncreaseComputationColumn)
				.withColumn("percentageIncreaseOfMA7Days", when(col("percentageIncreaseOfMA7Days").isNull(), 0.0).otherwise(col("percentageIncreaseOfMA7Days")));
	}
}
