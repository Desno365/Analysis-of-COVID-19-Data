package it.polimi.middleware.spark.operators.queries;

import it.polimi.middleware.spark.operators.DatasetOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

/**
 * Top10CountriesWithHighestPercentageIncrease finds the top 10 countries with the highest percentage increase of the seven days moving average, for each day.
 */
public class Top10CountriesWithHighestPercentageIncrease extends DatasetOperator {

	public Top10CountriesWithHighestPercentageIncrease(Dataset<Row> dataset) {
		super(dataset);
	}

	@Override
	public Dataset<Row> performOperation() {
		final WindowSpec windowInEachDateByPercentageIncrease = Window
				.partitionBy("date")
				.orderBy(desc("percentageIncreaseOfMA7Days"));
		return getDataset()
				.withColumn("rank", rank().over(windowInEachDateByPercentageIncrease))
				.where(col("rank").$less$eq(10))
				.orderBy("date", "rank");
	}
}

