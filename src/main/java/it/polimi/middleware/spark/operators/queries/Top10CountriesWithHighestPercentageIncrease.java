package it.polimi.middleware.spark.operators.queries;

import it.polimi.middleware.spark.operators.DatasetTransformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

/**
 * Top10CountriesWithHighestPercentageIncrease finds the top 10 countries with the highest percentage increase of the seven days moving average, for each day.
 */
public class Top10CountriesWithHighestPercentageIncrease extends DatasetTransformation {

	public Top10CountriesWithHighestPercentageIncrease(Dataset<Row> dataset) {
		super(dataset, "top-ten-countries-with-highest-percentage-increase");
	}

	@Override
	public Dataset<Row> getDatasetAfterTransformation() {
		final WindowSpec windowInEachDateByPercentageIncrease = Window
				.partitionBy("date")
				.orderBy(desc("percentageIncreaseOfMA7Days"));
		return getDataset()
				.withColumn("rank", rank().over(windowInEachDateByPercentageIncrease))
				.where(col("rank").$less$eq(10))
				.orderBy("date", "rank");
	}
}

