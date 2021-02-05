package it.polimi.middleware.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import javax.ws.rs.NotAllowedException;

public class SparkUtils {

	private SparkUtils() {
		throw new NotAllowedException("Utils class with static methods. Should not be instantiated.");
	}

	/**
	 * Saves the dataset on a single CSV file.
	 * Note: since it is saved on a single file it needs to bring the result in a single worker, creating a bottleneck.
	 * So this operation is not distributed.
	 * @param dataset the dataset to be saved.
	 * @param filePath the path where to save the file.
	 */
	public static void saveDatasetAsSingleCSV(final Dataset<Row> dataset, final String filePath) {
		dataset.coalesce(1)
				.write()
				.mode(SaveMode.Overwrite)
				.format("csv")
				.option("header", "true")
				.save(filePath);
	}
}
