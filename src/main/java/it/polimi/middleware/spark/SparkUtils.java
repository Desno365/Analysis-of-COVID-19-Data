package it.polimi.middleware.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import javax.ws.rs.NotAllowedException;

public class SparkUtils {

	private SparkUtils() {
		throw new NotAllowedException("Utils class with static methods. Should not be instantiated.");
	}

	public static void saveDatasetAsSingleCSV(final Dataset<Row> dataset, final String filePath) {
		dataset.coalesce(1)
				.write()
				.mode(SaveMode.Overwrite)
				.format("csv")
				.option("header", "true")
				.save(filePath);
	}
}
