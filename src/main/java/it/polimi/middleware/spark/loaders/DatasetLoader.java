package it.polimi.middleware.spark.loaders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class DatasetLoader {

	private final SparkSession sparkSession;
	private final String filePath;

	/**
	 * Creates a new DatasetLoader that will load the data contained in the file using the sparkSession provided.
	 * @param sparkSession the session from which to load the dataset.
	 * @param filePath the file from which getting the data.
	 */
	public DatasetLoader(SparkSession sparkSession, String filePath) {
		this.sparkSession = sparkSession;
		this.filePath = filePath;
	}

	/**
	 * Load the dataset.
	 * @return the dataset that has been loaded.
	 */
	public abstract Dataset<Row> load();

	protected SparkSession getSparkSession() {
		return sparkSession;
	}

	protected String getFilePath() {
		return filePath;
	}
}
