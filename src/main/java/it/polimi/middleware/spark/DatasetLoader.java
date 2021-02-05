package it.polimi.middleware.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class DatasetLoader {

	private final SparkSession sparkSession;
	private final String filePath;

	public DatasetLoader(SparkSession sparkSession, String filePath) {
		this.sparkSession = sparkSession;
		this.filePath = filePath;
	}

	protected SparkSession getSparkSession() {
		return sparkSession;
	}

	protected String getFilePath() {
		return filePath;
	}

	public abstract Dataset<Row> load();
}
