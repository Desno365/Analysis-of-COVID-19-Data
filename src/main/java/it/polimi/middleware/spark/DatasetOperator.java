package it.polimi.middleware.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class DatasetOperator {

	private final Dataset<Row> dataset;

	public DatasetOperator(Dataset<Row> dataset) {
		this.dataset = dataset;
	}

	protected Dataset<Row> getDataset() {
		return dataset;
	}

	public abstract Dataset<Row> performOperation();
}
