package it.polimi.middleware.spark.operators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class DatasetOperator {

	private final Dataset<Row> dataset;

	/**
	 * Creates a new DatasetOperator that will execute its operation on the dataset provided here in the constructor.
	 * @param dataset the dataset where the operation is executed.
	 */
	public DatasetOperator(Dataset<Row> dataset) {
		this.dataset = dataset;
	}

	/**
	 * Perform the operation of the operator.
	 * @return the dataset after applying the operation.
	 */
	public abstract Dataset<Row> performOperation();

	protected Dataset<Row> getDataset() {
		return dataset;
	}
}
