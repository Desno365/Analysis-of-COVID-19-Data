package it.polimi.middleware.spark.operators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class DatasetOperator {

	private final Dataset<Row> dataset;
	private final String datasetName;

	/**
	 * Creates a new DatasetOperator that will execute its operation on the dataset provided here in the constructor.
	 * @param dataset the dataset where the operation is executed.
	 */
	public DatasetOperator(Dataset<Row> dataset, String datasetName) {
		this.dataset = dataset;
		this.datasetName = datasetName;
	}

	/**
	 * Perform the operation of the operator.
	 * @return the dataset after applying the operation.
	 */
	public abstract Dataset<Row> performOperation();

	/**
	 * Returns the name associated to the dataset.
	 * @return the name associated to the dataset.
	 */
	public String getDatasetName() {
		return datasetName;
	}

	protected Dataset<Row> getDataset() {
		return dataset;
	}
}
