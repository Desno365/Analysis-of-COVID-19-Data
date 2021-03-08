package it.polimi.middleware.spark.operators;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class DatasetTransformation {

	private final Dataset<Row> dataset;
	private final String datasetName;

	/**
	 * Creates a new DatasetTransformation that will execute its transformation on the dataset provided here in the constructor.
	 * @param dataset the dataset where the transformation is executed.
	 */
	public DatasetTransformation(Dataset<Row> dataset, String datasetName) {
		this.dataset = dataset;
		this.datasetName = datasetName;
	}

	/**
	 * Apply the transformation on the input dataset, returning the resulting dataset.
	 * @return the dataset after applying the transformation.
	 */
	public abstract Dataset<Row> getDatasetAfterTransformation();

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
