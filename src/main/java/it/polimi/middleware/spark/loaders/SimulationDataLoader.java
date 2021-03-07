package it.polimi.middleware.spark.loaders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SimulationDataLoader extends DatasetLoader {

	private final StructType schema;

	public SimulationDataLoader(SparkSession sparkSession, String datasetsDirectoryPath) {
		super(sparkSession, datasetsDirectoryPath);

		// Create schema.
		final List<StructField> mySchemaFields = new ArrayList<>();
		mySchemaFields.add(DataTypes.createStructField("country", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("day", DataTypes.IntegerType, false));
		mySchemaFields.add(DataTypes.createStructField("currentInfected", DataTypes.IntegerType, false));
		mySchemaFields.add(DataTypes.createStructField("currentImmune", DataTypes.IntegerType, false));
		mySchemaFields.add(DataTypes.createStructField("numOfPeopleInside", DataTypes.IntegerType, false));
		this.schema = DataTypes.createStructType(mySchemaFields);
	}

	@Override
	public Dataset<Row> load() {
		return getSparkSession()
				.read()
				.option("header", "true")
				.option("delimiter", ",")
				.schema(schema)
				.csv(getDatasetsDirectoryPath() + "simulation-data/*.csv");
	}
}

