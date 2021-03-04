package it.polimi.middleware.spark.loaders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class EcdcDataLoader extends DatasetLoader {

	private final StructType schema;

	public EcdcDataLoader(SparkSession sparkSession, String filePath) {
		super(sparkSession, filePath);

		// Create schema.
		final List<StructField> mySchemaFields = new ArrayList<>();
		mySchemaFields.add(DataTypes.createStructField("date", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("yearAndWeek", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("casesWeekly", DataTypes.IntegerType, false));
		mySchemaFields.add(DataTypes.createStructField("deathsWeekly", DataTypes.IntegerType, false));
		mySchemaFields.add(DataTypes.createStructField("country", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("geoId", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("countryCode", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("population2019", DataTypes.IntegerType, false));
		mySchemaFields.add(DataTypes.createStructField("continent", DataTypes.StringType, false));
		mySchemaFields.add(DataTypes.createStructField("notificationRate", DataTypes.FloatType, false)); // It is calculated as: (New cases over 14 day period)/Population)*100'000
		this.schema = DataTypes.createStructType(mySchemaFields);
	}

	@Override
	public Dataset<Row> load() {
		return getSparkSession()
				.read()
				.option("header", "true")
				.option("delimiter", ",")
				.schema(schema)
				.csv(getPath());
	}
}
