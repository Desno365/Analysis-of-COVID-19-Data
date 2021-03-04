package it.polimi.middleware.spark.loaders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class TestLoader extends DatasetLoader{

    private final StructType schema;

    public TestLoader(SparkSession sparkSession, String filePath) {
        super(sparkSession, filePath);

        // Create schema.
        //country,date,yearAndWeek,dayOfTheWeek,casesWeekly,casesDaily,movingAverage7Days,percentageIncreaseOfMA7Days,rank
        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("country", DataTypes.StringType, false));
        mySchemaFields.add(DataTypes.createStructField("date", DataTypes.DateType, false));
        mySchemaFields.add(DataTypes.createStructField("yearAndWeek", DataTypes.StringType, false));
        mySchemaFields.add(DataTypes.createStructField("dayOfTheWeek", DataTypes.IntegerType, false));
        mySchemaFields.add(DataTypes.createStructField("casesWeekly", DataTypes.IntegerType, false));
        mySchemaFields.add(DataTypes.createStructField("casesDaily", DataTypes.IntegerType, false));
        mySchemaFields.add(DataTypes.createStructField("movingAverage7Days", DataTypes.FloatType, false));
        mySchemaFields.add(DataTypes.createStructField("percentageIncreaseOfMA7Days", DataTypes.FloatType, false));
        mySchemaFields.add(DataTypes.createStructField("rank", DataTypes.IntegerType, false));
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
