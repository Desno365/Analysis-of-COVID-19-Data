package it.polimi.middleware.spark.operators.preprocessors;

import it.polimi.middleware.spark.operators.DatasetOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class EcdcDataPreprocessor extends DatasetOperator {

	private final boolean showResultsInTerminal;

	public EcdcDataPreprocessor(Dataset<Row> dataset, boolean showResultsInTerminal) {
		super(dataset);
		this.showResultsInTerminal = showResultsInTerminal;
	}

	@Override
	public Dataset<Row> performOperation() {
		// Initialize the data.
		// How is it done:
		// 1) Select only interesting columns.
		// 2) Convert "date" from StringType to DateType.
		final Dataset<Row> rawDataset = getDataset()
				.select("country", "date", "yearAndWeek", "casesWeekly")
				.withColumn("date", to_date(col("date"), "dd/MM/yyyy"));

		if(showResultsInTerminal) {
			System.out.println("##############################################");
			System.out.println("############## Raw ECDC dataset ##############");
			System.out.println("##############################################");
			rawDataset.show();
		} else {
			System.out.println("Finished loading: Ecdc data.");
		}

		// Preprocessing of the data.
		// How is it done:
		// 1) Add a "dayOfTheWeek" column by adding 7 days for each date. But then "date" will be equal in all 7 days. So now we need to update the date.
		// 2) Update "date" by adding to each date a number of days equal to the day of the week - 1.
		// 3) Add "casesDaily" column computed from the "casesWeekly" column and dividing by 7.
		// 4) Reorder columns with a select.
		final Column daysOfTheWeekForEachDate = array(lit(7), lit(6), lit(5), lit(4), lit(3), lit(2), lit(1));
		return rawDataset
				.withColumn("dayOfTheWeek", explode(daysOfTheWeekForEachDate))
				.withColumn("date", date_add(col("date"), col("dayOfTheWeek").minus(1)))
				.withColumn("casesDaily", col("casesWeekly").divide(7).cast(DataTypes.IntegerType))
				.select("country", "date", "casesWeekly", "casesDaily");
	}
}
