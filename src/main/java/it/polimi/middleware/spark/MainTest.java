package it.polimi.middleware.spark;

import it.polimi.middleware.spark.loaders.DatasetLoader;
import it.polimi.middleware.spark.loaders.EcdcDataLoader;
import it.polimi.middleware.spark.loaders.TestLoader;
import it.polimi.middleware.spark.operators.DatasetOperator;
import it.polimi.middleware.spark.operators.preprocessors.EcdcDataPreprocessor;
import it.polimi.middleware.spark.operators.queries.PercentageIncrease7DaysMA;
import it.polimi.middleware.spark.operators.queries.SevenDaysMovingAverageOperator;
import it.polimi.middleware.spark.operators.queries.Top10CountriesWithHighestPercentageIncrease;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

class MainTest {

    @BeforeEach
    void setUp() {
    }

    @Test
    public void testEcdcTestDataset(){
        final String master = "local[4]";
        final String filePath = "./";
        Logger.getLogger("org").setLevel(Level.OFF);
        final SparkSession sparkSession = SparkSession
                .builder()
                .master(master)
                .appName("CovidReport")
                .getOrCreate();

        final DatasetLoader datasetLoader = new EcdcDataLoader(sparkSession, filePath + "files/datasets-test/test-data.csv");
        final Dataset<Row> covidDataset = datasetLoader.load();
        final DatasetOperator preprocessOperator = new EcdcDataPreprocessor(covidDataset);
        final Dataset<Row> preprocessedCovidDataset = preprocessOperator.performOperation();

        final DatasetOperator query1Operator = new SevenDaysMovingAverageOperator(preprocessedCovidDataset);
        final Dataset<Row> covidDatasetQuery1 = query1Operator.performOperation();

        final DatasetOperator query2Operator = new PercentageIncrease7DaysMA(covidDatasetQuery1);
        final Dataset<Row> covidDatasetQuery2 = query2Operator.performOperation();

        final DatasetOperator query3Operator = new Top10CountriesWithHighestPercentageIncrease(covidDatasetQuery2);
        final Dataset<Row> covidDatasetQuery3 = query3Operator.performOperation();

        SparkUtils.saveDatasetAsSingleCSV(covidDatasetQuery3, filePath + "files/outputs/test-output-to-compare");

        File file = new File(filePath + "files/outputs/test-output-to-compare");
        String fileName="";
        for(String string: file.list()){
            if(string.indexOf("SUCCESS")==-1 && string.indexOf("crc")==-1)
                fileName=string;
        }
        File testOutputCsv = new File(filePath + "files/outputs/test-output-to-compare/" + fileName);
        File correctOutputCsv = new File(filePath + "files/outputs/tests/outputs-test.csv");

        assert compareFiles(testOutputCsv,correctOutputCsv)==43;

//        covidTestDataset.show();
//        covidTestDataset.intersect(covidDatasetQuery3).show(1000);
    }


    @AfterEach
    void tearDown() {
    }

    int compareFiles(File file1,File file2) {
        BufferedReader bfr1 = null;
        BufferedReader bfr2 = null;
        List<String> file1Data = new ArrayList<>();
        List<String> file2Data = new ArrayList<>();
        List<String> matchedLines = new ArrayList<>();
        List<String> unmatchedLines = new ArrayList<>();

        try {
            bfr1 = new BufferedReader(new FileReader(file1));
            bfr2 = new BufferedReader(new FileReader(file2));

            String name1;
            String name2;
            try {
                // read the first file and store it in an ArrayList
                while ((name1 = bfr1.readLine()) != null) {
                    file1Data.add(name1);
                }
                // read the second file and store it in an ArrayList
                while ((name2 = bfr2.readLine()) != null) {
                    file2Data.add(name2);
                }

                // iterate once over the first ArrayList and compare with the second.
                for (String key1 : file1Data) {
                    if (file2Data.contains(key1)) { // here is your desired match
                        matchedLines.add(key1);
                    } else {
                        unmatchedLines.add(key1);
                    }
                }
                return matchedLines.size();
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } finally {
            try {
                bfr1.close();
                bfr2.close();
            } catch (IOException ex) {
                System.out.println(ex);
            }
        }
        return 0;
    }
}


