package it.polimi.middleware.spark;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CovidReportTest {

    @Test
    void testCorrectnessOfEcdcAnalysis(){
        final String master = "local[4]";
        final String filePath = "./";
        final String datasetsDirectoryPath = filePath + "files/datasets-test/";
        final String outputsDirectoryPath = filePath + "files/outputs-test/";

        // Perform analysis.
        final CovidReport covidReport = new CovidReport(master, datasetsDirectoryPath, outputsDirectoryPath, "ecdc", false);
        covidReport.performAnalysis();

        // Get File objects of both expected output and actual output.
        final String top10OutputDirectoryPath = filePath + "files/outputs-test/ecdc-dataset/top-ten-countries-with-highest-percentage-increase";
        final String fileName = getFileNameOfOutputCsvInOutputDirectory(top10OutputDirectoryPath);
        final File testOutputCsv = new File(top10OutputDirectoryPath + "/" + fileName);
        final File correctOutputCsv = new File(filePath + "files/datasets-test/ecdc-test-data-expected-output.csv");

        // Check that expected output and actual output are equal.
        assertEquals(43, getNumberOfMatchingLinesInCSVs(testOutputCsv, correctOutputCsv));
    }

    private String getFileNameOfOutputCsvInOutputDirectory(String outputDirectoryPath) {
        File file = new File(outputDirectoryPath);
        String fileName="";
        for(String string: file.list()){
            if(!string.contains("SUCCESS") && !string.contains("crc"))
                fileName=string;
        }
        return fileName;
    }

    private int getNumberOfMatchingLinesInCSVs(File file1, File file2) {
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


