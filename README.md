# Analysis of COVID-19 Data using Apache Spark

### Description of the project

Implement a program that analyzes open datasets to study the evolution of the COVID-19 situation worldwide. The program starts from the dataset of new reported cases for each country daily and computes the following queries:

1. Seven days moving average of new reported cases, for each country and for each day;
   
2. Percentage increase (with respect to the day before) of the seven days moving average, for each country
   and for each day;
   
3. Top 10 countries with the highest percentage increase of the seven days moving average, for each day;

You can either use [real open datasets](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) or synthetic data generated with a simulator.

### Assumptions and Guidelines
* When using a real dataset, for countries that provide weekly reports, you can assume that the weekly increment is evenly spread across the day of the week.