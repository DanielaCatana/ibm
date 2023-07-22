package big_data;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;

public class Tema3 {

    public static void main(String[] args) {
        //tema 1: creare sesiune spark, citire si afisare  fisier csv
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("F:/summer_practice_IBM/primul_program/src/main/resources/erasmus.csv/");
        dataset.show(25, false);

        //Dataset<Row> datset = null;
        saveData(dataset, "IT", "Italy");
        saveData(dataset, "MT", "Malta");
        saveData(dataset, "FR", "France");

        /*tema 2: functii: select, groupBY, count
        // Se afișează structura setului de date, adică numele coloanelor și tipurile acestora
        dataset.printSchema();
        dataset = dataset.filter(functions.col("Receiving Country Code").isin("IT","MT","FR"));
       // dataset.select("Receiving Country Code","Sending Country Code").show(50,false);
        dataset.groupBy("Receiving Country Code","Sending Country Code")
                .count()
                .orderBy(functions.col("Receiving Country Code").desc())
                .withColumnRenamed("count","Number of students")
                .show(50);*/


    }

        // Write to SQL Table
        public static void saveData(Dataset<Row> dataset, String countryCode, String tableName) {

        dataset
                    .filter(col("Receiving Country Code").isin(countryCode))
                    .drop()
                    .groupBy("Receiving Country Code", "Sending Country Code")
                    .count().orderBy("Receiving Country Code", "Sending Country Code")
                    .write()
                    .mode(SaveMode.Overwrite)
                    .format("jdbc")
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .option("url", "jdbc:mysql://localhost:3306/tema3?serverTimezone=UTC")
                    .option("dbtable", tableName)
                    .option("user", "root")
                    .option("password", "root")
                    .save(tableName + ".tema3");
        }

}














