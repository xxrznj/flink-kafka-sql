package com.xxr.flink.other;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Simple example that shows how the Batch SQL API is used in Java.
 *
 * <p>This example shows how to:
 *  - Convert DataSets to Tables
 *  - Register a Table under a name
 *  - Run a SQL query on the registered Table
 */
public class WordCountSQL {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<WC> input = env.fromElements(
			new WC("Hello", 10),
			new WC("Ciao", 1),
			new WC("Hello", 1));

		// register the DataSet as table "WordCount"
		tEnv.registerDataSet("WordCount", input, "word, frequency");

		// run a SQL query on the Table and retrieve the result as a new Table
		Table table = tEnv.sqlQuery(
			"SELECT word, AVG(frequency) as frequency FROM WordCount GROUP BY word");

		DataSet<WC> result = tEnv.toDataSet(table, WC.class);

		result.print();
	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO containing a word and its respective count.
	 */
	public static class WC {
		public String word;
		public long frequency;

		// public constructor to make it a Flink POJO
		public WC() {}

		public WC(String word, long frequency) {
			this.word = word;
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + frequency;
		}
	}
}

