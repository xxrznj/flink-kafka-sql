package com.xxr.flink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import kafka.common.Config;

public class MySQLSink extends RichSinkFunction<Tuple2<Integer, Integer>> {

	private static final long serialVersionUID = 1L;
	private Connection connection;
	private PreparedStatement preparedStatement;
	String drivername = JDBCTestBase.DRIVER_CLASS;
	String dburl = JDBCTestBase.DB_URL;

	@Override
	public void invoke(Tuple2<Integer, Integer> value) throws Exception {
		Class.forName(drivername);
		connection = DriverManager.getConnection(dburl);
		String sql = "replace into orders(order_id,order_no,order_price,timestam) values(?,?,?,?)";
		preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setInt(1, value.f0);
		preparedStatement.setString(2, "nuuu");
		preparedStatement.setInt(3, value.f1);
		preparedStatement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
		preparedStatement.executeUpdate();
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (connection != null) {
			connection.close();
		}

	}

}
