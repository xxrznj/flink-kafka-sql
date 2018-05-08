package com.xxr.flink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import kafka.common.Config;

public class WikiSQLSink extends RichSinkFunction<Tuple3<String,Long, Long>> {

	private static final long serialVersionUID = 1L;
	private Connection connection;
	private PreparedStatement preparedStatement;
	String drivername = JDBCTestBase.DRIVER_CLASS;
	String dburl = JDBCTestBase.DB_URL;

	@Override
	public void invoke(Tuple3<String,Long, Long> value) throws Exception {
		Class.forName(drivername);
		connection = DriverManager.getConnection(dburl);
		String sql = "INSERT into wiki(name,avg,time) values(?,?,?)";
		preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, value.f0);
		preparedStatement.setLong(2, value.f1);
		preparedStatement.setLong(3, value.f2);
		//preparedStatement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
		preparedStatement.executeUpdate();
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (connection != null) {
			connection.close();
		}

	}

}
