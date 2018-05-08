package com.xxr.flink.other;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.List;
import java.util.Properties;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

import com.xxr.flink.JDBCTestBase;
import com.xxr.flink.MySQLSink;

import akka.actor.FSM.Event;
import kafka.common.Config;

public class KafkaToDB {
	public static void main(String[] args) throws Exception { 
	
      Properties pro = new Properties();  
      pro.put("bootstrap.servers", JDBCTestBase.kafka_hosts);  
      pro.put("zookeeper.connect", JDBCTestBase.kafka_zookper);  
      pro.put("group.id",JDBCTestBase.kafka_group);  
      StreamExecutionEnvironment env = StreamExecutionEnvironment  
              .getExecutionEnvironment();  
      //env.getConfig().disableSysoutLogging();  //设置此可以屏蔽掉日记打印情况  
      //env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));  
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
      env.enableCheckpointing(5000);  
      DataStream<String> sourceStream = env  
              .addSource(new FlinkKafkaConsumer08<String>(JDBCTestBase  
                      .kafka_topic, new SimpleStringSchema(),  
                      pro));  

      DataStream<Tuple2<Integer, Integer>> sourceStreamTra = sourceStream.filter(new FilterFunction<String>() {   
          @Override  
          public boolean filter(String value) throws Exception {  
              return StringUtils.isNotBlank(value);  
          }  
      }).map(new MapFunction<String, Tuple2<Integer, Integer>>() {  
          private static final long serialVersionUID = 1L;  
                  @Override  
                  public Tuple2<Integer, Integer> map(String value)  
                          throws Exception {  
                      String[] args = value.split(":");  
                      return new Tuple2<Integer, Integer>(Integer  
                              .valueOf(args[0]), Integer  
                              .valueOf(args[2]));  
                  }  
              });  
      /**
      DataStream<Tuple2<Integer, Integer>> withTimestampsAndWatermarks =sourceStreamTra.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, Integer>>() {

          @Override
          public long extractAscendingTimestamp(Tuple2<Integer, Integer> element) {
              return element.getCreationTime();
          }
  });
  **/

      //.windowAll(TumblingEventTimeWindows.of(Time.days(1))).allowedLateness(Time.minutes(60))
      sourceStreamTra.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(300)))
      .apply(new RichAllWindowFunction<Tuple2<Integer, Integer>,Tuple2<Integer, Integer>, TimeWindow>(){

		@Override
		public void apply(TimeWindow window, Iterable<Tuple2<Integer, Integer>> values,
				Collector<Tuple2<Integer, Integer>> out) throws Exception {
			// TODO Auto-generated method stub
			Integer sum = 0;
			Integer count = 0;
			for (Tuple2<Integer, Integer> value : values) {
				sum += value.f1;
				count+=1;
			}

			out.collect(new Tuple2<>(count, sum));
		}
    	  
      });
      //.trigger(ContinuousEventTimeTrigger.<TimeWindow>of(Time.hours(1)))
      sourceStreamTra.addSink(new MySQLSink());  
      sourceStream.print();
      env.execute("data to mysql start");  
  }

	private static class SummingWindowFunction
			implements AllWindowFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Window> {

		public void apply(Window window, Iterable<Tuple2<Integer, Integer>> values,
				Collector<Tuple2<Integer, Integer>> out) {
			Integer sum = 0;
			Integer count = 0;
			for (Tuple2<Integer, Integer> value : values) {
				sum += value.f1;
				count += 1;
			}

			out.collect(new Tuple2<>(count, sum));
		}

	}

	public static class WC {
		public String word;
		public long frequency;

		// public constructor to make it a Flink POJO
		public WC() {
		}

		public WC(String word, long frequency) {
			this.word = word;
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + frequency;
		}
	}

	private static class Tuple3KeySelector implements KeySelector<Tuple3<String, String, Integer>, String> {

		@Override
		public String getKey(Tuple3<String, String, Integer> value) throws Exception {
			return value.f0;
		}
	}
}