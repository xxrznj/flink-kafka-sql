package com.xxr.flink;


import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

import akka.actor.FSM.Event;
import kafka.common.Config;
import com.xxr.flink.JDBCTestBase;

public class WikiToDB {
	public static void main(String[] args) throws Exception { 
	
      Properties pro = new Properties();  
      pro.put("bootstrap.servers", JDBCTestBase.kafka_hosts);  
      pro.put("zookeeper.connect", JDBCTestBase.kafka_zookper);  
      pro.put("group.id",JDBCTestBase.kafka_group);  
      StreamExecutionEnvironment env = StreamExecutionEnvironment  
              .getExecutionEnvironment();  
      //env.getConfig().disableSysoutLogging();  //设置此可以屏蔽掉日记打印情况  
      env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));  
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
      env.enableCheckpointing(5000);  
      DataStream<String> sourceStream = env  
              .addSource(new FlinkKafkaConsumer08<String>(JDBCTestBase  
                      .kafka_topic, new SimpleStringSchema(),  
                      pro));  

      DataStream<Tuple3<String,Long, Long>> sourceStreamTra = sourceStream.filter(new FilterFunction<String>() {   
          @Override  
          public boolean filter(String value) throws Exception {  
              return StringUtils.isNotBlank(value);  
          }  
      }).map(new MapFunction<String, Tuple3<String,Long, Long>>() {  
                  @Override  
                  public Tuple3<String,Long, Long> map(String value)  
                          throws Exception { 
                	  String temp=value.replaceAll("(\\(|\\))", "");
                      String[] args = temp.split(",");  
                      return new Tuple3<String,Long, Long>(args[0], Long  
                              .valueOf(args[1]),Long  
                              .valueOf(args[2]));  
                  }  
              });  
     
      DataStream<Tuple3<String,Long, Long>> withTimestampsAndWatermarks =sourceStreamTra.assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks());
      //.windowAll(TumblingEventTimeWindows.of(Time.days(1))).allowedLateness(Time.minutes(60))
      DataStream<Tuple2<Long, Integer>>aDataStream=withTimestampsAndWatermarks.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(10000)))
      //.fold(new Tuple3<String, Long, Long>("",0L, 0L), new MyFoldFunction(), new MyWindowFunction());

      .apply(new RichAllWindowFunction<Tuple3<String,Long, Long>,Tuple2<Long, Integer>, TimeWindow>(){
//
//		@Override
//		public void apply(TimeWindow window, Iterable<Tuple3<String,Long, Long>> values,
//				Collector<Tuple3<String,Long, Long>> out) throws Exception {
//			// TODO Auto-generated method stub
//			Long sum = 0L;
//			Long count = 0L;
//			String stringl="";	
//			for (Tuple3<String,Long, Long> value : values) {
//				sum += value.f1;
//				count+=1;
//			}
//
//			out.collect(new Tuple3<>(stringl,sum/count, count));
//		}


@Override
public void apply(TimeWindow window, Iterable<Tuple3<String, Long, Long>> values, Collector<Tuple2<Long, Integer>> out)
		throws Exception {
	// TODO Auto-generated method stub
	Long sum = 0L;
	Integer count = 0;
	for (Tuple3<String,Long, Long> value : values) {
		sum += value.f1;
		count+=1;
	}

	out.collect(new Tuple2<>(sum, count));
}
    	  
      });

      //.trigger(ContinuousEventTimeTrigger.<TimeWindow>of(Time.hours(1)))
      withTimestampsAndWatermarks.addSink(new WikiSQLSink());  
      withTimestampsAndWatermarks.print();
      aDataStream.print();
      env.execute("data to mysql start");  
  }
	private static class MyFoldFunction
    implements FoldFunction<Tuple3<String, Long, Integer> , Tuple2<Long, Integer> > {
  public Tuple2<Long, Integer> fold(Tuple2<Long, Integer> acc, Tuple3<String, Long, Integer> s) {
      Integer cur = acc.getField(2);
      acc.setField(2, cur + 1);
      return acc;
  }
}
private static class MyWindowFunction
    implements WindowFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Long, TimeWindow> {
  public void apply(Long key,
                    TimeWindow window,
                    Iterable<Tuple2<Long, Integer>> counts,
                    Collector<Tuple2<Long, Integer>> out) {
    Integer count = counts.iterator().next().getField(2);
    out.collect(new Tuple2<Long, Integer>(key,count));
  }
}



}