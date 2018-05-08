package com.xxr.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;

public class WikipediaAnalysis {
	public static void main(String[] args) throws Exception {

	    StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

	    DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

	    KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
	      .keyBy(new KeySelector<WikipediaEditEvent, String>() {
	        @Override
	        public String getKey(WikipediaEditEvent event) {
	          return event.getUser();
	        }
	      });

	    DataStream<Tuple3<String, Long,Long>> result = keyedEdits
	      .timeWindow(Time.seconds(10))
	      .fold(new Tuple3<>("", 0L,0L), new FoldFunction<WikipediaEditEvent, Tuple3<String, Long,Long>>() {
	        @Override
	        public Tuple3<String, Long,Long> fold(Tuple3<String, Long,Long> acc, WikipediaEditEvent event) {
	          acc.f0 = event.getUser().trim();
	          acc.f1 += event.getByteDiff();
	          acc.f2 = System.currentTimeMillis();
	          return acc;
	        }
	      });

	    result
	    .map(new MapFunction<Tuple3<String,Long,Long>, String>() {
	        @Override
	        public String map(Tuple3<String, Long,Long> tuple) {
	            return tuple.toString();
	        }
	    })
	    .addSink(new FlinkKafkaProducer08<>("localhost:9092", "wiki-result", new SimpleStringSchema()));
	    result.print();
	    see.execute();
	  }
}