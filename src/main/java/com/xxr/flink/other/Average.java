package com.xxr.flink.other;

import org.apache.flink.api.common.functions.AggregateFunction;

//implementation of an aggregation function for an 'average'
public class Average implements AggregateFunction<Integer, AverageAccumulator, Double> {

  public AverageAccumulator createAccumulator() {
      return new AverageAccumulator();
  }

  public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
      a.count += b.count;
      a.sum += b.sum;
      return a;
  }

  public AverageAccumulator add(Integer value, AverageAccumulator acc) {
      acc.sum += value;
      acc.count++;
      return acc;
  }

  public Double getResult(AverageAccumulator acc) {
      return acc.sum / (double) acc.count;
  }
}
