/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.queries;

import flink.sinks.DummyLatencyCountingSink;
import flink.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Query1 {

    private static final Logger logger = LoggerFactory.getLogger(Query1.class);


    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final float exchangeRate = params.getFloat("exchange-rate", 0.82F);
        final long bufftimeout = params.getLong("bufferTimeout", -1);
        String ratelist = params.getRequired("ratelist");

        //  --ratelist 400000_900000_11000_300000 --bufferTimeout -1
        int[] numbers = Arrays.stream(ratelist.split("_"))
                .mapToInt(Integer::parseInt)
                .toArray();
        System.out.println(Arrays.toString(numbers));
        List<List<Integer>> rates = new ArrayList<>();
        // The internal list will be [rate, time in ms]
        for (int i = 0; i < numbers.length - 1; i += 2) {
            rates.add(Arrays.asList(numbers[i], numbers[i + 1]));
        }

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.setBufferTimeout(bufftimeout);
        
        env.disableOperatorChaining();

        // enable latency tracking
        // env.getConfig().setLatencyTrackingInterval(5000);

        DataStream<Bid> bids = env.addSource(new BidSourceFunction(rates))
                .setParallelism(params.getInt("p-source", 1))
                .name("Bids Source")
                .uid("Bids-Source")
                .slotSharingGroup("src");
        // SELECT auction, DOLTOEUR(price), bidder, datetime
        DataStream<Tuple4<Long, Long, Long, Long>> mapped = bids.map(new MapFunction<Bid, Tuple4<Long, Long, Long, Long>>() {
                    @Override
                    public Tuple4<Long, Long, Long, Long> map(Bid bid) throws Exception {
                        return new Tuple4<>(bid.auction, dollarToEuro(bid.price, exchangeRate), bid.bidder, bid.dateTime);
                    }
                }).setParallelism(params.getInt("p-map", 2))
                .name("Mapper")
                .uid("Mapper").slotSharingGroup("map");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        mapped.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-sink", 1))
                .name("Latency Sink")
                .uid("Latency-Sink").slotSharingGroup("sink");

        // execute program
        env.execute("Nexmark Query1");
    }

    private static long dollarToEuro(long dollarPrice, float rate) {
        return (long) (rate * dollarPrice);
    }

}
