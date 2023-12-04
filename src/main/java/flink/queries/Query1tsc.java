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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.bu.cs.sesa.tsclog.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Query1tsc {

    private static final Logger logger = LoggerFactory.getLogger(Query1tsc.class);
    private static float exchangeRate;
    //private static int[] pincores;
    
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        exchangeRate = params.getFloat("exchange-rate", 0.82F);
        final long bufftimeout = params.getLong("bufferTimeout", -1);
        String ratelist = params.getRequired("ratelist");

	// pin cores
	/*String spincores = params.get("pincores", "");
	if (spincores != "") {
	    pincores = Arrays.stream(spincores.split("_"))
		.mapToInt(Integer::parseInt)
		.toArray();
	    System.out.println("pincores: "+Arrays.toString(pincores));
	} else {
	    System.out.println("No cores to pin");
	    }*/
	
        // --ratelist 400000_900000_11000_300000 --bufferTimeout -1
        int[] numbers = Arrays.stream(ratelist.split("_"))
	    .mapToInt(Integer::parseInt)
	    .toArray();
        System.out.println("ratelist: "+Arrays.toString(numbers));
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
        //env.getConfig().setLatencyTrackingInterval(5000);

        DataStream<Bid> bids = env.addSource(new BidSourceFunction(rates))
                .setParallelism(params.getInt("p-source", 1))
                .name("Bids Source")
                .uid("Bids-Source")
                .slotSharingGroup("src");
        // SELECT auction, DOLTOEUR(price), bidder, datetime
        DataStream<Tuple4<Long, Long, Long, Long>> mapped = bids
                .flatMap(new tscMapper()).setParallelism(params.getInt("p-map", 2))
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

    public static final class tscMapper extends RichFlatMapFunction<Bid, Tuple4<Long, Long, Long, Long>> {
        // TODO: use open()/close() under RichMapFunction to create/clean a mem buffer(to write log to) before/after init Mapper
        //int subtaskIndex;

        @Override
        public void flatMap(Bid _in, Collector<Tuple4<Long, Long, Long, Long>> _out) throws Exception {
	    //System.out.println("flatMap cpu: " + tsclog.cpu());
            _out.collect(new Tuple4<>(_in.auction, dollarToEuro(_in.price, exchangeRate), _in.bidder, _in.dateTime));
        }
        @Override
        public void open(Configuration cfg){
	    int avail = tsclog.availcpus();
	    int cpu = tsclog.cpu();
	    int tid = tsclog.tid();
	    int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
	    int pcpu = 0;
	    
	    System.out.println("[PRE PIN]: available cpus: "+avail+" open() cpu: "+cpu+" tid: "+tid+" subtaskIndex: "+subtaskIndex);
	    System.out.println("---------------------------------------");

	    /*switch(subtaskIndex) {
	    case 0:
		pcpu = 1;
		break;
	    case 1:
		pcpu = 3;
		break;
	    case 2:
		pcpu = 5;
		break;
	    case 3:
		pcpu = 7;
		break;
	    case 4:
		pcpu = 9;
		break;
	    case 5:
		pcpu = 11;
		break;
	    case 6:
		pcpu = 13;
		break;
	    case 7:
		pcpu = 15;
		break;
	    default:
		break;
	    }
	    
	    System.out.println("[PIN]: Pinning to core " + pcpu);
	    tsclog.pin(pcpu);
	    System.out.println("---------------------------------------");*/
		
	    /*System.out.println(pincores.length);
	    
	    if (pincores.length > 0) {
	    System.out.println("[PIN]: Pinning to core " + pincores[subtaskIndex]);
		tsclog.pin(pincores[subtaskIndex]);
		System.out.println("---------------------------------------");
		}*/

	    System.out.println("[PIN]: Pinning to core " + subtaskIndex);
	    tsclog.pin(subtaskIndex);
	    System.out.println("---------------------------------------");
	    
	    avail = tsclog.availcpus();
	    cpu = tsclog.cpu();
	    tid = tsclog.tid();
	    System.out.println("[POST PIN]: available cpus: " + avail + " open() cpu: " + cpu + " tid: " + tid);
	    System.out.println("---------------------------------------");
	}
	/*
        @Override
        public void close(){
            logger.info("mapper"+subtaskIndex+"  close");
        }*/
    }

    /*private static void tsclogging(){
        int avail = tsclog.availcpus();
        int cpu = tsclog.cpu();
        int tid = tsclog.tid();
        long tsc = tsclog.now();

        System.out.println("available cpus: " + avail);
        System.out.println("cpu: " + cpu + " tid: " + tid +
                " now: " + Long.toUnsignedString(tsc));

        tsclog.stdout_label_now("pre pin");
        tsclog.pin(avail-1);
        tsclog.stdout_label_now("post pin");
        cpu = tsclog.cpu();
        tid = tsclog.tid();
        tsc = tsclog.now();
        System.out.println("cpu: " + cpu + " tid: " + tid +
                " now: " + Long.toUnsignedString(tsc));

        tsclog.stdout_now();
        tsclog.stderr_now();
        tsclog.stdout_label_now("mapper1");
        tsclog.stderr_label_now("mapper2");

        tsclog log = new tsclog("",10,0,"");
        for (int i=0; i<10; i++) {
            log.log();
        }

        tsclog log1 = new tsclog("JAVALOG", 10,1,"i");
        for (int i=0; i<10; i++) {
            log1.log1(i);
        }
	}*/

    private static long dollarToEuro(long dollarPrice, float rate) {
        return (long) (rate * dollarPrice);
    }
}
