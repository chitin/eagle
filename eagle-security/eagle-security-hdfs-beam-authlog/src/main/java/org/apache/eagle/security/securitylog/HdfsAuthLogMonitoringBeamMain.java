/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.eagle.security.securitylog;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka8.Kafka8IO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.eagle.app.BeamApplication;
import org.apache.eagle.app.environment.impl.BeamEnviroment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class HdfsAuthLogMonitoringBeamMain extends BeamApplication {
    private static Logger LOG = LoggerFactory.getLogger(HdfsAuthLogMonitoringBeamMain.class);
    private static final String DEFAULT_CONFIG_PREFIX = "dataSourceConfig";

    private String configPrefix = DEFAULT_CONFIG_PREFIX;

    private SparkPipelineResult res;

    @Override
    public Pipeline execute(Config config, BeamEnviroment environment) {
        Config context = config;
        if (this.configPrefix != null) {
            context = config.getConfig(configPrefix);
        }
        Map<String, String> consumerProps = ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");

        String topic = context.getString("topic");
        String zkConnection = context.getString("ZkConnection");
        String sinkBrokerList = config.getString("dataSinkConfig.brokerList");
        String sinkTopic = config.getString("dataSinkConfig.topic");

        Kafka8IO.Read<String, String> read = Kafka8IO.<String, String>read()
            .withBootstrapServers(zkConnection)
            .withTopics(Collections.singletonList(topic))
            .withKeyCoder(StringUtf8Coder.of())
            .withValueCoder(StringUtf8Coder.of())
            .updateKafkaClusterProperties(consumerProps);

        SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
        Duration batchIntervalDuration = Duration.standardSeconds(5);
        options.setBatchIntervalMillis(batchIntervalDuration.getMillis());
        options.setMaxRecordsPerBatch(8L);
        options.setRunner(SparkRunner.class);
        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, String>> deduped =
            p.apply(read.withoutMetadata()).apply(ParDo.of(new ExtractLogFn()));

        deduped.apply(Kafka8IO.<String, String>write()
            .withBootstrapServers(sinkBrokerList)
            .withTopic(sinkTopic)
            .withKeyCoder(StringUtf8Coder.of())
            .withValueCoder(StringUtf8Coder.of())
            .updateProducerProperties(ImmutableMap.of("bootstrap.servers", sinkBrokerList))
        );

        return p;
    }

    private static class ExtractLogFn extends DoFn<KV<String, String>, KV<String, String>> {

        @ProcessElement
        public void processElement(ProcessContext c) throws UnsupportedEncodingException {
            String log = c.element().getValue();
            LOG.info("--------------log "+log);
            Map<String, String> map = (Map<String, String>)new HdfsAuthLogKafkaDeserializer().deserialize(log.getBytes("UTF-8"));
            String message = Arrays.asList(map).get(0).toString();
            c.output(KV.of("f1", message));
        }
    }
}