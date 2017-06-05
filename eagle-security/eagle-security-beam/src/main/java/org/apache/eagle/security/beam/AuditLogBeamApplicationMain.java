package org.apache.eagle.security.beam;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.sdk.Pipeline;

public class AuditLogBeamApplicationMain {
    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        Pipeline pipeline = new AuditLogBeamApplication().execute(config, null);
        SparkPipelineResult res = (SparkPipelineResult) pipeline.run();
        res.waitUntilFinish();
    }
}
