package org.apache.eagle.security.beam;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.impl.BeamEnviroment;
import org.junit.Ignore;
import org.junit.Test;

public class TestAuditLogBeamApplication {
    private BeamEnviroment environment;
    @Ignore
    @Test
    public void testPipeline() {
        Application<BeamEnviroment, Pipeline> executor = new AuditLogBeamApplication();

        Config config = ConfigFactory.load("application-test.conf");

        String appId = config.getString("appId");
        Pipeline pipeline = executor.execute(config, environment);
        // Run the pipeline.
        SparkPipelineResult res = (SparkPipelineResult) pipeline.run();
        res.waitUntilFinish();
    }
}
