package com.cloudera.director.toolkit;

import com.beust.jcommander.Parameters;
import com.cloudera.director.client.common.ApiClient;


import org.apache.log4j.Logger;
import org.joda.time.*;


/**
 * Example on how to use the Cloudera Director API to auto scale a cluster based on time of the day.
 * Adds worker nodes when the peak period starts.
 * Removes worker nodes added earlier when the peak period ends.
 */
@Parameters(commandDescription = "Auto Scale a cluster based on time of the day")
public class AutoScaleCluster extends CommonParameters {

    final static Logger logger = Logger.getLogger(AutoScaleCluster.class);

    /**
     * Go through the steps for growing or shrinking a cluster based on the configuration file.
     */
    public int start() throws Exception {

        ApiClient client = newAuthenticatedApiClient(this);
        loadClusterConfigs(client);

        DateTime dt = new DateTime();  // current time

        String systemTime = dt.getHourOfDay() + ":" + dt.getMinuteOfHour();

        String peakTime = config.get("autoscaling", "peakHourStart");
        String offPeakTime = config.get("autoscaling", "peakHourEnd");

        if(!systemTime.equals(peakTime) && !systemTime.equals(offPeakTime)) {
            logger.info("No action taken at this time of the day..." + systemTime);
            return -3;
        }

        int clusterWorkerSize = getCurrentClusterWorkersSize();

        if(systemTime.equals(peakTime)) {
            clusterWorkerSize = clusterWorkerSize + Integer.parseInt(config.get("autoscaling", "workersIncrement"));
        }
        else if(systemTime.equals(offPeakTime)) {
            clusterWorkerSize = clusterWorkerSize - Integer.parseInt(config.get("autoscaling", "workersIncrement"));
        }

        int clusterGatewaySize = getCurrentClusterGatewaySize();

        if(systemTime.equals(peakTime)) {
            clusterGatewaySize = clusterGatewaySize + Integer.parseInt(config.get("autoscaling", "gatewayIncrement"));
        }
        else if(systemTime.equals(offPeakTime)) {
            clusterGatewaySize = clusterGatewaySize - Integer.parseInt(config.get("autoscaling", "gatewayIncrement"));
        }


        logger.info("Autoscaling existing CDH cluster...");
        GrowOrShrinkCluster cluster = new GrowOrShrinkCluster();
        clusterName = cluster.modifyCluster(client, environmentName, deploymentName, clusterName, config, clusterWorkerSize, clusterGatewaySize);

        logger.info("Waiting for the cluster to be ready. Check the web interface for details.");
        waitForCluster(client, environmentName, deploymentName, clusterName);

        return 0;
    }


}
