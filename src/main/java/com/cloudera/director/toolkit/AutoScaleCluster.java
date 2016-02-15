package com.cloudera.director.toolkit;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.cloudera.director.client.common.ApiClient;

import java.io.File;
import java.io.FileNotFoundException;

import org.joda.time.*;

import org.ini4j.Ini;


/**
 * Example on how to use the Cloudera Director API to auto scale a cluster based on time of the day.
 * Adds worker nodes when the peak period starts.
 * Removes worker nodes added earlier when the peak period ends.
 */
@Parameters(commandDescription = "Auto Scale a cluster based on time of the day")
public class AutoScaleCluster extends CommonParameters {

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
            System.out.println("No action taken at this time of the day..." + systemTime);
            return -3;
        }

        int clusterSize = Integer.parseInt(config.get("cluster", "size"));

        if(systemTime.equals(peakTime)) {
            clusterSize = Integer.parseInt(config.get("autoscaling", "peakSize"));
        }

        System.out.println("Growing or Shrinking an existing CDH cluster...");
        GrowOrShrinkCluster cluster = new GrowOrShrinkCluster();
        clusterName = cluster.modifyCluster(client, environmentName, deploymentName, clusterName, config, clusterSize);

        System.out.println("Waiting for the cluster to be ready. Check the web interface for details.");
        waitForCluster(client, environmentName, deploymentName, clusterName);

        return 0;
    }


}
