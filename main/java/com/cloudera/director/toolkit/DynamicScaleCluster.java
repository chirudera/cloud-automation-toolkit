package com.cloudera.director.toolkit;

import com.beust.jcommander.Parameters;
import com.cloudera.director.client.common.ApiClient;


import org.apache.log4j.Logger;
import org.joda.time.*;
import org.joda.time.format.*;

import com.cloudera.api.*;
import com.cloudera.api.v10.*;
import com.cloudera.api.model.*;
import com.cloudera.api.v6.*;

import java.util.Calendar;

/**
 * Example on how to use the Cloudera Director API to dynamically scale a cluster based on the workload.
 * Increments worker nodes if the cluster load average percent > load average threshold 3 consecutive 5 min intervals.
 * Decrements worker nodes if the cluster load average percent < load average threshold 12 consecutive 5 min intervals.
 * The worker node count will never go below the original node count.
 */
@Parameters(commandDescription = "Auto Scale a cluster based on workload")
public class DynamicScaleCluster extends CommonParameters {

    final static Logger logger = Logger.getLogger(DynamicScaleCluster.class);

    /**
     * Go through the steps for growing or shrinking a cluster based on the configuration file.
     */
    public int start() throws Exception {

        ApiClient client = newAuthenticatedApiClient(this);
        loadClusterConfigs(client);

        RootResourceV10 apiRoot = new ClouderaManagerClientBuilder()
                .withHost(config.get("dynamic-scaling", "cmHostName"))
                .withUsernamePassword(config.get("dynamic-scaling", "cmUsername"), config.get("dynamic-scaling",
                        "cmPassword"))
                .build()
                .getRootV10();

        TimeSeriesResourceV6 tsResource = apiRoot.getTimeSeriesResource();
        String query = "select load_1_across_hosts where clusterName = \"" + clusterName + "\"";


        Calendar now = Calendar.getInstance();
        String endPeriod = ISODateTimeFormat.dateHourMinute().print(new DateTime(now));

        now.add(Calendar.MINUTE, -1);
        String startPeriod = ISODateTimeFormat.dateHourMinute().print(new DateTime(now));

        ApiTimeSeriesResponseList response = tsResource.queryTimeSeries(query, startPeriod, endPeriod);

        double excessLoad = 0.00;
        Integer clusterSize =  ClusterLoadTracker.getInstance().getCurrentSize();

        if(response.getResponses().size() > 0) {

            double clusterLoadAvg = response.getResponses().get(0).getTimeSeries().get(0).getData().get(0)
                    .getAggregateStatistics()
                    .getMax();
            if(clusterLoadAvg < 1)
                clusterLoadAvg = 1;

            excessLoad = 100 - (((Integer.parseInt(config.get("dynamic-scaling", "num_cores_per_node")) * clusterSize)
                    /clusterLoadAvg) * 100);
            logger.info("clusterLoadAvg: " + clusterLoadAvg);
            logger.info("Excess Load: " + excessLoad);
        }
        else {
            logger.info("No response from Cloudera Manager");
            return 0;
        }

        double loadAvgThreshold = Double.parseDouble(config.get("dynamic-scaling", "loadAvgThreshold"));


        int increment = Integer.parseInt(config.get("dynamic-scaling", "increment"));
        int grow = ClusterLoadTracker.getInstance().getClusterGrow();
        int shrink = ClusterLoadTracker.getInstance().getClusterShrink();

        if(excessLoad > loadAvgThreshold) {
            grow++;
            shrink=0;
        }
        else {
            grow=0;
            shrink++;
        }

        ClusterLoadTracker.getInstance().setClusterGrow(grow);
        ClusterLoadTracker.getInstance().setClusterShrink(shrink);

        if(grow == 3) {
            clusterSize = clusterSize + increment;
            ClusterLoadTracker.getInstance().setClusterGrow(0);
        }
        else if(shrink == 12) {
            clusterSize = clusterSize - increment;
            ClusterLoadTracker.getInstance().setClusterShrink(0);
        }
        else {
            logger.info("No action taken at this time.");
            return 0;
        }

       if(clusterSize < ClusterLoadTracker.getInstance().getOriginalSize()) {
           logger.info("No action taken at this time. Attempted to reduce the cluster size to less than the original " +
                   "size");
           return 0;
       }

        GrowOrShrinkCluster cluster = new GrowOrShrinkCluster();
        logger.info("DynamicScaling existing CDH cluster...");
        clusterName = cluster.modifyCluster(client, environmentName, deploymentName, clusterName, config,
                    clusterSize);
        logger.info("Waiting for the cluster to be ready. Check the web interface for details.");
        waitForCluster(client, environmentName, deploymentName, clusterName);
        ClusterLoadTracker.getInstance().setCurrentSize(clusterSize);

        return 0;
    }
}
