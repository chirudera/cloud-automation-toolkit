package com.cloudera.director.toolkit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.cloudera.director.client.common.ApiClient;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs as a scheduled service every 5 minutes.
 */
public class DynamicScaleClusterApp {

    public static void main(String[] args) throws Exception {

        final String[]  arguments = args;
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);

        executor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    DynamicScaleCluster cluster = new DynamicScaleCluster();

                    JCommander jc = new JCommander(cluster);
                    jc.setProgramName("DynamicScaleCluster");
                    try {
                        jc.parse(arguments);

                    } catch (ParameterException e) {
                        System.err.println(e.getMessage());
                        jc.usage();
                        System.exit(-1);
                    }
                    int currentSize = cluster.getClusterSize();
                    if(ClusterLoadTracker.getInstance().getOriginalSize() == 0) {
                        ClusterLoadTracker.getInstance().setOriginalSize(currentSize);
                    }
                    ClusterLoadTracker.getInstance().setCurrentSize(currentSize);
                    cluster.start();
                } catch (Exception ex) {
                    ex.printStackTrace(); //or loggger would be better
                }
            }
        }, 0, 5, TimeUnit.MINUTES);

    }


}