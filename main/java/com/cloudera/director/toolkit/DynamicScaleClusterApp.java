package com.cloudera.director.toolkit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs as a scheduled service every 5 minutes.
 */
public class DynamicScaleClusterApp extends CommonParameters {

    public static void main(String[] args) throws Exception {

        final String[]  arguments = args;
        final Logger logger = Logger.getLogger(DynamicScaleClusterApp.class);
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
                    int currentSize = cluster.getCurrentClusterSize();
                    if(ClusterLoadTracker.getInstance().getOriginalSize() == 0) {
                        ClusterLoadTracker.getInstance().setOriginalSize(currentSize);
                    }
                    ClusterLoadTracker.getInstance().setCurrentSize(currentSize);
                    cluster.start();
                } catch (Exception e) {
                    logger.error("ERROR: ", e);
                }
            }
        }, 0, 1, TimeUnit.MINUTES);

    }


}