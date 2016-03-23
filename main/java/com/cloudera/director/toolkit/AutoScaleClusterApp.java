package com.cloudera.director.toolkit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs as a scheduled service every minute.
 */
public class AutoScaleClusterApp {

    public static void main(String[] args) throws Exception {
        final String[]  arguments = args;
        final Logger logger = Logger.getLogger(AutoScaleClusterApp.class);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);
        executor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    AutoScaleCluster cluster = new AutoScaleCluster();
                    JCommander jc = new JCommander(cluster);
                    jc.setProgramName("AutoScaleCluster");
                    try {
                        jc.parse(arguments);

                    } catch (ParameterException e) {
                        System.err.println(e.getMessage());
                        jc.usage();
                        System.exit(-1);
                    }
                    cluster.start();
                } catch (Exception e) {
                    logger.error("ERROR: ", e);
                }
            }
        }, 0, 1, TimeUnit.MINUTES);

    }
}
