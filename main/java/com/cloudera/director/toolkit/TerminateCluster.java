package com.cloudera.director.toolkit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.cloudera.director.client.common.ApiClient;
import com.cloudera.director.client.common.ApiException;
import com.cloudera.director.client.latest.api.ClustersApi;
import com.cloudera.director.client.latest.model.ClusterTemplate;
import com.cloudera.director.client.latest.model.Status;
import com.cloudera.director.client.latest.model.VirtualInstance;
import com.cloudera.director.client.latest.model.VirtualInstanceGroup;
import org.apache.log4j.Logger;
import org.ini4j.Ini;

import java.util.List;

/**
 * Example on how to use API to delete an existing cluster.
 */

@Parameters(commandDescription = "Delete a Cloudera cluster")
public class TerminateCluster extends CommonParameters {

    final static Logger logger = Logger.getLogger(TerminateCluster.class);

    /**
     * Delete a CDH cluster with data from the configuration file.
     */
    public String deleteCluster(ApiClient client, String environmentName,
                                String deploymentName, String clusterName) throws ApiException {

        ClustersApi api = new ClustersApi(client);

        api.delete(environmentName, deploymentName, clusterName);

        return clusterName;
    }

    /**
     * Go through the steps for deleting a cluster based on the configuration file.
     */
    public int run() throws Exception {

        ApiClient client = newAuthenticatedApiClient(this);
        loadClusterConfigs(client);

        try {
            logger.info("Deleting an existing CDH cluster...");
            clusterName = deleteCluster(client, environmentName, deploymentName, clusterName);

            logger.info("Waiting for the cluster to be deleted. Check the web interface for details.");
            waitForClusterTermination(client, environmentName, deploymentName, clusterName);
        }
        catch(Exception e) {

            logger.error("ERROR: ", e);
        }

        return 0;
    }

    /**
     * Wait for cluster termination process to complete.
     */
    private void waitForClusterTermination(ApiClient client, String environmentName, String deploymentName,
                                  String clusterName) throws InterruptedException, ApiException {

        ClustersApi api = new ClustersApi(client);
        String stage = null;
        do {
            waitAndReportProgress();
            stage = api.getStatus(environmentName, deploymentName, clusterName).getStage();

        } while (!terminatedOrFailed(stage));

        logger.info(clusterName + " Cluster " + stage + " current stage is " + stage + " " + clusterName);
    }

    protected boolean terminatedOrFailed(String stage) {
        return Status.Stage.TERMINATED.equals(stage) || Status.Stage.TERMINATE_FAILED.equals(stage);
    }

    public static void main(String[] args) throws Exception {
        TerminateCluster cluster = new TerminateCluster();

        JCommander jc = new JCommander(cluster);
        jc.setProgramName("TerminateCluster");

        try {
            jc.parse(args);

        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        System.exit(cluster.run());
    }
}
