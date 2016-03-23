package com.cloudera.director.toolkit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.cloudera.director.client.common.ApiClient;
import com.cloudera.director.client.common.ApiException;
import com.cloudera.director.client.latest.api.ClustersApi;
import com.cloudera.director.client.latest.model.*;

import java.util.List;

import org.apache.log4j.Logger;

import org.ini4j.Ini;
import java.util.UUID;

/**
 * Example on how to use API to grow or shrink an existing cluster.
 */

@Parameters(commandDescription = "Grow or Shrink a Cloudera cluster on demand")

public class GrowOrShrinkCluster extends CommonParameters {
    final static Logger logger = Logger.getLogger(GrowOrShrinkCluster.class);

    private VirtualInstance copyVirtualInstanceWithRandomId(VirtualInstance existingVirtualInstance) {
        return VirtualInstance.builder()
                .id(UUID.randomUUID().toString())
                .template(existingVirtualInstance.getTemplate())
                .build();
    }

    /**
     * Grow or Shrink an existing CDH cluster with data from the configuration file.
     */
    public String modifyCluster(ApiClient client, String environmentName,
                                String deploymentName, String clusterName, Ini config, int clusterSize) throws ApiException {
        if(clusterSize < 3) {
            logger.info("Worker node count cannot be less than 3.");
            return clusterName;
        }

        ClustersApi api = new ClustersApi(client);
        ClusterTemplate template = null;

        try {
            template = api.getTemplateRedacted(environmentName, deploymentName, clusterName);
            template.setRedeployClientConfigsOnUpdate(true);
        }
        catch (ApiException e) {
            throw e;
        }

        VirtualInstanceGroup workersGroup = template.getVirtualInstanceGroups().get("workers");

        List<VirtualInstance> workerVirtualInstances = workersGroup.getVirtualInstances();

        if(workerVirtualInstances.size() == 0) {
            logger.info("Number of worker instances if zero");
            return clusterName;
        }

        if(workerVirtualInstances.get(0) == null) {
            logger.info("Worker Virtual Instances Template is null at index 0");
            return clusterName;
        }

        if (workerVirtualInstances.size() > clusterSize) {
            int i = workerVirtualInstances.size()-1;
            while (workerVirtualInstances.size() > clusterSize) {
                workerVirtualInstances.remove(i);
                i--;
            }
        } else {
            VirtualInstance currentVirtualInstance = workerVirtualInstances.get(0);
            while(workerVirtualInstances.size() < clusterSize) {
                VirtualInstance newVirtualInstance = copyVirtualInstanceWithRandomId(currentVirtualInstance);
                workerVirtualInstances.add(newVirtualInstance);
            }
        }

        api.update(environmentName, deploymentName, clusterName, template);

        return clusterName;
    }


    /**
     * Go through the steps for growing or shrinking a cluster based on the configuration file.
     */
    public int run() throws Exception {

        ApiClient client = newAuthenticatedApiClient(this);
        loadClusterConfigs(client);

        int clusterSize = Integer.parseInt(config.get("worker", "size"));

        try {

            logger.info("Growing or Shrinking an existing CDH cluster...");
            clusterName = modifyCluster(client, environmentName, deploymentName, clusterName, config, clusterSize);

            logger.info("Waiting for the cluster to be ready. Check the web interface for details.");
            waitForCluster(client, environmentName, deploymentName, clusterName);
        }
        catch(Exception e) {

            logger.error("ERROR: ", e);
        }

        return 0;
    }


    public static void main(String[] args) throws Exception {
        GrowOrShrinkCluster cluster = new GrowOrShrinkCluster();

        JCommander jc = new JCommander(cluster);
        jc.setProgramName("GrowOrShrinkCluster");

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