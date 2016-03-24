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
                                String deploymentName, String clusterName, Ini config, int workerSize, int gatewaySize) throws ApiException {
        if(workerSize < 3) {
            logger.info("Worker node count cannot be less than 3.");
            return clusterName;
        }

        if(gatewaySize < 1) {
            logger.info("Gateway node count cannot be less than 1.");
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

        if (workerVirtualInstances.size() > workerSize) {
            int i = workerVirtualInstances.size()-1;
            while (workerVirtualInstances.size() > workerSize) {
                workerVirtualInstances.remove(i);
                i--;
            }
        } else {
            VirtualInstance currentVirtualInstance = workerVirtualInstances.get(0);
            while(workerVirtualInstances.size() < workerSize) {
                VirtualInstance newVirtualInstance = copyVirtualInstanceWithRandomId(currentVirtualInstance);
                workerVirtualInstances.add(newVirtualInstance);
            }
        }


        VirtualInstanceGroup gatewayGroup = template.getVirtualInstanceGroups().get("gateway");


        List<VirtualInstance> gatewayGroupInstances = gatewayGroup.getVirtualInstances();

        if(gatewayGroupInstances.size() == 0) {
            logger.info("Number of gateway instances if zero");
            return clusterName;
        }

        if(gatewayGroupInstances.get(0) == null) {
            logger.info("Gateway Virtual Instances Template is null at index 0");
            return clusterName;
        }

        if (gatewayGroupInstances.size() > gatewaySize) {
            int i = gatewayGroupInstances.size()-1;
            while (gatewayGroupInstances.size() > gatewaySize) {
                gatewayGroupInstances.remove(i);
                i--;
            }
        } else {
            VirtualInstance currentVirtualInstance = gatewayGroupInstances.get(0);
            while(gatewayGroupInstances.size() < gatewaySize) {
                VirtualInstance newVirtualInstance = copyVirtualInstanceWithRandomId(currentVirtualInstance);
                gatewayGroupInstances.add(newVirtualInstance);
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
        //
        int workerSize = Integer.parseInt(config.get("worker", "size"));
        //
        int gatewaySize = Integer.parseInt(config.get("gateway", "size"));


        try {

            logger.info("Growing or Shrinking an existing CDH cluster...");
            clusterName = modifyCluster(client, environmentName, deploymentName, clusterName, config, workerSize, gatewaySize);

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