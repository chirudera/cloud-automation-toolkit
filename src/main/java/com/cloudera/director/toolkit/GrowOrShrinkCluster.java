package com.cloudera.director.toolkit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.cloudera.director.client.common.ApiClient;
import com.cloudera.director.client.common.ApiException;
import com.cloudera.director.client.latest.api.ClustersApi;
import com.cloudera.director.client.latest.api.EnvironmentsApi;
import com.cloudera.director.client.latest.model.ClusterTemplate;
import com.cloudera.director.client.latest.model.VirtualInstance;
import com.cloudera.director.client.latest.model.VirtualInstanceGroup;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import org.ini4j.Ini;

/**
 * Example on how to use API to grow or shrink an existing cluster.
 */

@Parameters(commandDescription = "Grow or Shrink a Cloudera cluster")

public class GrowOrShrinkCluster extends CommonParameters {

    /**
     * Grow or Shrink an existing CDH cluster with data from the configuration file.
     */
    public String modifyCluster(ApiClient client, String environmentName,
                                 String deploymentName, String clusterName, Ini config, int clusterSize) throws ApiException {
        if(clusterSize < 3)
            return clusterName;

        ClustersApi api = new ClustersApi(client);
        ClusterTemplate template = null;

        try {
            template = api.getTemplateRedacted(environmentName, deploymentName, clusterName);
        }
        catch (ApiException e) {
            if (e.getCode() == 404 || e.getCode() == 400) {
                System.out.println("Invalid environment name or deployment name or cluster name.");
                throw e;
            }
        }

        VirtualInstanceGroup workersGroup = template.getVirtualInstanceGroups().get("workers");

        List<VirtualInstance> workerVirtualInstances = workersGroup.getVirtualInstances();

        if (workerVirtualInstances.size() > clusterSize) {
            int i = workerVirtualInstances.size()-1;
            while (workerVirtualInstances.size() > clusterSize) {
                workerVirtualInstances.remove(i);
                i--;
            }
        } else {
            while(workerVirtualInstances.size() < clusterSize) {
                VirtualInstance virtualInstance = createVirtualInstanceWithRandomId(config, "worker");
                workerVirtualInstances.add(virtualInstance);
            }
        }

        api.update(environmentName, deploymentName, clusterName, template);

        return clusterName;
    }

    /**
     * Get worker node count of the existing cluster.
     */
    public int getClusterSize() throws Exception {

        ApiClient client = newAuthenticatedApiClient(this);
        loadClusterConfigs(client);

        ClustersApi api = new ClustersApi(client);
        ClusterTemplate template = api.getTemplateRedacted(environmentName, deploymentName, clusterName);

        VirtualInstanceGroup workersGroup = template.getVirtualInstanceGroups().get("workers");

        List<VirtualInstance> workerVirtualInstances = workersGroup.getVirtualInstances();

        return workerVirtualInstances.size();
    }

    /**
     * Go through the steps for growing or shrinking a cluster based on the configuration file.
     */
    public int run() throws Exception {

        ApiClient client = newAuthenticatedApiClient(this);
        loadClusterConfigs(client);

        int clusterSize = Integer.parseInt(config.get("cluster", "size"));

        System.out.println("Growing or Shrinking an existing CDH cluster...");
        clusterName = modifyCluster(client, environmentName, deploymentName, clusterName, config, clusterSize);

        System.out.println("Waiting for the cluster to be ready. Check the web interface for details.");
        waitForCluster(client, environmentName, deploymentName, clusterName);

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
