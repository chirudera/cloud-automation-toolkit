package com.cloudera.director.toolkit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.cloudera.director.client.common.ApiClient;
import com.cloudera.director.client.common.ApiException;
import com.cloudera.director.client.latest.api.ClustersApi;
import com.cloudera.director.client.latest.model.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;

import org.ini4j.Ini;
import org.ini4j.Profile;

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

    public Map<String,List<String>> getHashMapfromSections(String sectionName) throws IOException {

        Ini ini = new Ini(new FileReader("cluster.ini"));
        Profile.Section section = ini.get(sectionName);
        Map<String,List<String>> hMap = new HashMap<String, List<String>>();
        for (String optionKey : section.keySet()) {
            List<String> arrayList = Arrays.asList(section.get(optionKey).split("\\s*,\\s*"));
            hMap.put(optionKey,arrayList);
        }
        return hMap;
    }



    /**
     * Grow or Shrink an existing CDH cluster with data from the configuration file.
     */
    public String modifyCluster(ApiClient client, String environmentName,
                                String deploymentName, String clusterName, Ini config, int workerSize, int gatewaySize) throws ApiException, IOException {
        if(workerSize < 3) {
            logger.info("Worker node count cannot be less than 3.");
            return clusterName;
        }

        if(gatewaySize < 0) {
            logger.info("Gateway node count cannot be less than 0.");
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
            logger.info("Number of worker instances is zero");
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

        //do this only is there is presence of gateway nodes in the cluster
        if(gatewaySize >= 0){

            VirtualInstanceGroup gatewayGroup = template.getVirtualInstanceGroups().get("gateway");


            // If there were no gateway instances in the cluster create them from config
            if((gatewayGroup == null ||
                    gatewayGroup.getVirtualInstances() == null ||
                    gatewayGroup.getVirtualInstances().size() == 0 ||
                    gatewayGroup.getVirtualInstances().get(0) == null) && gatewaySize > 0) {
                logger.info("Number of gateway instances is zero, creating new instances of count :"+gatewaySize);


                Map<String, List<String>> gatewayRoles =  getHashMapfromSections("workerRoles");

                List<VirtualInstance> gatewayGroupInstances = new ArrayList<VirtualInstance>();
                String spotInstances = String.valueOf(config.get("gateway", "useSpotInstances"));
                String templateName = "gateway";
                int minCount = gatewaySize;
                if(spotInstances.equals("true")) {
                    logger.info("Trying for Spot Instances.");
                    System.out.print("Trying for Spot Instances.");
                    templateName = "gatewaywithspotinstances";
                    minCount = 0;
                }


                for (int i = 0; i < gatewaySize; i++) {
                    gatewayGroupInstances.add(createVirtualInstanceWithRandomId(config, templateName));
                }

                template.getVirtualInstanceGroups().put("gateway", VirtualInstanceGroup.builder()
                        .name("gateway")
                        .minCount(minCount)
                        .serviceTypeToRoleTypes(gatewayRoles)
                        .virtualInstances(gatewayGroupInstances)
                        .build());


            } else {
                logger.info("Number of gateway instances exists, adjusting as needed to size "+ gatewaySize);

                List<VirtualInstance> gatewayGroupInstances = gatewayGroup.getVirtualInstances();

                if (gatewayGroupInstances.size() > gatewaySize) {
                    int i = gatewayGroupInstances.size() - 1;
                    while (gatewayGroupInstances.size() > gatewaySize) {
                        gatewayGroupInstances.remove(i);
                        i--;
                    }
                    //if gateway size is zero remove it from the group.
                    if(gatewayGroupInstances.size() == 0) {
                        template.getVirtualInstanceGroups().remove("gateway");
                    }
                } else {
                    VirtualInstance currentVirtualInstance = gatewayGroupInstances.get(0);
                    while (gatewayGroupInstances.size() < gatewaySize) {
                        VirtualInstance newVirtualInstance = copyVirtualInstanceWithRandomId(currentVirtualInstance);
                        gatewayGroupInstances.add(newVirtualInstance);
                    }
                }

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