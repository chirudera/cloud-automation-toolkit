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
                                String deploymentName, String clusterName, Ini config, boolean scaleWorkes, int workerSize, String groupName, int groupSize) throws ApiException, IOException {
        if (scaleWorkes && workerSize < 3) {
            logger.info("Worker node count cannot be less than 3.");
            return clusterName;
        }

        if (groupName != null && groupSize < 0) {
            logger.info(groupName + " node count cannot be less than 0.");
            return clusterName;
        }


        ClustersApi api = new ClustersApi(client);
        ClusterTemplate template = null;

        try {
            template = api.getTemplateRedacted(environmentName, deploymentName, clusterName);
            template.setRedeployClientConfigsOnUpdate(true);
        } catch (ApiException e) {
            throw e;
        }


        if (scaleWorkes) {
            VirtualInstanceGroup workersGroup = template.getVirtualInstanceGroups().get("workers");


            List<VirtualInstance> workerVirtualInstances = workersGroup.getVirtualInstances();

            if (workerVirtualInstances.size() == 0) {
                logger.info("Number of worker instances is zero");
                return clusterName;
            }

            if (workerVirtualInstances.get(0) == null) {
                logger.info("Worker Virtual Instances Template is null at index 0");
                return clusterName;
            }

            if (workerVirtualInstances.size() > workerSize) {
                int i = workerVirtualInstances.size() - 1;
                while (workerVirtualInstances.size() > workerSize) {
                    workerVirtualInstances.remove(i);
                    i--;
                }
            } else {
                VirtualInstance currentVirtualInstance = workerVirtualInstances.get(0);
                while (workerVirtualInstances.size() < workerSize) {
                    VirtualInstance newVirtualInstance = copyVirtualInstanceWithRandomId(currentVirtualInstance);
                    workerVirtualInstances.add(newVirtualInstance);
                }
            }
        }
        if (groupName != null){
            updateNullableNodeGroup(config, template, groupName, groupSize);
        }
        api.update(environmentName, deploymentName, clusterName, template);

        return clusterName;
    }


    private void updateNullableNodeGroup(Ini config, ClusterTemplate template, String groupName, int groupSize ) throws IOException {


        //do this only is there is presence of group nodes in the cluster
        if(groupSize >= 0){

            VirtualInstanceGroup customGroup = template.getVirtualInstanceGroups().get(groupName);


            // If there were no gateway instances in the cluster create them from config
            if((customGroup == null ||
                    customGroup.getVirtualInstances() == null ||
                    customGroup.getVirtualInstances().size() == 0 ||
                    customGroup.getVirtualInstances().get(0) == null) && groupSize > 0) {
                logger.info("Number of "+groupName +" instances is zero, creating new instances of count :"+groupSize);


                Map<String, List<String>> gatewayRoles =  getHashMapfromSections((groupName+"Roles"));

                List<VirtualInstance> groupInstances = new ArrayList<VirtualInstance>();
                String spotInstances = String.valueOf(config.get(groupName, "useSpotInstances"));
                String templateName = groupName;
                int minCount = groupSize;
                if(spotInstances.equals("true")) {
                    logger.info("Trying for Spot Instances.");
                    System.out.print("Trying for Spot Instances.");
                    templateName = groupName+"withspotinstances";
                    minCount = 0;
                }


                for (int i = 0; i < groupSize; i++) {
                    groupInstances.add(createVirtualInstanceWithRandomId(config, templateName));
                }

                template.getVirtualInstanceGroups().put(groupName, VirtualInstanceGroup.builder()
                        .name(groupName)
                        .minCount(minCount)
                        .serviceTypeToRoleTypes(gatewayRoles)
                        .virtualInstances(groupInstances)
                        .build());

                // do this if we are adding the nodes for the first time.
            } else {
                logger.info("Number of "+groupName+" instances exists, adjusting as needed to size "+ groupSize);

                List<VirtualInstance> groupInstances = customGroup.getVirtualInstances();

                if (groupInstances.size() > groupSize) {
                    int i = groupInstances.size() - 1;
                    while (groupInstances.size() > groupSize) {
                        groupInstances.remove(i);
                        i--;
                    }
                    //if group size is zero remove it from the group template.
                    if(groupInstances.size() == 0) {
                        template.getVirtualInstanceGroups().remove(groupName);
                    }
                } else {
                    VirtualInstance currentVirtualInstance = groupInstances.get(0);
                    while (groupInstances.size() < groupSize) {
                        VirtualInstance newVirtualInstance = copyVirtualInstanceWithRandomId(currentVirtualInstance);
                        groupInstances.add(newVirtualInstance);
                    }
                }

            }

        }


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
            clusterName = modifyCluster(client, environmentName, deploymentName, clusterName, config, true, workerSize, "gateway", gatewaySize);

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