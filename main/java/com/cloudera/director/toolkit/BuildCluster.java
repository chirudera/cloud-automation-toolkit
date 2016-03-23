package com.cloudera.director.toolkit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.cloudera.director.client.common.ApiClient;
import com.cloudera.director.client.common.ApiException;
import com.cloudera.director.client.latest.api.ClustersApi;
import com.cloudera.director.client.latest.api.DeploymentsApi;
import com.cloudera.director.client.latest.api.EnvironmentsApi;
import com.cloudera.director.client.latest.model.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;
import java.io.FileReader;


/**
 * Example on how to use API to build a new cluster from scratch.
 */

@Parameters(commandDescription = "Build a new Cloudera cluster on Cloud")
public class BuildCluster extends CommonParameters {

    final static Logger logger = Logger.getLogger(BuildCluster.class);

    /**
     * Create a new environment with data from the configuration file.
     *
     * @param client authenticated API client
     * @param config parsed configuration file
     * @return the name of the new environment
     * @throws FileNotFoundException if the key file can't be found
     * @throws ApiException
     */
    private String createEnvironment(ApiClient client, Ini config, String environmentName)
            throws FileNotFoundException, ApiException {
        String clusterName = config.get("cluster", "name");

        SshCredentials credentials = SshCredentials.builder()
                .username(config.get("ssh", "username"))
                .privateKey(readFile(config.get("ssh", "privateKey")))
                .port(22)
                .build();

        Map<String, String> properties = new HashMap<String, String>();
        /*properties.put("accessKeyId", config.get("provider", "accessKeyId"));
        properties.put("secretAccessKey", config.get("provider", "secretAccessKey"));
        properties.put("region", config.get("provider", "region"));
        String sessionToken = String.valueOf(config.get("provider", "sessionToken"));
        if(sessionToken != null && !sessionToken.isEmpty())
            properties.put("sessionToken", config.get("provider", "sessionToken"));*/

        InstanceProviderConfig provider = InstanceProviderConfig.builder()
                .type(config.get("provider", "type"))
                .config(properties)
                .build();


        Environment environment = Environment.builder()
                .name(environmentName)
                .credentials(credentials)
                .provider(provider)
                .build();

        EnvironmentsApi api = new EnvironmentsApi(client);

        try {
            api.create(environment);

        } catch (ApiException e) {
            if (e.getCode() == 302 /* found */) {
                logger.info("Warning: an environment with the same name already exists");
            } else {
                throw e;
            }
        }

        return environment.getName();
    }

    /**
     * Create a new deployment (Cloudera Manager) with data from the configuration file.
     */
    private String createDeployment(ApiClient client, String environmentName, Ini config, String deploymentName)
            throws ApiException {
        String clusterName = config.get("cluster", "name");

        Map<String, String> cmConfigs = new HashMap<String, String>();
        cmConfigs.put("enable_api_debug", "true");

        Map<String, Map<String, String>> overrides = new HashMap<String, Map<String, String>>();
        overrides.put("CLOUDERA_MANAGER", cmConfigs);

        DeploymentTemplate template = DeploymentTemplate.builder()
                .name(deploymentName)
                .managerVirtualInstance(
                        createVirtualInstanceWithRandomId(config, "manager"))
                .port(7180)
                .enableEnterpriseTrial(true)
                .configs(overrides)
                .build();

        DeploymentsApi api = new DeploymentsApi(client);
        try {
            api.create(environmentName, template);

        } catch (ApiException e) {
            if (e.getCode() == 302 /* found */) {
                logger.info("Warning: a deployment with the same name already exists");
            } else {
                throw e;
            }
        }

        return template.getName();
    }

    /**
     * Create a new CDH cluster with data from the configuration file.
     */
    private String createCluster(ApiClient client, String environmentName,
                                 String deploymentName, Ini config) throws ApiException,
            IOException {

        String clusterName = this.clusterName;
        int clusterSize = Integer.parseInt(config.get("worker", "size"));

        // Create the master group

        Map<String, List<String>> masterRoles =  getHashMapfromSections("masterRoles");

        Map<String, VirtualInstanceGroup> groups = new HashMap<String, VirtualInstanceGroup>();
        groups.put("masters", VirtualInstanceGroup.builder()
                .name("masters")
                .minCount(1)
                .serviceTypeToRoleTypes(masterRoles)
                .virtualInstances(Arrays.asList(createVirtualInstanceWithRandomId(config, "master")))
                .build());

        // Create the workers group

        Map<String, List<String>> workerRoles =  getHashMapfromSections("workerRoles");

        List<VirtualInstance> workerVirtualInstances = new ArrayList<VirtualInstance>();
        String spotInstances = String.valueOf(config.get("worker", "useSpotInstances"));
        String templateName = "worker";
        int minCount = clusterSize;
        if(spotInstances.equals("true")) {
            logger.info("Trying for Spot Instances.");
            System.out.print("Trying for Spot Instances.");
            templateName = "workerwithspotinstances";
            minCount = 0;
        }

        for (int i = 0; i < clusterSize; i++) {
            workerVirtualInstances.add(createVirtualInstanceWithRandomId(config, templateName));
        }

        groups.put("workers", VirtualInstanceGroup.builder()
                .name("workers")
                .minCount(minCount)
                .serviceTypeToRoleTypes(workerRoles)
                .virtualInstances(workerVirtualInstances)
                .build());

        // Create the cluster template

        ClusterTemplate template = ClusterTemplate.builder()
                .name(clusterName)
                .productVersions(newMap("CDH", config.get("cluster", "cdh_version")))
                .services(getWorkerServices())
                .virtualInstanceGroups(groups)
                .build();

        ClustersApi api = new ClustersApi(client);
        try {
            api.create(environmentName, deploymentName, template);

        } catch (ApiException e) {
            if (e.getCode() == 302 /* found */) {
                logger.info("Warning: a cluster with the same name already exists");
            } else {
                throw e;
            }
        }

        return template.getName();
    }

    public Map<String,List<String>> getHashMapfromSections(String sectionName) throws IOException{

        Ini ini = new Ini(new FileReader("cluster.ini"));
        Section section = ini.get(sectionName);
        Map<String,List<String>> hMap = new HashMap<String, List<String>>();
        for (String optionKey : section.keySet()) {
            List<String> arrayList = Arrays.asList(section.get(optionKey).split("\\s*,\\s*"));
            hMap.put(optionKey,arrayList);
        }
        return hMap;
    }

    public ArrayList<String> getWorkerServices() throws IOException {
        Ini ini = new Ini(new FileReader("cluster.ini"));
        Section section = ini.get("workerRoles");
        ArrayList<String> arrayList = new ArrayList<String>();
        for (String optionKey : section.keySet()) {
            arrayList.add(optionKey);
        }
        return arrayList;
    }


    /**
     * Go through the steps for creating a cluster based on the configuration file.
     */
    public int run() throws Exception {
        ApiClient client = newAuthenticatedApiClient(this);
        loadClusterConfigs(client);

        try {
            logger.info("Creating a new environment...");
            String environmentName = createEnvironment(client, config, this.environmentName);

            logger.info("Creating a new Cloudera Manager instance...");
            String deploymentName = createDeployment(client, environmentName, config, this.deploymentName);

            logger.info("Waiting for deployment to be ready. Check the web interface for details.");
            waitForDeployment(client, environmentName, deploymentName);

            logger.info("Creating a new CDH cluster...");
            String clusterName = createCluster(client, environmentName, deploymentName, config);

            logger.info("Waiting for the cluster to be ready. Check the web interface for details.");
            waitForCluster(client, environmentName, deploymentName, clusterName);
        }
        catch(Exception e) {
            logger.error("ERROR: ", e);
        }

        return 0;
    }

    /**
     * Wait for deployment bootstrap process to complete.
     */
    private void waitForDeployment(ApiClient client, String environmentName,
                                   String deploymentName) throws InterruptedException, ApiException {

        DeploymentsApi api = new DeploymentsApi(client);
        String stage = null;
        do {
            waitAndReportProgress();
            stage = api.getStatus(environmentName, deploymentName).getStage();

        } while (!readyOrFailed(stage));

        System.out.printf("%nDeployment '%s' current stage is '%s'%n", deploymentName, stage);

    }

    /* private String readFile(String path) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(path), "UTF-8");
        try {
            return scanner.useDelimiter("\\Z").next();

        } finally {
            scanner.close();
        }
    }

    private <T> Map<T, T> newMap(T... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("This function expects an even number of arguments");
        }
        Map<T, T> result = new HashMap<T, T>();
        for (int i = 0; i < pairs.length; i += 2) {
            result.put(pairs[i], pairs[i + 1]);
        }
        return result;
    } */



    public static void main(String[] args) throws Exception {
        BuildCluster cluster = new BuildCluster();

        JCommander jc = new JCommander(cluster);
        jc.setProgramName("BuildCluster");

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
