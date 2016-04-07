/*
 * Copyright (c) 2015 Cloudera, Inc. All rights reserved.
 */

package com.cloudera.director.toolkit;

import com.beust.jcommander.Parameter;
import com.cloudera.director.client.common.ApiClient;
import com.cloudera.director.client.common.ApiException;
import com.cloudera.director.client.latest.api.AuthenticationApi;
import com.cloudera.director.client.latest.api.ClustersApi;
import com.cloudera.director.client.latest.model.*;
import org.apache.log4j.Logger;
import org.ini4j.Ini;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import java.util.Scanner;

/**
 * A common list of parameters for all commands that need to talk
 * with a Cloudera Director Server over the public API
 */
public class CommonParameters {

    final static Logger logger = Logger.getLogger(CommonParameters.class);

    @Parameter(names = "--admin-username",
            description = "Name of an user with administrative access")
    private String adminUsername = "admin";

    @Parameter(names = "--admin-password",
            description = "Password for the administrative user", password = true)
    private String adminPassword = "admin";

    @Parameter(names = "--server", description = "Cloudera Director server URL")
    private String serverUrl = "http://localhost:7189";

    @Parameter(names = "--config", required = true,
            description = "Path to the cluster configuration file")
    private String configFile;

    protected String environmentName;
    protected String deploymentName;
    protected String clusterName;

    protected Ini config;

    public String getAdminUsername() {
        return adminUsername;
    }

    public String getAdminPassword() {
        return adminPassword;
    }

    public String getServerUrl() { return serverUrl; }


    protected ApiClient newAuthenticatedApiClient(CommonParameters common)
            throws ApiException {
        ApiClient client = new ApiClient(common.getServerUrl());

        Login login = Login.builder()
                .username(common.getAdminUsername())
                .password(common.getAdminPassword())
                .build();

        new AuthenticationApi(client).login(login);
        logger.info("Logged into Cloudera Director.");

        return client;
    }

    protected void loadClusterConfigs(ApiClient client) throws Exception {
        try {
            this.config = new Ini(new File(this.configFile));

        } catch (FileNotFoundException e) {
            logger.error("Configuration file not found: " + this.configFile);
            throw e;
        }


        this.environmentName = config.get("cluster", "environmentName");
        this.deploymentName = config.get("cluster", "deploymentName");
        this.clusterName = config.get("cluster", "name");
    }

    /**
     * Create a new virtual instance object with a random ID and a template from the configuration file.
     */
    protected VirtualInstance createVirtualInstanceWithRandomId(Ini config, String templateName) {
        return VirtualInstance.builder()
                .id(UUID.randomUUID().toString())
                .template(createInstanceTemplate(config, templateName))
                .build();
    }

    /**
     * Create an instance template with data from the configuration file.
     */
    private InstanceTemplate createInstanceTemplate(Ini config, String templateName) {

        String instanceImage = "";
        String instanceType = "";

        Map<String, String> configs = new HashMap<String, String>();

        NodeType currentNode = NodeType.valueOf(templateName.toUpperCase());

        switch(currentNode) {
            case MANAGER:
                configs.put("subnetId", config.get("manager", "subnetId"));
                configs.put("securityGroupsIds", config.get("manager", "securityGroupId"));
                configs.put("instanceNamePrefix", config.get("manager", "namePrefix"));
                instanceImage = config.get("manager", "image");
                instanceType = config.get("manager", "type");
                break;
            case MASTER:
                configs.put("subnetId", config.get("master", "subnetId"));
                configs.put("securityGroupsIds", config.get("master", "securityGroupId"));
                configs.put("instanceNamePrefix", config.get("master", "namePrefix"));
                instanceImage = config.get("master", "image");
                instanceType = config.get("master", "type");
                break;
            case WORKER:
                configs.put("subnetId", config.get("worker", "subnetId"));
                configs.put("securityGroupsIds", config.get("worker", "securityGroupId"));
                configs.put("instanceNamePrefix", config.get("worker", "namePrefix"));
                instanceImage = config.get("worker", "image");
                instanceType = config.get("worker", "type");
                break;
            case WORKERWITHSPOTINSTANCES:
                configs.put("subnetId", config.get("worker", "subnetId"));
                configs.put("securityGroupsIds", config.get("worker", "securityGroupId"));
                configs.put("instanceNamePrefix", config.get("worker", "namePrefix"));
                configs.put("useSpotInstances", config.get("worker", "useSpotInstances"));
                configs.put("spotBidUSDPerHr", config.get("worker", "spotBidUSDPerHr"));
                instanceImage = config.get("worker", "image");
                instanceType = config.get("worker", "type");
                break;
            case GATEWAY:
                configs.put("subnetId", config.get("gateway", "subnetId"));
                configs.put("securityGroupsIds", config.get("gateway", "securityGroupId"));
                configs.put("instanceNamePrefix", config.get("gateway", "namePrefix"));
                instanceImage = config.get("gateway", "image");
                instanceType = config.get("gateway", "type");
                break;

            case GATEWAYWITHSPOTINSTANCES:
                configs.put("subnetId", config.get("gateway", "subnetId"));
                configs.put("securityGroupsIds", config.get("gateway", "securityGroupId"));
                configs.put("instanceNamePrefix", config.get("gateway", "namePrefix"));
                configs.put("useSpotInstances", config.get("gateway", "useSpotInstances"));
                configs.put("spotBidUSDPerHr", config.get("gateway", "spotBidUSDPerHr"));
                instanceImage = config.get("gateway", "image");
                instanceType = config.get("gateway", "type");
                break;
        }

        return InstanceTemplate.builder()
                .name(templateName)
                .image(instanceImage)
                .type(instanceType)
                .config(configs)
                .build();
    }

    /**
     * Wait for cluster bootstrap process to complete.
     */
    protected void waitForCluster(ApiClient client, String environmentName, String deploymentName,
                                String clusterName) throws InterruptedException, ApiException {

        ClustersApi api = new ClustersApi(client);
        String stage = null;
        do {
            waitAndReportProgress();
            stage = api.getStatus(environmentName, deploymentName, clusterName).getStage();

        } while (!readyOrFailed(stage));

        logger.info(clusterName + " Cluster " + stage + " current stage is " + stage + " " + clusterName);
    }

    protected boolean readyOrFailed(String stage) {
        return Status.Stage.READY.equals(stage) || Status.Stage.BOOTSTRAP_FAILED.equals(stage) || Status.Stage
                .UPDATE_FAILED.equals(stage);
    }

    protected void waitAndReportProgress() throws InterruptedException {
        System.out.print(".");
        System.out.flush();

        TimeUnit.SECONDS.sleep(1);
    }

    protected String readFile(String path) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(path), "UTF-8");
        try {
            return scanner.useDelimiter("\\Z").next();

        } finally {
            scanner.close();
        }
    }

    protected <T> Map<T, T> newMap(T... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException("This function expects an even number of arguments");
        }
        Map<T, T> result = new HashMap<T, T>();
        for (int i = 0; i < pairs.length; i += 2) {
            result.put(pairs[i], pairs[i + 1]);
        }
        return result;
    }

    /**
     * Get worker node count of the existing cluster.
     * @param groupName
     */
    protected int getCurrentClusterGroupSize(String groupName) throws Exception {

        ApiClient client = newAuthenticatedApiClient(this);
        loadClusterConfigs(client);

        ClustersApi api = new ClustersApi(client);
        ClusterTemplate template = api.getTemplateRedacted(environmentName, deploymentName, clusterName);

        VirtualInstanceGroup workersGroup = template.getVirtualInstanceGroups().get(groupName);

        List<VirtualInstance> workerVirtualInstances = workersGroup.getVirtualInstances();

        return workerVirtualInstances.size();
    }






    /**
     * Get worker node count of the existing cluster.
     */
    protected int getCurrentClusterCustomTypeSize(String customType) throws Exception {

        ApiClient client = newAuthenticatedApiClient(this);
        loadClusterConfigs(client);

        ClustersApi api = new ClustersApi(client);
        ClusterTemplate template = api.getTemplateRedacted(environmentName, deploymentName, clusterName);

        VirtualInstanceGroup customGroup = template.getVirtualInstanceGroups().get(customType);

        List<VirtualInstance> customVirtualInstances = customGroup.getVirtualInstances();

        return customVirtualInstances.size();
    }
}
