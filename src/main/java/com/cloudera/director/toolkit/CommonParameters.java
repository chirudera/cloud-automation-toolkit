/*
 * Copyright (c) 2015 Cloudera, Inc. All rights reserved.
 */

package com.cloudera.director.toolkit;

import com.beust.jcommander.Parameter;
import com.cloudera.director.client.common.ApiClient;
import com.cloudera.director.client.common.ApiException;
import com.cloudera.director.client.latest.api.AuthenticationApi;
import com.cloudera.director.client.latest.api.ClustersApi;
import com.cloudera.director.client.latest.api.EnvironmentsApi;
import com.cloudera.director.client.latest.model.*;
import org.ini4j.Ini;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * A common list of parameters for all commands that need to talk
 * with a Cloudera Director Server over the public API
 */
public class CommonParameters {

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

        return client;
    }

    protected void loadClusterConfigs(ApiClient client) throws Exception {
        try {
            this.config = new Ini(new File(this.configFile));

        } catch (FileNotFoundException e) {
            System.err.println("Configuration file not found: " + this.configFile);
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

        configs.put("subnetId", config.get("instance", "subnetId"));
        configs.put("securityGroupsIds", config.get("instance", "securityGroupId"));
        configs.put("instanceNamePrefix", config.get("instance", "namePrefix"));

        instanceImage = config.get("instance", "image");
        instanceType = config.get("instance", "type");

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

        System.out.printf("%nCluster '%s' current stage is '%s'%n", clusterName, stage);
    }

    protected boolean readyOrFailed(String stage) {
        return Status.Stage.READY.equals(stage) || Status.Stage.BOOTSTRAP_FAILED.equals(stage);
    }

    protected void waitAndReportProgress() throws InterruptedException {
        System.out.print(".");
        System.out.flush();

        TimeUnit.SECONDS.sleep(1);
    }
}
