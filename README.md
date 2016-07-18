# cloud-automation-toolkit
Cloud Automation Scripts

Note: The use of these scripts is unsupported and if support is required, please contact Cloudera.

Cloud Automation Toolkit has scripts for Auto Scaling and On-Demand Scaling of Cloudera clusters on Cloud. The progress can be tracked on the Cloudera Director UI.

About-

This repository contains the source code of the Cloud Automation Toolkit in Java that interacts with the Director Server API. This can be used to grow and shrink CDH clusters managed by Cloudera Manager on a cloud infrastructure.

This folder contains a a set of classes for auto scaling or on-demand scaling of CDH clusters.

You can run them from an IDE or via Maven like this:

    mvn compile exec:java -Dexec.mainClass="com.cloudera.director.toolkit.GrowOrShrinkCluster" \
        -Dexec.args="--admin-username admin --admin-password --server \"http://localhost:7189\" --config cluster.ini"

    OR

    mvn compile exec:java -Dexec.mainClass="com.cloudera.director.samples.AutoScaleClusterApp" \
        -Dexec.args="--admin-username admin --admin-password --server \"http://localhost:7189\" --config cluster.ini"
        
    OR 
    
    mvn compile exec:java -Dexec.mainClass="com.cloudera.director.samples.DynamicScaleClusterApp" \
        -Dexec.args="--admin-username admin --admin-password --server \"http://localhost:7189\" --config cluster.ini"
