package com.cloudera.director.toolkit;

/**
 * Created by chirub on 2/12/16.
 */
public class ClusterLoadTracker {

    private  int clusterGrow;

    private int originalWorkerSize;

    private int originalGatewaySize;

    public int getOriginalWorkerSize() {
        return originalWorkerSize;
    }

    public void setOriginalWorkerSize(int originalWorkerSize) {
        this.originalWorkerSize = originalWorkerSize;
    }



    public int getCurrentWorkersSize() {
        return currentWorkersSize;
    }

    public void setCurrentWorkersSize(int currentWorkersSize) {
        this.currentWorkersSize = currentWorkersSize;
    }

    private  int currentWorkersSize;

    public int getCurrentGatewaySize() {
        return currentGatewaySize;
    }

    public void setCurrentGatewaySize(int currentGatewaySize) {
        this.currentGatewaySize = currentGatewaySize;
    }

    public int getOriginalGatewaySize() {
        return originalGatewaySize;
    }

    public void setOriginalGatewaySize(int originalGatewaySize) {
        this.originalGatewaySize = originalGatewaySize;
    }

    private  int currentGatewaySize;

    public int getClusterShrink() {
        return clusterShrink;
    }

    public void setClusterShrink(int clusterShrink) {
        this.clusterShrink = clusterShrink;
    }

    public int getClusterGrow() {
        return clusterGrow;
    }

    public void setClusterGrow(int clusterGrow) {
        this.clusterGrow = clusterGrow;
    }

    private  int clusterShrink;

    /* A private Constructor prevents any other
     * class from instantiating.
     */
    private ClusterLoadTracker(){ }

    private static ClusterLoadTracker singleton;

    /* Static 'instance' method */
    public static ClusterLoadTracker getInstance( ) {
        if(singleton == null)
          singleton = new ClusterLoadTracker();
        return singleton;
    }
    /* Other methods protected by singleton-ness */

}
