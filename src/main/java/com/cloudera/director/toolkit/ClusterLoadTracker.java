package com.cloudera.director.toolkit;

/**
 * Created by chirub on 2/12/16.
 */
public class ClusterLoadTracker {

    private  int clusterGrow;
    private int originalGroupSize;
    private  int clusterShrink;
    private  int currenGroupSize;


    public int getOriginalGroupSize() {
        return originalGroupSize;
    }

    public void setOriginalGroupSize(int originalGroupSize) {
        this.originalGroupSize = originalGroupSize;
    }
    public int getCurrenGroupSize() {
        return currenGroupSize;
    }

    public void setCurrenGroupSize(int currenGroupSize) {
        this.currenGroupSize = currenGroupSize;
    }

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
