package com.cloudera.director.toolkit;

/**
 * Created by chirub on 2/12/16.
 */
public class ClusterLoadTracker {

    private  int clusterGrow;

    private int originalSize;

    public int getOriginalSize() {
        return originalSize;
    }

    public void setOriginalSize(int originalSize) {
        this.originalSize = originalSize;
    }

    public int getCurrentSize() {
        return currentSize;
    }

    public void setCurrentSize(int currentSize) {
        this.currentSize = currentSize;
    }

    private  int currentSize;

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
