/*
 * Copyright (c) 2015 Cloudera, Inc. All rights reserved.
 */

package com.cloudera.director.toolkit;

public interface Command {

  /**
   * Implement this command for your logic
   *
   * @param commonParameters common parameters
   */
  void run(CommonParameters commonParameters) throws Exception;

}
