/*
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.recordbreaker.fisheye;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/*************************************************************
 * The <code>AccessController</code> object manages the user login
 * and tests access rights.
 *
 * The right thing to do (eventually) is to hook it up to Hadoop's
 * user login system.  But right now it serves as an owning class
 * placeholder for anything to do with Fisheye access control.
 *
 * @author "Michael Cafarella" <mjc>
 * @version 1.0
 * @since 1.0
 *************************************************************/
public class AccessController {
  String currentUser;

  public AccessController() {
  }

  //////////////////////////////////////////////////////
  // Log in/out of Fisheye.
  // (REMIND -- mjc -- Eventually this will contact Hadoop's user database)
  //////////////////////////////////////////////////////
  /**
   * <code>login</code> tests the user's credentials, and changes
   * the current user if appropriate.
   *
   * @param username a <code>String</code> value
   * @param password a <code>String</code> value
   * @return a <code>boolean</code> value
   */
  public boolean login(String username, String password) {
    // For now, the password is always the same as the username
    if (username.equals(password)) {
      this.currentUser = username;
      return true;
    } else {
      return false;
    }
  }

  /**
   * <code>logout</code> resets the current user.  Calling
   * this method will always succeed.
   */
  public void logout() {
    this.currentUser = null;
  }

  //////////////////////////////////////////////////////
  // Test access privileges.
  // (REMIND -- mjc -- eventually this will need to contact
  // a back-end user database in order to support groups.
  //////////////////////////////////////////////////////
  public boolean hasReadAccess(String fileOwner, String fileGroup, String filePermissions) {
    FsPermission fsp = new FsPermission(filePermissions);

    // Check world-readable
    FsAction otherAction = fsp.getOtherAction();
    if (otherAction == FsAction.ALL ||
        otherAction == FsAction.READ ||
        otherAction == FsAction.READ_EXECUTE ||
        otherAction == FsAction.READ_WRITE) {
      return true;
    }

    // Check group-readable
    // REMIND -- mjc -- implement group-readable testing when we have the user database
    // that will tell us the current logged-in-user's groups.

    // Check owner-readable
    if (currentUser != null && currentUser.equals(fileOwner)) {
      FsAction userAction = fsp.getUserAction();    
      if (userAction == FsAction.ALL ||
          userAction == FsAction.READ ||
          userAction == FsAction.READ_EXECUTE ||
          userAction == FsAction.READ_WRITE) {
        return true;
      }
    }

    return false;
  }
  
  /**
   * <code>getCurrentUser</code> returns the currently-logged-in
   * user.  If no one is logged in, this returns null.
   */
  public String getCurrentUser() {
    return this.currentUser;
  }
}