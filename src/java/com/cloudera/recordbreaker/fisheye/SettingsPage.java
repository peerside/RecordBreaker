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

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.util.value.ValueMap;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.markup.html.form.TextArea;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.form.RequiredTextField;
import org.apache.wicket.markup.html.form.PasswordTextField;



import java.io.IOException;

/**
 * Wicket Page class that allows user to edit Settings
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see WebPage
 */
public class SettingsPage extends WebPage {
  public final class LoginForm extends Form<ValueMap> {
    public LoginForm(final String id, ValueMap vm) {
      super(id, new CompoundPropertyModel<ValueMap>(vm));
      add(new RequiredTextField<String>("loginusername").setType(String.class));
      add(new PasswordTextField("loginpassword").setType(String.class));      
    }
    public void onSubmit() {
      FishEye fe = FishEye.getInstance();      
      ValueMap vals = getModelObject();
      if (fe.login((String) vals.get("loginusername"), (String) vals.get("loginpassword"))) {
        vals.put("currentuser", (String) vals.get("loginusername"));
      }
      vals.put("loginpassword", "");
    }
    public boolean isVisible() {
      FishEye fe = FishEye.getInstance();            
      return fe.getUsername() == null;
    }
  }

  public final class LogoutForm extends Form<ValueMap> {
    public LogoutForm(final String id, ValueMap vm) {
      super(id, new CompoundPropertyModel<ValueMap>(vm));
      add(new Label("currentuser"));
    }
    public void onSubmit() {
      FishEye fe = FishEye.getInstance();      
      fe.logout();
    }
    public boolean isVisible() {
      FishEye fe = FishEye.getInstance();            
      return fe.getUsername() != null;
    }
  }
  
  public SettingsPage() {
    FishEye fe = FishEye.getInstance();
    final String username = fe.getUsername();
    ValueMap logins = new ValueMap();
    logins.put("currentuser", username);
  
    add(new LoginForm("loginform", logins));
    add(new LogoutForm("logoutform", logins));

    add(new Label("fisheyeStarttime", fe.getStartTime().toString()));
    add(new Label("fisheyePort", "" + fe.getPort()));
    try {
      add(new Label("fisheyeDir", "" + fe.getFisheyeDir().getCanonicalPath()));
    } catch (IOException iex) {
      add(new Label("fisheyeDir", "unknown"));
    }
  }
}
