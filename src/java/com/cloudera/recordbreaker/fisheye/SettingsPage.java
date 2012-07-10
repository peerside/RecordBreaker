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

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.markup.html.form.AjaxButton;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextArea;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.form.RequiredTextField;
import org.apache.wicket.markup.html.form.PasswordTextField;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.StringResourceModel;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.util.value.ValueMap;

import com.cloudera.recordbreaker.analyzer.CrawlSummary;

import java.io.IOException;
import java.util.List;

/************************************************
 * Wicket Page class that allows user to edit Settings
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see WebPage
 *************************************************/
public class SettingsPage extends WebPage {
  public final class LoginForm extends Form<ValueMap> {
    public LoginForm(final String id, ValueMap vm) {
      super(id, new CompoundPropertyModel<ValueMap>(vm));
      add(new RequiredTextField<String>("loginusername").setType(String.class));
      add(new PasswordTextField("loginpassword").setType(String.class));
      
      add(new AjaxButton("submitbutton") {
          protected void onSubmit(final AjaxRequestTarget target, final Form form) {
            wmc.setVisibilityAllowed(false);            
            target.add(wmc);
          }
          protected void onError(final AjaxRequestTarget target, final Form form) {
            wmc.setVisibilityAllowed(true);            
            target.add(wmc);
          }
        });
    }
    public void onSubmit() {
      FishEye fe = FishEye.getInstance();      
      ValueMap vals = getModelObject();
      if (fe.login((String) vals.get("loginusername"), (String) vals.get("loginpassword"))) {
        vals.put("currentuser", (String) vals.get("loginusername"));

        wmc.setVisibilityAllowed(false);
        setResponsePage(new SettingsPage());
      } else {
        wmc.setVisibilityAllowed(true);        
      }
      vals.put("loginpassword", "");
    }
    public void onError() {
      ValueMap vals = getModelObject();
      wmc.setVisibilityAllowed(true);
      vals.put("loginpassword", "");      
    }
    public void onConfigure() {
      setVisibilityAllowed(FishEye.getInstance().getUsername() == null);
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
      setResponsePage(new SettingsPage());      
    }
    public void onConfigure() {
      setVisibilityAllowed(FishEye.getInstance().getUsername() != null);
    }
  }

  final WebMarkupContainer wmc = new WebMarkupContainer("errorMsgContainer");
  public SettingsPage() {
    FishEye fe = FishEye.getInstance();    
    final String username = fe.getUsername();
    final ValueMap logins = new ValueMap();    
    logins.put("currentuser", username);
    this.setOutputMarkupPlaceholderTag(true);        

    // Login/logout
    add(new LoginForm("loginform", logins));
    add(new LogoutForm("logoutform", logins));
    final Label errorLabel = new Label("loginErrorMessage", "Your username and password did not match.");
    wmc.add(errorLabel);
    wmc.setOutputMarkupPlaceholderTag(true);
    add(wmc);
    wmc.setVisibilityAllowed(false);

    //
    // Display filesystem add box, if there is no current FS.
    //
    WebMarkupContainer fsAddContainer = new WebMarkupContainer("fsAddContainer");

    Form<?> hdfsAddForm = new Form<ValueMap>("hdfsaddform", new CompoundPropertyModel<ValueMap>(new ValueMap())) {
      protected void onSubmit() {
        System.err.println("Hooh! hdfs dir is: " + (String) getModelObject().get("hdfsDir"));
      }
    };
    hdfsAddForm.add(new RequiredTextField<String>("hdfsDir").setType(String.class));
    fsAddContainer.add(hdfsAddForm);

    Form<?> localFsAddForm = new Form<ValueMap>("localfsaddform", new CompoundPropertyModel<ValueMap>(new ValueMap())) {
      protected void onSubmit() {
        System.err.println("Hooh! local fs dir is: " + (String) getModelObject().get("localfsDir"));
      }
    };
    localFsAddForm.add(new RequiredTextField<String>("localfsDir").setType(String.class));
    fsAddContainer.add(localFsAddForm);
    
    fsAddContainer.setOutputMarkupPlaceholderTag(true);
    add(fsAddContainer);
    fsAddContainer.setVisibilityAllowed(fe.getFSUrl() == null);
    
    //
    // Display filesystem info, if there is any.
    //
    List<CrawlSummary> crawlList = fe.getAnalyzer().getCrawlSummaries();    
    WebMarkupContainer fsDisplayContainer = new WebMarkupContainer("fsDisplayContainer");
    fsDisplayContainer.add(new Label("fsName", "" + fe.getFSUrl()));
    fsDisplayContainer.add(new Label("numCrawls", "" + crawlList.size()));
    fsDisplayContainer.setOutputMarkupPlaceholderTag(true);    
    add(fsDisplayContainer);
    fsDisplayContainer.setVisibilityAllowed(fe.getFSUrl() != null);
    
    //
    // If the filesystem is there, we need to have info about its crawls
    //
    WebMarkupContainer crawlContainer = new WebMarkupContainer("crawlContainer");
    ListView<CrawlSummary> crawlListView = new ListView<CrawlSummary>("crawlListView", crawlList) {
      protected void populateItem(ListItem<CrawlSummary> item) {
        CrawlSummary cs = item.getModelObject();
        // Fields are: 'crawlid' and 'crawllastexamined'
        item.add(new Label("crawlid", "" + cs.getCrawlId()));
        item.add(new Label("crawllastexamined", cs.getLastExamined()));
      }
    };
    crawlContainer.add(crawlListView);
    fsDisplayContainer.add(crawlContainer);
    crawlContainer.setVisibilityAllowed(crawlList.size() > 0);

    //
    // Standard environment variables
    //
    add(new Label("fisheyeStarttime", fe.getStartTime().toString()));
    add(new Label("fisheyePort", "" + fe.getPort()));
    try {
      add(new Label("fisheyeDir", "" + fe.getFisheyeDir().getCanonicalPath()));
    } catch (IOException iex) {
      add(new Label("fisheyeDir", "unknown"));
    }
  }
}
