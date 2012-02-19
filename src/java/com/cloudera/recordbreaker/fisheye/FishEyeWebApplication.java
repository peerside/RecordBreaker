package com.cloudera.recordbreaker.fisheye;

import org.apache.wicket.Page;  
import org.apache.wicket.protocol.http.WebApplication;
import org.apache.wicket.settings.IResourceSettings;
import org.apache.wicket.util.resource.locator.ResourceStreamLocator;
import org.apache.wicket.util.resource.IResourceStream;


/**
 * The <code>FishEyeWebApplication</code> class sets up some config information
 * for the Web app.  It doesn't do a ton of interesting things.
 *
 */
public class FishEyeWebApplication extends WebApplication {
  public FishEyeWebApplication() {
  }

  class PathStripperLocator extends ResourceStreamLocator {
    public PathStripperLocator() {
    }
    public IResourceStream locate(final Class clazz, final String path) {
      IResourceStream located = super.locate(clazz, trimFolders(path));
      if (located != null) {
        return located;
      }
      return super.locate(clazz, path);
    }
    private String trimFolders(String path) {
      return path.substring(path.lastIndexOf("/") + 1);
    }
  }
  
  public void init() {
    super.init();
    IResourceSettings resourceSettings = getResourceSettings();
    String htmlDir = this.getClass().getClassLoader().getResource("Overview.html").toExternalForm();
    resourceSettings.addResourceFolder(htmlDir);
    resourceSettings.setResourceStreamLocator(new PathStripperLocator());

    mountPage("/Overview", Overview.class);
    mountPage("/About", AboutPage.class);

    mountPage("/Files", FilesPage.class);
    mountPage("/Filetypes", FiletypesPage.class);
    mountPage("/Schemas", SchemasPage.class);    

    mountPage("/File", FilePage.class);    
    mountPage("/Filetype", FiletypePage.class);    
    mountPage("/Schema", SchemaPage.class);

    mountPage("/Settings", SettingsPage.class);        
  }
  
  @Override
  public Class<? extends Page> getHomePage() {
    return Overview.class;
  }
}