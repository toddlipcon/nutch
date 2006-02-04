/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.clustering;

import org.apache.nutch.plugin.*;
import org.apache.hadoop.conf.Configuration;
import java.util.logging.Logger;
import org.apache.hadoop.util.LogFormatter;

/**
 * A factory for retrieving {@link OnlineClusterer} extensions.
 *
 * @author Dawid Weiss
 * @version $Id: OnlineClustererFactory.java,v 1.2 2005/02/07 19:09:26 cutting Exp $
 */
public class OnlineClustererFactory {
  public static final Logger LOG = LogFormatter
    .getLogger(OnlineClustererFactory.class.getName());
  private ExtensionPoint extensionPoint;
  private String extensionName;

  public OnlineClustererFactory(Configuration conf) {
      this.extensionPoint = PluginRepository.get(conf).getExtensionPoint(OnlineClusterer.X_POINT_ID);
      this.extensionName = conf.get("extension.clustering.extension-name");
  }

  /**
  * @return Returns the online clustering extension specified
  * in nutch configuration's key
  * <code>extension.clustering.extension-name</code>. If the name is
  * empty (no preference), the first available clustering extension is
  * returned.
  */
  public OnlineClusterer getOnlineClusterer()
    throws PluginRuntimeException {

    if (this.extensionPoint == null) {
      // not even an extension point defined.
      return null;
    }
    
    if (extensionName != null) {
      Extension extension = findExtension(extensionName);
      if (extension != null) {
        LOG.info("Using clustering extension: " + extensionName);
        return (OnlineClusterer) extension.getExtensionInstance();
      }
      LOG.warning("Clustering extension not found: '" + extensionName 
        + "', trying the default");
      // not found, fallback to the default, if available.
    }

    Extension[] extensions = this.extensionPoint.getExtensions();
    if (extensions.length > 0) {
      LOG.info("Using the first clustering extension found: "
        + extensions[0].getId());
      return (OnlineClusterer) extensions[0].getExtensionInstance();
    } else {
      return null;
    }
  }

  private Extension findExtension(String name)
    throws PluginRuntimeException {

    Extension[] extensions = this.extensionPoint.getExtensions();

    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];

      if (name.equals(extension.getId()))
        return extension;
    }
    return null;
  }

} 
