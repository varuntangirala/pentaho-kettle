/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2024 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.connections.vfs.builder;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;

public class RootLocationConfigurationBuilder extends VFSConnectionConfigurationBuilder {

  private static final String ROOT_LOCATION = "rootLocation";

  public RootLocationConfigurationBuilder( FileSystemOptions fileSystemOptions ) {
    super( fileSystemOptions );
  }
  public String getRootLocation() {
    return this.getParam( getFileSystemOptions(), ROOT_LOCATION);
  }

  public void setRootLocation( String rootLocation ) {
    this.setParam( getFileSystemOptions(), ROOT_LOCATION, rootLocation );
  }

  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return DefaultLocationFileSystem.class;
  }

  /**
   * Dummy class that implements FileSystem.
   */
  abstract static class DefaultLocationFileSystem implements FileSystem {
  }
}
