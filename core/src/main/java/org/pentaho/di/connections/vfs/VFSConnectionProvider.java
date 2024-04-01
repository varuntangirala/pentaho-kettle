/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2023 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.connections.vfs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.pentaho.di.connections.ConnectionProvider;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;

import java.util.List;

/**
 * Created by bmorrise on 2/3/19.
 */
public interface VFSConnectionProvider<T extends VFSConnectionDetails> extends ConnectionProvider<T> {
  FileSystemOptions getOpts( T vfsConnectionDetails );
  List<VFSRoot> getLocations( T vfsConnectionDetails );
  String getProtocol( T vfsConnectionDetails );
  String sanitizeName( String string );

  /**
   * If the provided ConnectionDetails are the correct type, return a FileObject for the provided path. This method and
   * the returning FileObject must not depend on a ConnectionManager or MetaStore.
   *
   * @param connectionDetails for the connection
   * @param path path relative to the connection
   *
   * @return FileSystem or null if the provided connection details are not the matching type
   */
  default FileObject getDirectFile( T connectionDetails, String path ) throws KettleFileException {
    String pvfsUrl = connectionDetails.getType() + "://" + path;
    // use an empty Variables to prevent other "connection" values from causing StackOverflowErrors
    return KettleVFS.getFileObject( pvfsUrl, new Variables(), getOpts( connectionDetails ) );
  }

  default boolean usesBuckets( T connectionDetails ) {
    // TODO: At this moment, HCP is configured to have buckets, however it returns no locations/buckets.
    // This is incorrect, and should be fixed.
    // Currently, HCP is also the only hasBuckets connection which has a domain set.
    // PDI's code for `VFSFileProvider#getFiles()` tests for `file.getPath().matches( DOMAIN_ROOT )`
    // on the `connectionFolder`. This test excludes any connection with a domain, given that those with a domain
    // have an internal path formed by the protocol followed by the domain.
    // To accommodate for this case, adding the `StringUtils.isEmpty( connectionDetails.getDomain() )` condition,
    // but this should be removed once the HCP configuration is fixed.

    return connectionDetails.hasBuckets() &&
      StringUtils.isEmpty( connectionDetails.getDomain() ) &&
      getResolvedRootLocation( connectionDetails ) == null;
  }
}
