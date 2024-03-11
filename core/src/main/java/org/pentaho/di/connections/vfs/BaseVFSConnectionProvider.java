/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2024 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.utils.ConnectionTestOptions;
import org.pentaho.di.connections.vfs.builder.RootLocationConfigurationBuilder;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;

import org.apache.commons.vfs2.FileObject;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class BaseVFSConnectionProvider<T extends VFSConnectionDetails> implements VFSConnectionProvider<T> {

  private final Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;
  private static final Log LOGGER = LogFactory.getLog( BaseVFSConnectionProvider.class );

  @Override public List<String> getNames() {
    return connectionManagerSupplier.get().getNamesByType( getClass() );
  }

  @SuppressWarnings( "unchecked" )
  @Override public List<T> getConnectionDetails() {
    return (List<T>) connectionManagerSupplier.get().getConnectionDetailsByScheme( getKey() );
  }

  @Override public T prepare( T connectionDetails ) throws KettleException {
    return connectionDetails;
  }

  // Check exists... pvfs://connection name ->  exists ?
  // 1. External path / Generic file path: pvfs://connection name/foo // ConnectionFileProvider  END USER
  // 2. Internal path / Apache VFS path / Connection path: s3-avfs://foo + FSOptions (Connection Name)
  // 3. Physical path: S3 API actually knows of: s3://root/path/here/foo  ADMIN USER will config the root physical path

  // Local
  // 2. Internal path: local://foo + FSOptions (Connection Name, Root Path...)
  // 3. Physical path: c:/root/path/here/foo
  // KettleVFS.getFileObject() -->> FileObject -->> FileName -->> FileSystem
  @Override
  public boolean test( T connectionDetails, ConnectionTestOptions connectionTestOptions ) throws KettleException {
    boolean valid = test( connectionDetails );
    if ( !valid ) {
      return false;
    }

    if ( !connectionDetails.isSupportsRootLocation()  || connectionTestOptions.isIgnoreRootLocation() ) {
      return true;
    }

    String resolvedRootLocation = getResolvedRootLocation( connectionDetails );
    if ( resolvedRootLocation == null ) {
      return !connectionDetails.isRootLocationRequired();
    }

    FileObject fileObject = getDirectFile( connectionDetails, connectionDetails.getDomain() );
    try {
      return fileObject.exists();
    } catch ( FileSystemException fileSystemException ) {
      LOGGER.error(fileSystemException.getMessage() );
      return false;
    }
  }

  public String getResolvedRootLocation( T connectionDetails ) {
    if ( StringUtils.isNotEmpty( connectionDetails.getRootLocation() ) ) {
      VariableSpace space = getSpace( connectionDetails );
      String resolvedRootLocation = getVar( connectionDetails.getRootLocation(), space );
      if ( StringUtils.isNotBlank( resolvedRootLocation ) ) {
        return resolvedRootLocation;
      }
    }

    return null;
  }

  @Override public String sanitizeName( String string ) {
    return string;
  }

  // Utility method to perform variable substitution on values
  protected String getVar( String value, VariableSpace variableSpace ) {
    if ( variableSpace != null ) {
      return variableSpace.environmentSubstitute( value );
    }
    return value;
  }

  // Utility method to derive a boolean checkbox setting that may use variables instead
  protected static boolean getBooleanValueOfVariable( VariableSpace space, String variableName, String defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = space.environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        Boolean b = ValueMetaBase.convertStringToBoolean( value );
        return b != null && b;
      }
    }
    return Objects.equals( Boolean.TRUE, ValueMetaBase.convertStringToBoolean( defaultValue ) );
  }

  protected VariableSpace getSpace( ConnectionDetails connectionDetails ) {
    return connectionDetails.getSpace() == null ? Variables.getADefaultVariableSpace() : connectionDetails.getSpace();
  }

  @Override
  public FileSystemOptions getOpts( T connectionDetails ) {
    FileSystemOptions opts = new FileSystemOptions();

    if ( connectionDetails.isSupportsRootLocation() ) {
      String resolvedRootLocation = getResolvedRootLocation( connectionDetails );
      if ( resolvedRootLocation != null ) {
        new RootLocationConfigurationBuilder( opts ).setRootLocation( resolvedRootLocation );
      }
    }

    return opts;
  }
}
