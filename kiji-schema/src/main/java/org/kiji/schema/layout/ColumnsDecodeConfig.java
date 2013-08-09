/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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

package org.kiji.schema.layout;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.kiji.schema.KijiColumnName;

/**
 * An immutable mapping from columns in a Kiji table to options for decoding cells from the
 * column.
 *
 * <p>
 *  Users can obtain a new instance by calling the factory method {@link #create()}. A new
 *  instance with an added decode configuration for a column can then be obtained by calling
 *  {@link #withDecodeConfig(org.kiji.schema.KijiColumnName, DecodeConfig)}.
 * </p>
 *
 */
public final class ColumnsDecodeConfig implements Serializable {
  /** Serialization version. */
  public static final long serialVersionUID = 1L;

  // A serializable mapping from column names to decode configurations.
  private final Map<String, DecodeConfig> mDecodeConfigs;

  /**
   * Constructs a new instance using the provided mapping of decode configurations.
   *
   * @param decodeConfigs the decode configurations for the new instance.
   */
  private ColumnsDecodeConfig(Map<String, DecodeConfig> decodeConfigs) {
    mDecodeConfigs = decodeConfigs;
  }

  /**
   * Creates a new instance containing no decode configurations.
   *
   * @return a fresh instance containing no decode configurations.
   */
  public static ColumnsDecodeConfig create() {
    return new ColumnsDecodeConfig(new HashMap<String, DecodeConfig>());
  }

  /**
   * Gets a new instance with an added decode configuration for a column.
   *
   * @param column to associate with the decode configuration.
   * @param config for decoding cells from a column.
   * @return a new instance with the added configuration.
   */
  public ColumnsDecodeConfig withDecodeConfig(KijiColumnName column, DecodeConfig config) {
    Map<String, DecodeConfig> newConfig = new HashMap<String, DecodeConfig>(mDecodeConfigs);
    newConfig.put(column.toString(), config);
    return new ColumnsDecodeConfig(newConfig);
  }

  /**
   * @return the set of columns with decode configurations.
   */
  public Set<KijiColumnName> getColumns() {
    Set<String> columnKeys = mDecodeConfigs.keySet();
    Set<KijiColumnName> columns = new HashSet<KijiColumnName>();
    for (String columnKey : columnKeys) {
      columns.add(new KijiColumnName(columnKey));
    }
    return columns;
  }

  /**
   * Determines if a column has a decode configuration.
   *
   * @param column to check for.
   * @return <code>true</code> if the specified column has a decode configuration,
   *    <code>false</code> otherwise.
   */
  public boolean containsColumn(KijiColumnName column) {
    return mDecodeConfigs.containsKey(column.toString());
  }

  /**
   * Gets the decode configuration for a particular column.
   *
   * @param column whose decode configuration should be retrieved.
   * @return the decode configuration for the column, or <code>null</code> if none exists.
   */
  public DecodeConfig getDecodeConfig(KijiColumnName column) {
    return mDecodeConfigs.get(column.toString());
  }

  /**
   * @return determines if any decode configurations exist in this instance.
   */
  public boolean isEmpty() {
    return mDecodeConfigs.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (other instanceof ColumnsDecodeConfig) {
      ColumnsDecodeConfig that = (ColumnsDecodeConfig) other;
      return mDecodeConfigs.equals(that.mDecodeConfigs);
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mDecodeConfigs.hashCode();
  }
}
