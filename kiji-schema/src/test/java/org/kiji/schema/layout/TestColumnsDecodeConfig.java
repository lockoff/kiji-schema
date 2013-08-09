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

import org.apache.commons.lang.SerializationUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiColumnName;

public class TestColumnsDecodeConfig {

  private ColumnsDecodeConfig mColumnsConfig;

  @Before
  public void setup() {
    mColumnsConfig = ColumnsDecodeConfig.create();
  }

  @Test
  public void testCreate() {
    Assert.assertTrue(mColumnsConfig.isEmpty());
  }

  @Test
  public void testAddConfig() {
    mColumnsConfig = mColumnsConfig.withDecodeConfig(new KijiColumnName("info:email"),
        DecodeConfig.create());
    Assert.assertFalse(mColumnsConfig.isEmpty());
    Assert.assertTrue(mColumnsConfig.containsColumn(new KijiColumnName("info:email")));
    Assert.assertEquals(DecodeConfig.create(),
        mColumnsConfig.getDecodeConfig(new KijiColumnName("info:email")));
  }

  @Test
  public void testEquals() {
    ColumnsDecodeConfig other = ColumnsDecodeConfig.create();
    Assert.assertEquals(other, mColumnsConfig);
    other = other.withDecodeConfig(new KijiColumnName("info:email"), DecodeConfig.create());
    Assert.assertFalse(other.equals(mColumnsConfig));
    mColumnsConfig = mColumnsConfig.withDecodeConfig(new KijiColumnName("info:email"),
        DecodeConfig.create());
    Assert.assertEquals(other, mColumnsConfig);
  }

  @Test
  public void testIsSerializable() {
    byte[] serialized = SerializationUtils.serialize(mColumnsConfig);
    ColumnsDecodeConfig reborn = (ColumnsDecodeConfig) SerializationUtils.deserialize(serialized);
    Assert.assertEquals(mColumnsConfig, reborn);
  }
}
