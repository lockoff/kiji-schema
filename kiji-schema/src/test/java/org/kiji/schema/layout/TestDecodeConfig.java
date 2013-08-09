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

import org.apache.avro.Schema;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDecodeConfig {

  /** The decode configuration to use in tests. */
  private DecodeConfig mDecodeConfig;

  @Before
  public void setup() {
    mDecodeConfig = DecodeConfig.create();
  }

  @Test
  public void testCreate() {
    // Ensure the decode configuration created in setup hsa the correct default values.
    Assert.assertTrue(mDecodeConfig.isUseTableReaderSchema());
    Assert.assertFalse(mDecodeConfig.isUseReaderSchema());
    Assert.assertFalse(mDecodeConfig.isUseWriterSchema());
    Assert.assertNull(mDecodeConfig.getReaderSchema());

    Assert.assertTrue(mDecodeConfig.isUseAvroSpecificDecoder());
    Assert.assertFalse(mDecodeConfig.isUseAvroGenericDecoder());
  }

  @Test
  public void testUseWriterSchema() {
    mDecodeConfig = mDecodeConfig.useWriterSchema();
    Assert.assertFalse(mDecodeConfig.isUseTableReaderSchema());
    Assert.assertFalse(mDecodeConfig.isUseReaderSchema());
    Assert.assertTrue(mDecodeConfig.isUseWriterSchema());
    Assert.assertNull(mDecodeConfig.getReaderSchema());
  }

  @Test
  public void testUseReaderSchema() {
    Schema schema = Schema.create(Schema.Type.NULL);
    mDecodeConfig = mDecodeConfig.useReaderSchema(schema);
    Assert.assertFalse(mDecodeConfig.isUseTableReaderSchema());
    Assert.assertTrue(mDecodeConfig.isUseReaderSchema());
    Assert.assertFalse(mDecodeConfig.isUseWriterSchema());
    Assert.assertNotNull(mDecodeConfig.getReaderSchema());
    Assert.assertEquals(schema, mDecodeConfig.getReaderSchema());
  }

  @Test(expected = NullPointerException.class)
  public void testUseReaderSchemaInvalid() {
    mDecodeConfig = mDecodeConfig.useReaderSchema(null);
  }

  @Test
  public void testUseAvroGeneric() {
    mDecodeConfig = mDecodeConfig.useAvroGenericDecoder();
    Assert.assertFalse(mDecodeConfig.isUseAvroSpecificDecoder());
    Assert.assertTrue(mDecodeConfig.isUseAvroGenericDecoder());
  }
  @Test
  public void testUseAvroSpecific() {
    mDecodeConfig = mDecodeConfig.useAvroGenericDecoder()
        .useAvroSpecificDecoder();
    Assert.assertTrue(mDecodeConfig.isUseAvroSpecificDecoder());
    Assert.assertFalse(mDecodeConfig.isUseAvroGenericDecoder());
  }

  @Test
  public void testSchemaMultiChange() {
    mDecodeConfig = mDecodeConfig.useWriterSchema()
        .useReaderSchema(Schema.create(Schema.Type.NULL))
        .useTableReaderSchema();

    Assert.assertTrue(mDecodeConfig.isUseTableReaderSchema());
    Assert.assertFalse(mDecodeConfig.isUseReaderSchema());
    Assert.assertFalse(mDecodeConfig.isUseWriterSchema());
    Assert.assertNull(mDecodeConfig.getReaderSchema());
  }

  @Test
  public void testIsSerializable() {
    byte[] serialized = SerializationUtils.serialize(mDecodeConfig);
    DecodeConfig reborn = (DecodeConfig) SerializationUtils.deserialize(serialized);
    Assert.assertEquals(mDecodeConfig, reborn);
  }
}
