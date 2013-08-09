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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Immutable options controlling how cells from a Kiji table are decoded. Users may configure
 * the schema and decoder to use when decoding cells.
 *
 * <p>
 *   Users should call the factory method {@link #create()} to obtain an instance.
 * </p>
 *
 * <p>
 *   Users may configure the schema to use by calling one of three methods.
 *  <ul>
 *    <li>{@link #useReaderSchema(org.apache.avro.Schema)}</li>
 *    <li>{@link #useWriterSchema()}</li>
 *     <li>{@link #useTableReaderSchema()}</li>
 *  </ul>
 *  Calling one of these methods returns a new instance configured to use the specified schema.
 *  Similarly, users can obtain a new instance configured to use a choice of decoder by calling
 *  {@link #useAvroSpecificDecoder()} or {@link #useAvroGenericDecoder()}.
 * </p>
 *
 * OPEN QUESTIONS FOR REVIEWER (to be removed after review):
 *  * Not sure if the name is quite right.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class DecodeConfig implements Serializable {
  /** Serialization version. */
  public static final long serialVersionUID = 1L;

  /** Where to find the schema to use when decoding. */
  private final SchemaMode mSchemaMode;

  /** The decoder to use when decoding cells in a column. */
  private final Decoder mDecoder;

  /**
   * A reader schema to use when decoding cells in a column. Non-null iff the schema mode is
   * "specified reader schema".
   */
  private final String mReaderSchemaJson;

  /** Choices of decoder to use (either of the Avro GenericData or SpecificData API decoders). */
  private static enum Decoder {
    AVRO_SPECIFIC,
    AVRO_GENERIC
  };

  /**
   * Choices of schema to use when decoding cells.
   */
  private static enum SchemaMode {
    TABLE_READER_SCHEMA,
    SPECIFIED_READER_SCHEMA,
    WRITER_SCHEMA
  };

  /**
   * Constructs a new instance configuring the schema and decoder to use for decoding cells.
   *
   * @param schemaMode determines where the decode schema will be found.
   * @param decoder to use when decoding cells.
   * @param readerSchemaJSON to use if the decode schema is a specified reader schema (specify
   *     <code>null</code> otherwise).
   */
  private DecodeConfig(SchemaMode schemaMode, Decoder decoder, String readerSchemaJSON) {
    Preconditions.checkArgument(
        schemaMode == SchemaMode.SPECIFIED_READER_SCHEMA && null == readerSchemaJSON,
        "While constructing a DecodeConfig a reader schema was not specified when "
        + "using SchemaMode SPECIFIED_READER_SCHEMA.");
    Preconditions.checkArgument(
        schemaMode != SchemaMode.SPECIFIED_READER_SCHEMA && null != readerSchemaJSON,
        "While constructing a DecodeConfig a reader schema was specified when not using "
        + "SchemaMode SPECIFIED_READER_SCHEMA.");
    mSchemaMode = schemaMode;
    mReaderSchemaJson = readerSchemaJSON;
    mDecoder = decoder;
  }

  /**
   * Creates a new instance with the default configuration. The defaults are that the table
   * layout reader schema and SpecificData decoder should be used when decoding cells.
   *
   * @return a new instance using default options.
   */
  public static DecodeConfig create() {
    return new DecodeConfig(SchemaMode.TABLE_READER_SCHEMA, Decoder.AVRO_SPECIFIC, null);
  }

  /**
   * Specifies that a particular reader schema should be used when decoding cells.
   *
   * @param schema to use as reader schema when decoding cells.
   * @return options configured to use the specified reader schema when decoding.
   */
  public DecodeConfig useReaderSchema(Schema schema) {
    Preconditions.checkNotNull(schema, "You must specify a reader schema to use, "
        + "or instead call useWriterSchema() or useTableReaderSchema() to use those schemas.");
    return new DecodeConfig(SchemaMode.SPECIFIED_READER_SCHEMA, mDecoder, schema.toString());
  }

  /**
   * Specifies that the writer schema should be used when decoding cells.
   *
   * @return options configured to use the writer schema when decoding.
   */
  public DecodeConfig useWriterSchema() {
    return new DecodeConfig(SchemaMode.WRITER_SCHEMA, mDecoder, null);
  }

  /**
   * Specifies that the table layout reader schema should be used when decoding cells.
   *
   * @return options configured to use the table layout reader schema when decoding.
   */
  public DecodeConfig useTableReaderSchema() {
    return new DecodeConfig(SchemaMode.TABLE_READER_SCHEMA, mDecoder, null);
  }

  /**
   * Specifies that the Avro SpecificData decoder should be used when decoding cells.
   *
   * @return options configured to use the Avro SpecificData decoder when decoding cells.
   */
  public DecodeConfig useAvroSpecificDecoder() {
    return new DecodeConfig(mSchemaMode, Decoder.AVRO_SPECIFIC, mReaderSchemaJson);
  }

  /**
   * Specifies that the Avro GenericData decoder should be used when decoding cells.
   *
   * @return options configured to use the Avro GenericData decoder when decoding cells.
   */
  public DecodeConfig useAvroGenericDecoder() {
    return new DecodeConfig(mSchemaMode, Decoder.AVRO_GENERIC, mReaderSchemaJson);
  }

  /**
   * @return <code>true</code> if a specified reader schema is configured for use when decoding.
   */
  public boolean isUseReaderSchema() {
    return mSchemaMode == SchemaMode.SPECIFIED_READER_SCHEMA;
  }

  /**
   * @return a specified reader schema to use when decoding, or <code>null</code> if another
   *     schema should be used.
   */
  public Schema getReaderSchema() {
    return null == mReaderSchemaJson ? null : (new Schema.Parser()).parse(mReaderSchemaJson);
  }

  /**
   * @return <code>true</code> if the writer schema is configured for use when decoding.
   */
  public boolean isUseWriterSchema() {
    return mSchemaMode == SchemaMode.WRITER_SCHEMA;
  }

  /**
   * @return <code>true</code> if the table layout reader schema is configured for use when
   *     decoding.
   */
  public boolean isUseTableReaderSchema() {
    return mSchemaMode == SchemaMode.TABLE_READER_SCHEMA;
  }

  /**
   * @return <code>true</code> if the Avro SpecificData decoder is configured for use when
   *    decoding.
   */
  public boolean isUseAvroSpecificDecoder() {
    return mDecoder == Decoder.AVRO_SPECIFIC;
  }

  /**
   * @return <code>true</code> if the Avro GenericData decoder is configured for use when decoding.
   */
  public boolean isUseAvroGenericDecoder() {
    return mDecoder == Decoder.AVRO_GENERIC;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (other instanceof DecodeConfig) {
      DecodeConfig that = (DecodeConfig) other;
      return mSchemaMode == that.mSchemaMode
          && mDecoder == that.mDecoder
          && mReaderSchemaJson == that.mReaderSchemaJson;
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mSchemaMode, mDecoder, mReaderSchemaJson);
  }
}
