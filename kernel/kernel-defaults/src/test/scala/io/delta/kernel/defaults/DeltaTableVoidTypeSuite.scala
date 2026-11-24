/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.defaults

import java.io.File
import java.util.Optional

import io.delta.kernel.Table
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultConstantVector
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.types.VoidType
import io.delta.kernel.internal.util.SchemaUtils
import io.delta.kernel.types.{ArrayType, IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for the Delta protocol's `void` type: readers null-fill void columns, writers
 * omit them from data files, and unwritable schema shapes reject data writes.
 */
class DeltaTableVoidTypeSuite extends AnyFunSuite with WriteUtils {

  private val voidSchema = new StructType()
    .add("a", IntegerType.INTEGER)
    .add("s", new StructType().add("x", IntegerType.INTEGER).add("v", VoidType.VOID))
    .add("v", VoidType.VOID)

  /** The schema of the data supplied by a writer: no void columns at any level. */
  private val voidSchemaData = new StructType()
    .add("a", IntegerType.INTEGER)
    .add("s", new StructType().add("x", IntegerType.INTEGER))

  /** Asserts that every Parquet file under the table only has the given columns. */
  private def assertDataFileColumns(tablePath: String, expectedColumns: Seq[String]): Unit = {
    val dataFiles = new File(tablePath).listFiles().filter(_.getName.endsWith(".parquet"))
    assert(dataFiles.nonEmpty)
    dataFiles.foreach { file =>
      assert(spark.read.parquet(file.getAbsolutePath).schema.fieldNames.toSeq === expectedColumns)
    }
  }

  /** Reads the whole table with Kernel and returns, per row, whether column `colName` is null. */
  private def readIsNull(tablePath: String, engine: io.delta.kernel.engine.Engine, colName: String)
      : Seq[Boolean] = {
    val schema = latestSnapshot(tablePath, engine).getSchema
    val ordinal = schema.indexOf(colName)
    readTableUsingKernel(engine, tablePath, schema).flatMap { batch =>
      val vector = batch.getData.getColumnVector(ordinal)
      (0 until batch.getData.getSize).map(vector.isNullAt)
    }
  }

  test("read: void columns of a table written by Spark are null") {
    withTempDirAndEngine { (tablePath, engine) =>
      spark.sql(s"CREATE TABLE delta.`$tablePath`(x INT, y VOID) USING DELTA")
      spark.sql(s"INSERT INTO delta.`$tablePath` (x) VALUES (1), (2), (3)")

      val schema = latestSnapshot(tablePath, engine).getSchema
      assert(schema === new StructType().add("x", IntegerType.INTEGER).add("y", VoidType.VOID))
      assert(readIsNull(tablePath, engine, "y") === Seq(true, true, true))
      assert(readIsNull(tablePath, engine, "x") === Seq(false, false, false))
      assertDataFileColumns(tablePath, Seq("x"))
    }
  }

  test("write: void columns are omitted from data files and read back as null") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, voidSchema)
      assert(latestSnapshot(tablePath, engine).getSchema === voidSchema)

      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] ->
          generateData(voidSchemaData, Seq.empty, Map.empty, batchSize = 10, numBatches = 2)))

      assert(readIsNull(tablePath, engine, "v") === Seq.fill(20)(true))
      assert(readIsNull(tablePath, engine, "a").contains(false))
      assertDataFileColumns(tablePath, Seq("a", "s"))
      assert(spark.read.parquet(
        new File(tablePath).listFiles().filter(_.getName.endsWith(".parquet")).head.getPath)
        .schema("s").dataType.simpleString === "struct<x:int>")
    }
  }

  test("write: data that includes a void column is rejected") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, voidSchema)
      val numRows = 5
      val batch = new DefaultColumnarBatch(
        numRows,
        new StructType().add("a", IntegerType.INTEGER).add("v", VoidType.VOID),
        Array(
          new DefaultConstantVector(IntegerType.INTEGER, numRows, 1),
          new DefaultConstantVector(VoidType.VOID, numRows, null)))
      val e = intercept[KernelException] {
        appendData(
          engine,
          tablePath,
          data = Seq(Map.empty[String, Literal] -> Seq(new FilteredColumnarBatch(
            batch,
            Optional.empty()))))
      }
      assert(e.getMessage.contains("doesn't match the table schema"))
    }
  }

  Seq(
    "all columns are void" -> new StructType().add("v", VoidType.VOID),
    "a struct with only void fields" ->
      new StructType().add("a", IntegerType.INTEGER)
        .add("s", new StructType().add("v", VoidType.VOID)),
    "a void array element" ->
      new StructType().add("a", IntegerType.INTEGER).add("arr", new ArrayType(VoidType.VOID, true)))
    .foreach { case (name, schema) =>
      test(s"write: $name can be created but data writes are rejected") {
        withTempDirAndEngine { (tablePath, engine) =>
          createEmptyTable(engine, tablePath, schema)
          assert(latestSnapshot(tablePath, engine).getSchema === schema)

          val data = generateData(
            new StructType().add("a", IntegerType.INTEGER),
            Seq.empty,
            Map.empty,
            batchSize = 5,
            numBatches = 1)
          val e = intercept[KernelException] {
            appendData(engine, tablePath, data = Seq(Map.empty[String, Literal] -> data))
          }
          assert(e.getMessage.contains("unwritable void type"))
        }
      }
    }

  test("a void column can evolve into another type, which makes the table writable") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        new StructType().add("v", VoidType.VOID),
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))

      val table = Table.forPath(engine, tablePath)
      val current = table.getLatestSnapshot(engine).getSchema
      intercept[KernelException] {
        SchemaUtils.validateVoidTypesAreWritable(current)
      }

      val evolved = new StructType().add("v", StringType.STRING, true, current.get("v").getMetadata)
      updateTableMetadata(engine, tablePath, evolved)

      val updated = table.getLatestSnapshot(engine).getSchema
      assert(updated.get("v").getDataType === StringType.STRING)
      // void evolution is not a type widening
      assert(updated.get("v").getTypeChanges.isEmpty)
      SchemaUtils.validateVoidTypesAreWritable(updated)
    }
  }
}
