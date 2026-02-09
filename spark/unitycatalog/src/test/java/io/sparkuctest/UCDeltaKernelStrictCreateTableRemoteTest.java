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

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.TableInfo;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Remote-only test: exercises the STRICT DSv2 (Kernel) CREATE TABLE path against a real Unity
 * Catalog server and validates that the Delta log commitInfo includes the Kernel engineInfo.
 *
 * <p>This is intentionally metadata-only: we avoid INSERT/CTAS because STRICT DSv2 writes are not
 * supported yet (SupportsWrite is not implemented on the Kernel-backed staged table).
 */
@EnabledIfEnvironmentVariable(named = "UC_REMOTE", matches = "(?i)true")
public class UCDeltaKernelStrictCreateTableRemoteTest extends UnityCatalogSupport {

  private SparkSession spark;

  @BeforeAll
  public void setUpSpark() {
    UnityCatalogInfo uc = unityCatalogInfo();
    String catalogName = uc.catalogName();

    SparkConf conf =
        new SparkConf()
            .setAppName("UnityCatalog Kernel STRICT Integration Test")
            .setMaster("local[2]")
            .set("spark.ui.enabled", "false")
            // Delta Lake required configurations
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            // Force the unified DeltaCatalog to route CREATE TABLE through the DSv2 (Kernel) path.
            .set("spark.databricks.delta.v2.enableMode", "STRICT")
            // Remote UC catalog configuration.
            .set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("spark.sql.catalog." + catalogName, "io.unitycatalog.spark.UCSingleCatalog")
            .set("spark.sql.catalog." + catalogName + ".uri", uc.serverUri())
            .set("spark.sql.catalog." + catalogName + ".token", uc.serverToken());

    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public void tearDownSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void strictKernelCreateManagedTableWritesEngineInfo() throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    String suffix = UUID.randomUUID().toString().replace("-", "");
    String fullTableName =
        uc.catalogName() + "." + uc.schemaName() + ".kernel_strict_create_" + suffix;

    try {
      spark.sql(
          String.format(
              "CREATE TABLE %s (i INT, s STRING) USING DELTA "
                  + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported', 'Foo'='Bar')",
              fullTableName));

      // Verify UC server-side storage location exists and the table is managed.
      TablesApi tablesApi = new TablesApi(uc.createApiClient());
      TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);
      assertThat(tableInfo.getStorageLocation()).isNotNull();
      assertThat(tableInfo.getTableType().name()).isEqualTo("MANAGED");

      // Validate that the first Delta commit contains the Kernel engineInfo marker.
      String storageLocation = tableInfo.getStorageLocation();
      String commit0 =
          storageLocation
              + (storageLocation.endsWith("/") ? "" : "/")
              + "_delta_log/00000000000000000000.json";
      String contents = readFullyAsUtf8(spark.sessionState().newHadoopConf(), new Path(commit0));
      assertThat(contents).contains("\"engineInfo\":\"kernel-spark-dsv2\"");
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fullTableName);
    }
  }

  @Test
  public void strictKernelCreateExternalTableWritesEngineInfo() throws Exception {
    UnityCatalogInfo uc = unityCatalogInfo();
    String suffix = UUID.randomUUID().toString().replace("-", "");
    String fullTableName =
        uc.catalogName() + "." + uc.schemaName() + ".kernel_strict_external_" + suffix;
    String location = uc.baseTableLocation() + "/kernel_strict_external_" + suffix;

    try {
      spark.sql(
          String.format(
              "CREATE TABLE %s (i INT, s STRING) USING DELTA "
                  + "TBLPROPERTIES ('Foo'='Bar') LOCATION '%s'",
              fullTableName, location));

      TablesApi tablesApi = new TablesApi(uc.createApiClient());
      TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);
      assertThat(tableInfo.getStorageLocation()).isEqualTo(location);
      assertThat(tableInfo.getTableType().name()).isEqualTo("EXTERNAL");

      String commit0 =
          location + (location.endsWith("/") ? "" : "/") + "_delta_log/00000000000000000000.json";
      String contents = readFullyAsUtf8(spark.sessionState().newHadoopConf(), new Path(commit0));
      assertThat(contents).contains("\"engineInfo\":\"kernel-spark-dsv2\"");
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + fullTableName);
    }
  }

  private static String readFullyAsUtf8(Configuration conf, Path path) throws Exception {
    FileSystem fs = path.getFileSystem(conf);
    try (FSDataInputStream in = fs.open(path)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[16 * 1024];
      int n;
      while ((n = in.read(buf)) >= 0) {
        out.write(buf, 0, n);
      }
      return out.toString(StandardCharsets.UTF_8);
    }
  }
}
