/*
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
package com.facebook.presto.catalog;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.LegacyConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.List;
import java.util.Optional;

public class DynamicRestCatalogStoreConfig {
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private File catalogConfigurationDir = new File("etc/catalog/");
    private List<String> disabledCatalogs;
    private String zkFlag;
    private String restPath;
    private long timeInterval;
    private String dynamicSecretkey;
    private long connectTimeOut;
    private long readTimeOut;
    private long writeTimeOut;

    @NotNull
    public File getCatalogConfigurationDir() {
        return catalogConfigurationDir;
    }

    @LegacyConfig("plugin.config-dir")
    @Config("catalog.config-dir")
    public DynamicRestCatalogStoreConfig setCatalogConfigurationDir(File dir) {
        this.catalogConfigurationDir = dir;
        return this;
    }

    public List<String> getDisabledCatalogs() {
        return disabledCatalogs;
    }

    @Config("catalog.disabled-catalogs")
    public DynamicRestCatalogStoreConfig setDisabledCatalogs(String catalogs) {
        this.disabledCatalogs = (catalogs == null) ? null : SPLITTER.splitToList(catalogs);
        return this;
    }

    public DynamicRestCatalogStoreConfig setDisabledCatalogs(List<String> catalogs) {
        this.disabledCatalogs = (catalogs == null) ? null : ImmutableList.copyOf(catalogs);
        return this;
    }

    @Config("catalog.rest.path")
    public DynamicRestCatalogStoreConfig setCatalogRestPath(String restPath) {
        this.restPath = restPath;
        return this;
    }

    public String getCatalogRestPath() {
        return restPath;
//        return Objects.requireNonNullElse(restPath, "/catalog/meta");
    }

    @Config("catalog.zk.flag")
    public DynamicRestCatalogStoreConfig setCatalogZkFlag(String zkFlag) {
        this.zkFlag = zkFlag;
        return this;
    }

    public String getCatalogZkFlag() {
        return zkFlag;
    }

    @Config("catalog.rest.time-interval")
    public DynamicRestCatalogStoreConfig setCatalogRestTimeInterval(long timeInterval) {
        this.timeInterval = timeInterval;
        return this;
    }

    public long getCatalogRestTimeInterval() {
        return timeInterval;
    }



    public long getCatalogConnectTimeOut() {
        return connectTimeOut;
    }

    @Config("catalog.dynamic.connect-timeout")
    public DynamicRestCatalogStoreConfig setCatalogConnectTimeOut(long connectTimeOut) {
        this.connectTimeOut = connectTimeOut;
        return this;
    }

    public long getCatalogReadTimeOut() {
        return readTimeOut;
    }

    @Config("catalog.dynamic.read-timeout")
    public DynamicRestCatalogStoreConfig setCatalogReadTimeOut(long readTimeOut) {
        this.readTimeOut = readTimeOut;
        return this;
    }

    public long getCatalogWriteTimeOut() {
        return writeTimeOut;
    }
    @Config("catalog.dynamic.write-timeout")
    public DynamicRestCatalogStoreConfig setCatalogWriteTimeOut(long writeTimeOut) {
        this.writeTimeOut = writeTimeOut;
        return this;
    }

    @Config("catalog.dynamic.secret-key")
    public DynamicRestCatalogStoreConfig setCatalogDynamicSecretkey(String dynamicSecretkey) {
        this.dynamicSecretkey = dynamicSecretkey;
        return this;
    }

    public String getCatalogDynamicSecretkey() {
        return dynamicSecretkey;
    }
}
