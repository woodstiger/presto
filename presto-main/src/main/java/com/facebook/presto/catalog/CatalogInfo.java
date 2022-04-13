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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class CatalogInfo {

    private final String catalogName;
    private final String connectorName;
    private final String creator;
    private final Map<String, String> properties;

    @JsonCreator
    public CatalogInfo(@JsonProperty("catalogName") String catalogName,
                       @JsonProperty("connectorName") String connectorName,
                       @JsonProperty("creator") String creator,
                       @JsonProperty("properties") Map<String, String> properties) {
        this.catalogName = catalogName;
        this.connectorName = connectorName;
        this.creator = creator;
        this.properties = properties;
    }

    @JsonProperty
    public String getCatalogName() {
        return catalogName;
    }

    @JsonProperty
    public String getConnectorName() {
        return connectorName;
    }

    @JsonProperty
    public String getCreator() {
        return creator;
    }

    @JsonProperty
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "CatalogInfo{" +
                "catalogName='" + catalogName + '\'' +
                ", connectorName='" + connectorName + '\'' +
                ", creator='" + creator + '\'' +
                ", properties=" + properties +
                '}';
    }
}
