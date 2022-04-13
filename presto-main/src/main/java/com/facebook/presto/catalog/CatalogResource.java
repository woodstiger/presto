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

import com.facebook.airlift.log.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.facebook.presto.catalog.ResourceSecurity.AccessType.PUBLIC;

@Path("/v1/catalogs")
public class CatalogResource {

    private static final Logger log = Logger.get(CatalogResource.class);

    @Inject
    public CatalogResource() {

    }

    @GET
    @Path("/test")
    @ResourceSecurity(PUBLIC)
    public Response test() {
        return Response.ok("hello catalog").build();
    }

    @GET
    @ResourceSecurity(PUBLIC)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String list() {
        try {
            StringBuffer sb = new StringBuffer();
            sb.append("catalogInfoCache => ");
            sb.append(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(DynamicRestCatalogStore.catalogInfoCache));
            sb.append(" fileCatalogCacheMap => ");
            sb.append(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(DynamicRestCatalogStore.fileCatalogCacheMap));
            return sb.toString();
        } catch (Exception e) {
            log.error("failed get catalogs ", e);
            return null;
        }
    }

    @GET
    @Path("/{catalogName}")
    @ResourceSecurity(PUBLIC)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String detail(@PathParam("catalogName") String catalogName) {
        try {
            StringBuffer sb = new StringBuffer();
            sb.append("catalogInfoCache => ");
            sb.append(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(DynamicRestCatalogStore.catalogInfoCache.get(catalogName)));
            sb.append(" fileCatalogCacheMap => ");
            sb.append(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(DynamicRestCatalogStore.fileCatalogCacheMap.get(catalogName)));
            return sb.toString();
        } catch (Exception e) {
            log.error("failed get catalog detail");
            throw DynamicCatalogException.newInstance("failed get catalog detail", e);
        }
    }


}
