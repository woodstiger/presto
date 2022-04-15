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
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.catalog.ResourceSecurity.AccessType.PUBLIC;

@Path("/v1/catalogs")
public class CatalogResource {

    private static final Logger log = Logger.get(CatalogResource.class);

    DynamicRestCatalogStore dynamicRestCatalogStore;

    @Inject
    public CatalogResource(DynamicRestCatalogStore dynamicRestCatalogStore) {
        this.dynamicRestCatalogStore = dynamicRestCatalogStore;
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

    @GET
    @Path("/restPath/{restPath}")
    @ResourceSecurity(PUBLIC)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response restPath(@PathParam("restPath")String restPath) {
        DynamicRestCatalogStoreConfig config = dynamicRestCatalogStore.getDynamicRestCatalogStoreConfig();
        String before=config.getCatalogRestPath();

        okhttp3.Response execute = null;
        try {
            OkHttpClient client = new OkHttpClient.Builder()
                    .connectTimeout(config.getCatalogConnectTimeOut(), TimeUnit.SECONDS)
                    .writeTimeout(config.getCatalogWriteTimeOut(), TimeUnit.SECONDS)
                    .readTimeout(config.getCatalogReadTimeOut(), TimeUnit.SECONDS)
                    .build();
            Request.Builder builder = new Request.Builder();
            Request request = builder.url(restPath).build();
            HttpUrl.Builder urlBuilder = request.url().newBuilder();
            Headers.Builder headerBuilder = request.headers().newBuilder();

            builder.url(urlBuilder.build()).headers(headerBuilder.build());
            execute = client.newCall(builder.build()).execute();
           /*new ObjectMapper().readValue(execute.body().string(), new TypeReference<Map<String, CatalogInfo>>() {
            });*/
        } catch (Exception e) {
            e.printStackTrace();
            log.error("---check url errror ---", restPath, e.getMessage());
            return Response.ok("---check url errror --- \n"+
                    "url is "+ restPath +" : \n"
                    +getStackTraceInfo(e)).build();
        }
        config.setCatalogRestPath(URLDecoder.decode(restPath));
        String after=config.getCatalogRestPath();
        return Response.ok(before+" <before==after> "+after).build();
    }

    /**
     * 获取异常信息
     */
    public static String getStackTraceInfo(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        try {
            e.printStackTrace(pw);
            pw.flush();
            sw.flush();
            return sw.toString();
        } catch (Exception ex) {
            return "异常信息转换错误";
        } finally {
            try {
                pw.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                sw.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
