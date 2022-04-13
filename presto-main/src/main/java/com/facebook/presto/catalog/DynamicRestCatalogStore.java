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

import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import okhttp3.*;
import sun.security.provider.MD5;

import static com.facebook.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static com.google.common.base.Strings.nullToEmpty;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicRestCatalogStore {
    private static final Logger log = Logger.get(DynamicRestCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final ScheduledExecutorService scheduledExecutorService =
            new ScheduledThreadPoolExecutor(1);
    public final static Map<String, CatalogInfo> catalogInfoCache = new ConcurrentHashMap<>();
    public final static Map<String, Map<String, String>> fileCatalogCacheMap = new ConcurrentHashMap<>();
    private Announcer announcer;
    private DynamicRestCatalogStoreConfig config;

    @Inject
    public DynamicRestCatalogStore(ConnectorManager connectorManager,
                                   DynamicRestCatalogStoreConfig config,
                                   Announcer announcer) {
        this(connectorManager, config,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()), announcer);
    }

    public DynamicRestCatalogStore(ConnectorManager connectorManager,
                                   DynamicRestCatalogStoreConfig config,
                                   File catalogConfigurationDir,
                                   List<String> disabledCatalogs,
                                   Announcer announcer) {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.config = config;
        this.announcer = requireNonNull(announcer, "announcer is null");
    }

    public boolean areCatalogsLoaded() {
        return catalogsLoaded.get();
    }

    public DynamicRestCatalogStoreConfig getDynamicRestCatalogStoreConfig() {
        return this.config;
    }

    public void loadCatalogs()
            throws Exception {
        loadCatalogs(ImmutableMap.of());
    }

    public void loadCatalogs(Map<String, Map<String, String>> additionalCatalogs)
            throws Exception {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }
        //第一次可以先从文件加载 若需要启动时也调用接口在注释foreach
        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }

        // 取消注释 则为启动时也调用接口
//        load();

        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    reload();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("--scheduledExecutorService run error --", e.getMessage());
                }
            }
        }, 60, config.getCatalogRestTimeInterval(), TimeUnit.SECONDS);
// ?
//        additionalCatalogs.forEach(this::loadCatalog);

        catalogsLoaded.set(true);

    }

    /**
     * start
     */
    private Map<String, CatalogInfo> requertCatalogInfoMapstr(String restPath) {
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
        Response execute = null;
        try {
            execute = client.newCall(builder.build()).execute();
            return new ObjectMapper().readValue(execute.body().string(), new TypeReference<Map<String, CatalogInfo>>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
            log.error("---query catalogInfoMap from ---", restPath, e.getMessage());
        }
        return null;
    }

    private void load() {
        Map<String, CatalogInfo> catalogInfoMap = requertCatalogInfoMapstr(config.getCatalogRestPath());
        if (null == catalogInfoMap) {
            log.warn("--load() catalogInfoMap is null forEach Loading catalog   --");
            return;
        }
        catalogInfoMap.forEach((key, catalogInfo) -> {
            try {
                loadCatalog(catalogInfo);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("--catalogInfoMap forEach Loading catalog  --", e);
            }
        });
        catalogInfoCache.putAll(catalogInfoMap);
    }

    private void reload() {
        log.info("--newCatalogInfoMap reLoading catalog---");
        Map<String, CatalogInfo> newCatalogInfoMap = requertCatalogInfoMapstr(config.getCatalogRestPath());
        if (null == newCatalogInfoMap) {
            log.warn("--newCatalogInfoMap is null forEach Loading catalog ");
            return;
        }

        Map<String, CatalogInfo> addCatalogInfoMap = new HashMap<>();
        Map<String, CatalogInfo> updCatalogInfoMap = new HashMap<>();
        Map<String, CatalogInfo> delCatalogInfoMap = new HashMap<>();
//        System.out.println(fileCatalogCacheMap.toString());
        //md5(toJsonStr(CatalogInfo))  catalogInfoCache
        newCatalogInfoMap.forEach((key, catalogInfo) -> {
            //配置文件加载的catalog 不操作
            if (!fileCatalogCacheMap.containsKey(key)) {
                //新增 新的里面有  老的里面没有
                if (!catalogInfoCache.containsKey(key)) {
                    addCatalogInfoMap.put(key, catalogInfo);
                    catalogInfoCache.put(key, catalogInfo);
                }
            }

        });

        newCatalogInfoMap.forEach((key, catalogInfo) -> {
            //配置文件加载的catalog 不操作
            if (!fileCatalogCacheMap.containsKey(key)) {
                //修改
                if (catalogInfoCache.containsKey(key)) {
                    String oldCatalogInf = null;
                    try {
                        oldCatalogInf = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(catalogInfoCache.get(key));
                        String newCatalogInf = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(newCatalogInfoMap.get(key));
                        if (!oldCatalogInf.equals(newCatalogInf)) {
                            updCatalogInfoMap.put(key, catalogInfo);
                            catalogInfoCache.put(key, catalogInfo);
                        }
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        catalogInfoCache.forEach((key, catalogInfo) -> {
            //配置文件加载的catalog 不操作
            if (!fileCatalogCacheMap.containsKey(key)) {
                //删除  老的里面有  新的里面没有。
                if (!newCatalogInfoMap.containsKey(key)) {
                    delCatalogInfoMap.put(key, catalogInfo);
                    catalogInfoCache.remove(key);
                }
            }
        });

//        System.out.println(System.getProperty("java.library.path"));
        //新增  修改  删除
        addCatalogInfoMap.forEach((key, catalogInfo) -> {
            try {
                addCatalog(catalogInfo);
            } catch (Exception e) {
                e.printStackTrace();
                catalogInfoCache.remove(key);
            }

        });

        updCatalogInfoMap.forEach((key, catalogInfo) -> {
            log.info("--updateCatalog:" + catalogInfo.toString());
            try {
                deleteCatalog(catalogInfo);
                addCatalog(catalogInfo);
            } catch (Exception e) {
                e.printStackTrace();
                catalogInfoCache.remove(key);
            }

        });

        delCatalogInfoMap.forEach((key, catalogInfo) -> {
            try {
                deleteCatalog(catalogInfo);
            } catch (Exception e) {
                e.printStackTrace();
            }

        });
        addCatalogInfoMap.clear();
        updCatalogInfoMap.clear();
        delCatalogInfoMap.clear();
    }

    private void deleteCatalog(CatalogInfo catalogInfo) {
        log.info("--deleteCatalog:" + catalogInfo.toString());
        String catalogName = catalogInfo.getCatalogName();
        connectorManager.dropConnection(catalogName);
        updateConnectorIds(announcer, catalogName, CataLogAction.DELETE);
    }

    private void addCatalog(CatalogInfo catalogInfo) {
        log.info("--addCatalog:" + catalogInfo.toString());
        String catalogName = catalogInfo.getCatalogName();
        String connectorName = catalogInfo.getConnectorName();
        connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(catalogInfo.getProperties()));
        updateConnectorIds(announcer, catalogName, CataLogAction.ADD);
        log.info("--------------add or update catalog ------------------" + catalogInfo.toString());
    }

    private void updateConnectorIds(Announcer announcer, String catalogName, CataLogAction action) {
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        String property = nullToEmpty(announcement.getProperties().get("connectorIds"));
        List<String> values = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property);
        Set<String> connectorIds = new LinkedHashSet<>(values);
        if (CataLogAction.DELETE.equals(action)) {
            connectorIds.remove(catalogName);
        } else {
            connectorIds.add(catalogName);
        }

        ServiceAnnouncement.ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("connectorIds")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());

        announcer.forceAnnounce();

    }


    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements) {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }


    public enum CataLogAction {
        ADD, DELETE, UPDATE
    }

    private void loadCatalog(CatalogInfo catalogInfo)
            throws Exception {
        String catalogName = catalogInfo.getCatalogName();

        log.info("-- Loading catalog  %s --", catalogName);
        Map<String, String> properties = catalogInfo.getProperties();
        String connectorName = catalogInfo.getConnectorName();
        checkState(properties.containsKey("connector.name"), "Catalog configuration %s does not contain connector.name", connectorName);

        loadCatalog(catalogName, properties);
    }

    /**
     * end
     */

    private void loadCatalog(File file)
            throws Exception {
        String catalogName = Files.getNameWithoutExtension(file.getName());

        log.info("-- Loading catalog properties %s --", file);
        Map<String, String> properties = loadProperties(file);
        checkState(properties.containsKey("connector.name"), "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());
        fileCatalogCacheMap.put(catalogName, properties);
        loadCatalog(catalogName, properties);
    }

    private void loadCatalog(String catalogName, Map<String, String> properties) {
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", catalogName);

        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals("connector.name")) {
                connectorName = entry.getValue();
            } else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }

        checkState(connectorName != null, "Configuration for catalog %s does not contain connector.name", catalogName);

        connectorManager.createConnection(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private static List<File> listFiles(File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
