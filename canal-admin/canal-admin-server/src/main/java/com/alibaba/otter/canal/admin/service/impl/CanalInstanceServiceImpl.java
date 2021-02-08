package com.alibaba.otter.canal.admin.service.impl;

import io.ebean.Query;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.common.Threads;
import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.admin.connector.AdminConnector;
import com.alibaba.otter.canal.admin.connector.SimpleAdminConnectors;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.model.Pager;
import com.alibaba.otter.canal.admin.service.CanalInstanceService;
import com.alibaba.otter.canal.protocol.SecurityUtil;
import com.google.common.collect.Lists;

/**
 * Canal实例配置信息业务层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Service
public class CanalInstanceServiceImpl implements CanalInstanceService {

    /**
     * canal实例配置列表信息，远程拉去是否处于运行中的状态
     * @param canalInstanceConfig
     * @param pager
     * @return
     */
    @Override
    public Pager<CanalInstanceConfig> findList(CanalInstanceConfig canalInstanceConfig, Pager<CanalInstanceConfig> pager) {
        /**
         * cluster：
         *  根据clusterId和[name]  对应多个 canalInstanceConfig
         * server:
         *  根据serverId和[name]   对应1个 canalInstanceConfig
         * 获取canalInstanceConfig信息
         */
        Query<CanalInstanceConfig> query = CanalInstanceConfig.find.query()
            .setDisableLazyLoading(true)
            .select("clusterId, serverId, name, modifiedTime")
            .fetch("canalCluster", "name")
            .fetch("nodeServer", "name,ip,adminPort");
        if (canalInstanceConfig != null) {
            if (StringUtils.isNotEmpty(canalInstanceConfig.getName())) {
                query.where().like("name", "%" + canalInstanceConfig.getName() + "%");
            }
            if (StringUtils.isNotEmpty(canalInstanceConfig.getClusterServerId())) {
                if (canalInstanceConfig.getClusterServerId().startsWith("cluster:")) {
                    query.where()
                        .eq("clusterId", Long.parseLong(canalInstanceConfig.getClusterServerId().substring(8)));
                } else if (canalInstanceConfig.getClusterServerId().startsWith("server:")) {
                    query.where().eq("serverId", Long.parseLong(canalInstanceConfig.getClusterServerId().substring(7)));
                }
            }
        }

        Query<CanalInstanceConfig> queryCnt = query.copy();
        int count = queryCnt.findCount();
        pager.setCount((long) count);

        query.setFirstRow(pager.getOffset().intValue()).setMaxRows(pager.getSize()).order().asc("id");
        List<CanalInstanceConfig> canalInstanceConfigs = query.findList();
        pager.setItems(canalInstanceConfigs);

        if (canalInstanceConfigs.isEmpty()) {
            return pager;
        }

        /**
         * canalInstanceConfig
         * cluster 对应多个 nodeServer
         * server  对应一个 nodeServer
         */
        // check all canal instances running status
        List<Future<Void>> futures = new ArrayList<>(canalInstanceConfigs.size());
        for (CanalInstanceConfig canalInstanceConfig1 : canalInstanceConfigs) {
            futures.add(Threads.executorService.submit(() -> {
                List<NodeServer> nodeServers;
                if (canalInstanceConfig1.getClusterId() != null) { // 集群模式
                    nodeServers = NodeServer.find.query()
                        .where()
                        .eq("clusterId", canalInstanceConfig1.getClusterId())
                        .findList();
                } else if (canalInstanceConfig1.getServerId() != null) { // 单机模式
                    nodeServers = Collections.singletonList(canalInstanceConfig1.getNodeServer());
                } else {
                    return null;
                }

                for (NodeServer nodeServer : nodeServers) {
                    /**
                     * 获取所有当前节点下运行中的实例  runningInstances
                     */
                    String runningInstances = SimpleAdminConnectors.execute(nodeServer.getIp(),
                        nodeServer.getAdminPort(),
                        AdminConnector::getRunningInstances);
                    if (runningInstances == null) {
                        continue;
                    }
                    String[] instances = runningInstances.split(",");
                    /**
                     * 匹配当前canalInstanceConfig1.name 即可设置为 运行中
                     */
                    for (String instance : instances) {
                        if (instance.equals(canalInstanceConfig1.getName())) {
                            // 集群模式下server对象为空
                            if (canalInstanceConfig1.getNodeServer() == null) {
                                canalInstanceConfig1.setNodeServer(nodeServer);
                            }
                            canalInstanceConfig1.setRunningStatus("1");
                            break;
                        }
                    }
                }

                return null;
            }));
        }

        futures.forEach(f -> {
            try {
                f.get(3, TimeUnit.SECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                // ignore
            }
        });
        return pager;
    }

    /**
     * 通过Server id获取当前Server下所有运行的Instance
     *
     * @param serverId server id
     */
    @Override
    public List<CanalInstanceConfig> findActiveInstanceByServerId(Long serverId) {
        NodeServer nodeServer = NodeServer.find.byId(serverId);
        if (nodeServer == null) {
            return null;
        }
        String runningInstances = SimpleAdminConnectors.execute(nodeServer.getIp(),
            nodeServer.getAdminPort(),
            AdminConnector::getRunningInstances);
        if (runningInstances == null) {
            return null;
        }

        String[] instances = runningInstances.split(",");
        Object obj[] = Lists.newArrayList(instances).toArray();
        // 单机模式和集群模式区分处理
        if (nodeServer.getClusterId() != null) { // 集群模式
            List<CanalInstanceConfig> list = CanalInstanceConfig.find.query()
                .setDisableLazyLoading(true)
                .select("clusterId, serverId, name, modifiedTime")
                .where()
                // 暂停的实例也显示 .eq("status", "1")
                .in("name", obj)
                .findList();
            list.forEach(config -> config.setRunningStatus("1"));
            return list; // 集群模式直接返回当前运行的Instances
        } else { // 单机模式
            // 当前Server所配置的所有Instance
            List<CanalInstanceConfig> list = CanalInstanceConfig.find.query()
                .setDisableLazyLoading(true)
                .select("clusterId, serverId, name, modifiedTime")
                .where()
                // 暂停的实例也显示 .eq("status", "1")
                .eq("serverId", serverId)
                .findList();
            List<String> instanceList = Arrays.asList(instances);
            list.forEach(config -> {
                if (instanceList.contains(config.getName())) {
                    config.setRunningStatus("1");
                }
            });
            return list;
        }
    }

    @Override
    public void save(CanalInstanceConfig canalInstanceConfig) {
        if (StringUtils.isEmpty(canalInstanceConfig.getClusterServerId())) {
            throw new ServiceException("empty cluster or server id");
        }
        if (canalInstanceConfig.getClusterServerId().startsWith("cluster:")) {
            Long clusterId = Long.parseLong(canalInstanceConfig.getClusterServerId().substring(8));
            canalInstanceConfig.setClusterId(clusterId);
        } else if (canalInstanceConfig.getClusterServerId().startsWith("server:")) {
            Long serverId = Long.parseLong(canalInstanceConfig.getClusterServerId().substring(7));
            canalInstanceConfig.setServerId(serverId);
        }

        try {
            String contentMd5 = SecurityUtil.md5String(canalInstanceConfig.getContent());
            canalInstanceConfig.setContentMd5(contentMd5);
        } catch (NoSuchAlgorithmException e) {
            // ignore
        }

        canalInstanceConfig.insert();
    }

    @Override
    public CanalInstanceConfig detail(Long id) {
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig != null) {
            if (canalInstanceConfig.getClusterId() != null) {
                canalInstanceConfig.setClusterServerId("cluster:" + canalInstanceConfig.getClusterId());
            } else if (canalInstanceConfig.getServerId() != null) {
                canalInstanceConfig.setClusterServerId("server:" + canalInstanceConfig.getServerId());
            }
        }
        return canalInstanceConfig;
    }

    @Override
    public void updateContent(CanalInstanceConfig canalInstanceConfig) {
        if (StringUtils.isEmpty(canalInstanceConfig.getClusterServerId())) {
            throw new ServiceException("empty cluster or server id");
        }
        if (canalInstanceConfig.getClusterServerId().startsWith("cluster:")) {
            Long clusterId = Long.parseLong(canalInstanceConfig.getClusterServerId().substring(8));
            canalInstanceConfig.setClusterId(clusterId);
            canalInstanceConfig.setServerId(null);
        } else if (canalInstanceConfig.getClusterServerId().startsWith("server:")) {
            Long serverId = Long.parseLong(canalInstanceConfig.getClusterServerId().substring(7));
            canalInstanceConfig.setServerId(serverId);
            canalInstanceConfig.setClusterId(null);
        }

        try {
            String contentMd5 = SecurityUtil.md5String(canalInstanceConfig.getContent());
            canalInstanceConfig.setContentMd5(contentMd5);
        } catch (NoSuchAlgorithmException e) {
            // ignore
        }

        canalInstanceConfig.update("content", "contentMd5", "clusterId", "serverId");
    }

    @Override
    public void delete(Long id) {
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig != null) {
            canalInstanceConfig.delete();
        }
    }

    @Override
    public Map<String, String> remoteInstanceLog(Long id, Long nodeId) {
        Map<String, String> result = new HashMap<>();

        NodeServer nodeServer = NodeServer.find.byId(nodeId);
        if (nodeServer == null) {
            return result;
        }
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig == null) {
            return result;
        }

        String log = SimpleAdminConnectors.execute(nodeServer.getIp(),
            nodeServer.getAdminPort(),
            adminConnector -> adminConnector.instanceLog(canalInstanceConfig.getName(), null, 100));

        result.put("instance", canalInstanceConfig.getName());
        result.put("log", log);
        return result;
    }

    @Override
    public boolean remoteOperation(Long id, Long nodeId, String option) {
        NodeServer nodeServer = null;
        if ("start".equals(option)) {
            if (nodeId != null) {
                nodeServer = NodeServer.find.byId(nodeId);
            } else {
                nodeServer = NodeServer.find.query().findOne();
            }
        } else {
            if (nodeId == null) {
                return false;
            }
            nodeServer = NodeServer.find.byId(nodeId);
        }
        if (nodeServer == null) {
            return false;
        }
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig == null) {
            return false;
        }
        Boolean result = null;
        if ("start".equals(option)) {
            if (nodeServer.getClusterId() == null) {
                // 非集群模式下直接将状态置为1
                return instanceOperation(id, "start");
            } else {
                throw new ServiceException("集群模式不允许指定server启动");
            }
        } else if ("stop".equals(option)) {
            if (nodeServer.getClusterId() != null) {
                // 集群模式,通知主动释放
                result = SimpleAdminConnectors.execute(nodeServer.getIp(),
                    nodeServer.getAdminPort(),
                    adminConnector -> adminConnector.releaseInstance(canalInstanceConfig.getName()));
            } else { // 非集群模式下直接将状态置为0
                return instanceOperation(id, "stop");
            }
        } else {
            return false;
        }

        if (result == null) {
            result = false;
        }
        return result;
    }

    @Override
    public boolean instanceOperation(Long id, String option) {
        CanalInstanceConfig canalInstanceConfig = CanalInstanceConfig.find.byId(id);
        if (canalInstanceConfig == null) {
            return false;
        }
        if ("stop".equals(option)) {
            canalInstanceConfig.setStatus("0");
            canalInstanceConfig.update("status");
        } else if ("start".equals(option)) {
            canalInstanceConfig.setStatus("1");
            canalInstanceConfig.update("status");
        } else {
            return false;
        }
        return true;
    }
}
