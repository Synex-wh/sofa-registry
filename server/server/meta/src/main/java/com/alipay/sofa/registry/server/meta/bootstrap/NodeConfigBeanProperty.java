/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.registry.server.meta.bootstrap;

import com.alipay.sofa.registry.log.Logger;
import com.alipay.sofa.registry.log.LoggerFactory;
import com.alipay.sofa.registry.net.NetUtil;
import org.springframework.beans.factory.annotation.Value;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author shangyu.wh
 * @version $Id: NodeConfigBeanProperty.java, v 0.1 2018-05-03 16:21 shangyu.wh Exp $
 */
public class NodeConfigBeanProperty implements NodeConfig {

    private static final Logger                              LOGGER = LoggerFactory
                                                                        .getLogger(NodeConfigBeanProperty.class);

    @Value("#{PropertySplitter.mapOfList('${nodes.metaNode}')}")
    private Map<String/*dataCenterId*/, Collection<String>> metaNode;

    @Value("${nodes.localDataCenter}")
    private String                                           localDataCenter;

    @Override
    public Map<String, Collection<String>> getMetaNode() {
        return metaNode;
    }

    /**
     * Getter method for property <tt>localDataCenter</tt>.
     *
     * @return property value of localDataCenter
     */
    @Override
    public String getLocalDataCenter() {
        return localDataCenter;
    }

    @Override
    public Set<String> getDataCenterMetaServers(String dataCenterIn) {
        Map<String, Collection<String>> metaMap = metaNode;
        Set<String> metaServerIpSet = new HashSet<>();
        if (metaMap != null && metaMap.size() > 0) {
            Collection<String> list = metaMap.get(dataCenterIn);
            if (list != null) {
                metaServerIpSet.addAll(list);
            }
        }
        return metaServerIpSet;
    }

    @Override
    public Collection<String> getAllDataCenters() {
        if (metaNode != null) {
            return metaNode.keySet();
        }
        return null;
    }

    public String getLocalHost(){
        //if current ip existed in config list,register it
        if (metaNode != null && metaNode.size() > 0) {

            String ip = NetUtil.getLocalAddress().getHostAddress();
            Collection<String> metas = (metaNode.get(localDataCenter)).stream().filter(domain-> ip.equals(convertIp(domain))).collect(
                    Collectors.toList());

            if (metas != null && metas.size()==1) {
                return metas.iterator().next();

            } else {
                LOGGER.error(
                        "Get localhost fail!meta node list config not contains current ip {}",
                        ip);
                throw new RuntimeException(
                        "Get localhost fail!meta node list config not contains current ip!");
            }
        }
        return null;
    }

    private String convertIp(String domain) {
        String ip = NetUtil.getIPAddressFromDomain(domain);
        if (ip == null) {
            LOGGER.error("Node config convert domain {} error!", domain);
            throw new RuntimeException("Node config convert domain {" + domain + "} error!");
        }
        return ip;
    }
}