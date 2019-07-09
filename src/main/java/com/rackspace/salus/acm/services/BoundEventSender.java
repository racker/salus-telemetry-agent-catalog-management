/*
 * Copyright 2019 Rackspace US, Inc.
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

package com.rackspace.salus.acm.services;

import com.rackspace.salus.common.messaging.KafkaMessageKeyBuilder;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.telemetry.messaging.AgentInstallChangeEvent;
import com.rackspace.salus.telemetry.messaging.OperationType;
import com.rackspace.salus.telemetry.model.AgentType;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class BoundEventSender {

  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final KafkaTopicProperties kafkaTopicProperties;

  @Autowired
  public BoundEventSender(KafkaTemplate<String,Object> kafkaTemplate, KafkaTopicProperties kafkaTopicProperties) {
    this.kafkaTemplate = kafkaTemplate;
    this.kafkaTopicProperties = kafkaTopicProperties;
  }
  /**
   *  @param op {@link OperationType#UPSERT} or {@link OperationType#DELETE} where update is more
   * like an upsert and might indicate the first install for the resource
   * @param agentType
   * @param affectedResources
   */
  public void sendTo(OperationType op,
                     AgentType agentType,
                     List<TenantResource> affectedResources) {

    final String topic = kafkaTopicProperties.getInstalls();

    for (TenantResource affectedResource : affectedResources) {
      final AgentInstallChangeEvent event = new AgentInstallChangeEvent()
          .setTenantId(affectedResource.getTenantId())
          .setResourceId(affectedResource.getResourceId())
          .setOp(op)
          .setAgentType(agentType);

      log.debug("Sending event={} on topic={}", event, topic);
      final String key = KafkaMessageKeyBuilder.buildMessageKey(event);
      kafkaTemplate.send(topic, key, event);
    }
  }
}
