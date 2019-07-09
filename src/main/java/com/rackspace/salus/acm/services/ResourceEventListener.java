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

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ResourceEventListener {

  private final KafkaTopicProperties kafkaTopicProperties;
  private final AgentInstallService agentInstallService;

  @Autowired
  public ResourceEventListener(KafkaTopicProperties kafkaTopicProperties,
                               AgentInstallService agentInstallService) {
    this.kafkaTopicProperties = kafkaTopicProperties;
    this.agentInstallService = agentInstallService;
  }

  public String getTopic() {
    return kafkaTopicProperties.getResources();
  }

  @KafkaListener(topics = "#{__listener.topic}")
  public void consumeResourceEvent(ResourceEvent event) {
    agentInstallService.handleResourceEvent(event);
  }
}
