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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.telemetry.messaging.AgentInstallChangeEvent;
import com.rackspace.salus.telemetry.messaging.OperationType;
import com.rackspace.salus.telemetry.model.AgentType;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@SuppressWarnings("unchecked")
@RunWith(SpringRunner.class)
@SpringBootTest
public class BoundEventSenderTest {

  @Configuration
  @Import({BoundEventSender.class})
  public static class TestConfig {

    @Bean
    public KafkaTopicProperties kafkaTopicProperties() {
      return new KafkaTopicProperties();
    }
  }

  @MockBean
  KafkaTemplate kafkaTemplate;

  @Autowired
  KafkaTopicProperties kafkaTopicProperties;

  @Autowired
  BoundEventSender boundEventSender;

  @Test
  public void testSending() {
    boundEventSender.sendTo(OperationType.UPSERT, AgentType.TELEGRAF, Arrays.asList(
        new TenantResource("t-1", "r-1"),
        new TenantResource("t-1", "r-2")
    ));

    verify(kafkaTemplate).send(
        kafkaTopicProperties.getInstalls(),
        "t-1:r-1",
        new AgentInstallChangeEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setAgentType(AgentType.TELEGRAF)
            .setOp(OperationType.UPSERT)
    );
    verify(kafkaTemplate).send(
        kafkaTopicProperties.getInstalls(),
        "t-1:r-2",
        new AgentInstallChangeEvent()
            .setTenantId("t-1")
            .setResourceId("r-2")
            .setAgentType(AgentType.TELEGRAF)
            .setOp(OperationType.UPSERT)
    );

    verifyNoMoreInteractions(kafkaTemplate);
  }
}