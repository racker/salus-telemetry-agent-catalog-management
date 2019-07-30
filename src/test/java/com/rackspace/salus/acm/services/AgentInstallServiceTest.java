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

import static com.rackspace.salus.telemetry.model.AgentType.FILEBEAT;
import static com.rackspace.salus.telemetry.model.AgentType.TELEGRAF;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.salus.acm.entities.AgentInstall;
import com.rackspace.salus.acm.entities.AgentRelease;
import com.rackspace.salus.acm.entities.BoundAgentInstall;
import com.rackspace.salus.acm.repositories.AgentInstallRepository;
import com.rackspace.salus.acm.repositories.AgentReleaseRepository;
import com.rackspace.salus.acm.repositories.BoundAgentInstallRepository;
import com.rackspace.salus.acm.web.model.AgentInstallCreate;
import com.rackspace.salus.acm.web.model.AgentInstallDTO;
import com.rackspace.salus.resource_management.web.client.ResourceApi;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import com.rackspace.salus.telemetry.messaging.OperationType;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.model.NotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.PlatformTransactionManager;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(SpringRunner.class)
@SpringBootTest(properties = "salus.kafka.chained.transaction.manager=false")
@AutoConfigureTestDatabase
@EnableAutoConfiguration(exclude = KafkaAutoConfiguration.class)
public class AgentInstallServiceTest {

  @Autowired
  PlatformTransactionManager transactionManager;
  @MockBean
  ResourceApi resourceApi;

  @MockBean
  BoundEventSender boundEventSender;

  @MockBean
  KafkaTemplate kafkaTemplate;

  @MockBean
  ResourceEventListener resourceEventListener;

  @Autowired
  AgentReleaseService agentReleaseService;

  @Autowired
  AgentInstallService agentInstallService;

  @Autowired
  AgentReleaseRepository agentReleaseRepository;

  @Autowired
  AgentInstallRepository agentInstallRepository;

  @Autowired
  BoundAgentInstallRepository boundAgentInstallRepository;

  @Autowired
  EntityManager em;

  @Captor
  ArgumentCaptor<List<TenantResource>> tenantResourcesArg;

  @After
  public void tearDown() {
    boundAgentInstallRepository.deleteAll();
    agentInstallRepository.deleteAll();
    agentReleaseRepository.deleteAll();
  }

  @Test
  public void testInstall_noPrior_noneSelected() {
    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);

    when(resourceApi.getResourcesWithLabels(eq("t-1"), any()))
        .thenReturn(Collections.emptyList());

    // EXECUTE

    final Map<String, String> labelSelector = Collections.singletonMap("os", "linux");

    final AgentInstallDTO install = agentInstallService.install(
        "t-1",
        new AgentInstallCreate()
            .setAgentReleaseId(release1.getId())
    );
  }
  private AgentRelease saveRelease(String v, AgentType agentType) {
    return agentReleaseRepository.save(
        new AgentRelease()
            .setType(agentType).setVersion(v).setUrl("").setExe("").setLabels(emptyMap())
    );
  }


}