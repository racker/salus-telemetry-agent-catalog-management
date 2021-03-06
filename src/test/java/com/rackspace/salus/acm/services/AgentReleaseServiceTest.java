/*
 * Copyright 2020 Rackspace US, Inc.
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

import static com.rackspace.salus.telemetry.model.AgentType.TELEGRAF;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.rackspace.salus.acm.web.controller.AgentReleaseControllerTest;
import com.rackspace.salus.acm.web.model.AgentReleaseCreate;
import com.rackspace.salus.telemetry.entities.AgentInstall;
import com.rackspace.salus.telemetry.entities.AgentRelease;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.model.LabelSelectorMethod;
import com.rackspace.salus.telemetry.repositories.AgentInstallRepository;
import com.rackspace.salus.telemetry.repositories.AgentReleaseRepository;
import com.rackspace.salus.test.EnableTestContainersDatabase;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(
    properties = {
        "salus.services.resourceManagementUrl=http://this-is-a-non-null-value"
    }
)
@EnableTestContainersDatabase
@EnableAutoConfiguration(exclude = KafkaAutoConfiguration.class)
// skip the cache config to avoid cache already exists errors
@AutoConfigureCache(cacheProvider = CacheType.NONE)
public class AgentReleaseServiceTest {

  @MockBean
  AgentInstallService agentInstallService;

  @MockBean
  BoundEventSender boundEventSender;

  @Autowired
  AgentReleaseService agentReleaseService;

  @Autowired
  AgentReleaseRepository agentReleaseRepository;

  @Autowired
  AgentInstallRepository agentInstallRepository;

  @Test
  public void testCreate_unique() {
    final AgentReleaseCreate in = new AgentReleaseCreate()
        .setType(TELEGRAF)
        .setVersion("1.11.0")
        .setLabels(singletonMap("os", "linux"))
        .setUrl(
            "https://dl.influxdata.com/telegraf/releases/telegraf-1.11.0-static_linux_amd64.tar.gz")
        .setExe("./telegraf/telegraf");
    final AgentRelease agentRelease = agentReleaseService.create(in);

    assertThat(agentRelease).isNotNull();
    assertThat(agentRelease.getType()).isEqualTo(in.getType());
    assertThat(agentRelease.getVersion()).isEqualTo(in.getVersion());
    assertThat(agentRelease.getLabels()).isEqualTo(in.getLabels());
    assertThat(agentRelease.getUrl()).isEqualTo(in.getUrl());
    assertThat(agentRelease.getExe()).isEqualTo(in.getExe());
  }

  @Test
  public void testCreate_alreadyExists() {
    saveRelease("1.11.0", TELEGRAF, singletonMap("os", "linux"));

    final AgentReleaseCreate in = new AgentReleaseCreate()
        .setType(TELEGRAF)
        .setVersion("1.11.0")
        .setLabels(singletonMap("os", "linux"))
        .setUrl(
            "https://dl.influxdata.com/telegraf/releases/telegraf-1.11.0-static_linux_amd64.tar.gz")
        .setExe("./telegraf/telegraf");

    try {
      agentReleaseService.create(in);
      fail("Expected AlreadyExistsException");
    } catch (AlreadyExistsException e) {
      assertThat(e).hasMessage("An agent release with same type, version, and labels already exists");
    }

  }

  /**
   * This test confirms the expected behavior in
   * {@link AgentReleaseControllerTest#testGetAgentReleasesForTenant_specificType_beyondLastPage()}
   * where the repository is mocked.
   */
  @Test
  public void testQueryRepositoryBeyondLastPage() {
    saveRelease("1.11.0", TELEGRAF, singletonMap("os", "linux"));

    final Page<AgentRelease> page = agentReleaseRepository.findAll(PageRequest.of(500, 5));
    assertThat(page.getTotalElements()).isEqualTo(1);
    assertThat(page.getContent()).isEmpty();
    assertThat(page.getTotalPages()).isEqualTo(1);
    assertThat(page.getNumber()).isEqualTo(500);
  }

  @Test
  public void testDelete_present() {
    final AgentRelease release = saveRelease("1.11.0", TELEGRAF, singletonMap("os", "linux"));

    agentReleaseService.delete(release.getId());

    final Optional<AgentRelease> result = agentReleaseRepository.findById(release.getId());
    assertThat(result).isNotPresent();
  }

  @Test
  public void testDelete_stillReferenced() {
    final AgentRelease release = saveRelease("1.11.0", TELEGRAF, singletonMap("os", "linux"));

    saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    assertThatThrownBy(() -> {
      agentReleaseService.delete(release.getId());
    }).isInstanceOf(DataIntegrityViolationException.class);

    final Optional<AgentRelease> result = agentReleaseRepository.findById(release.getId());
    assertThat(result).isPresent();
  }

  private AgentRelease saveRelease(String v, AgentType agentType, Map<String, String> labels) {
    return agentReleaseRepository.save(
        new AgentRelease()
            .setType(agentType).setVersion(v).setUrl("").setExe("").setLabels(labels)
    );
  }

  @After
  public void tearDown() throws Exception {
    agentInstallRepository.deleteAll();
    agentReleaseRepository.deleteAll();
  }

  private AgentInstall saveInstall(AgentRelease release, String tenantId, LabelSelectorMethod labelSelectorMethod,
                                   String... labelPairs) {
    final Map<String, String> labelSelector = new HashMap<>();
    for (int i = 2; i <= labelPairs.length; i += 2) {
      labelSelector.put(labelPairs[i - 2], labelPairs[i - 1]);
    }

    return agentInstallRepository.save(
        new AgentInstall()
            .setAgentRelease(release)
            .setTenantId(tenantId)
            .setLabelSelector(labelSelector)
            .setLabelSelectorMethod(labelSelectorMethod)
    );
  }

}