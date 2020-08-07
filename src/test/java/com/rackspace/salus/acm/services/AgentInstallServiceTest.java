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

import static com.rackspace.salus.telemetry.model.AgentType.FILEBEAT;
import static com.rackspace.salus.telemetry.model.AgentType.TELEGRAF;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.salus.acm.web.model.AgentInstallCreate;
import com.rackspace.salus.resource_management.web.client.ResourceApi;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.telemetry.entities.AgentInstall;
import com.rackspace.salus.telemetry.entities.AgentRelease;
import com.rackspace.salus.telemetry.entities.BoundAgentInstall;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import com.rackspace.salus.telemetry.messaging.OperationType;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.model.LabelSelectorMethod;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.repositories.AgentInstallRepository;
import com.rackspace.salus.telemetry.repositories.AgentReleaseRepository;
import com.rackspace.salus.telemetry.repositories.BoundAgentInstallRepository;
import com.rackspace.salus.telemetry.repositories.TenantMetadataRepository;
import com.rackspace.salus.test.EnableTestContainersDatabase;
import java.net.ConnectException;
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
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.ResourceAccessException;

@SuppressWarnings("OptionalGetWithoutIsPresent")
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
public class AgentInstallServiceTest {

  @MockBean
  ResourceApi resourceApi;

  @MockBean
  BoundEventSender boundEventSender;

  @MockBean
  KafkaTemplate kafkaTemplate;

  @MockBean
  ResourceEventListener resourceEventListener;

  @MockBean
  TenantMetadataRepository tenantMetadataRepository;

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
  public void testGetInstallsFromLabels() {

    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);
    final AgentRelease release2 = saveRelease("2.0.0", TELEGRAF);
    final AgentRelease release3 = saveRelease("3.0.0", TELEGRAF);
    final AgentRelease release4 = saveRelease("4.0.0", TELEGRAF);

    final AgentInstall install1 = saveInstall(
        release1, "t-1", LabelSelectorMethod.AND, "os", "linux", "cluster", "prod");
    final AgentInstall install2 = saveInstall(
        release2, "t-1", LabelSelectorMethod.AND, "os", "linux", "cluster", "prod");
    final AgentInstall install3 = saveInstall(
        release3, "t-1", LabelSelectorMethod.AND, "os", "linux", "cluster", "prod");
    // different cluster
    final AgentInstall install4 = saveInstall(
        release4, "t-1", LabelSelectorMethod.AND, "os", "linux", "cluster", "dev");
    // different tenant
    final AgentInstall install5 = saveInstall(
        release1, "t-2", LabelSelectorMethod.AND, "os", "linux", "cluster", "prod");
    // different LabelSelectorMethod - same labels
    final AgentInstall install6 = saveInstall(
        release1, "t-1", LabelSelectorMethod.OR, "os", "windows", "cluster", "prod");
    // different labelSelectorMethod - no matching labels
    final AgentInstall install7 = saveInstall(
        release1, "t-1", LabelSelectorMethod.OR, "os", "windows", "cluster", "staging");
    // different labelSelectorMethod - only one label
    final AgentInstall install8 = saveInstall(
        release1, "t-1", LabelSelectorMethod.OR, "os", "linux");
    // Empty Label Selector on the install should match everything for this tenant
    final AgentInstall install9 = saveInstall(
        release1, "t-1", LabelSelectorMethod.OR);
    {
      // typical case
      Map<String, String> resourceLabels = new HashMap<>();
      resourceLabels.put("os", "linux");
      resourceLabels.put("arch", "x64");
      resourceLabels.put("cluster", "prod");

      final List<AgentInstall> matches = agentInstallService
          .getInstallsFromResourceLabels("t-1", resourceLabels);

      final List<UUID> installIds = matches.stream()
          .map(AgentInstall::getId)
          .collect(Collectors.toList());
      assertThat(installIds).containsExactlyInAnyOrder(
          install1.getId(), install2.getId(), install3.getId(), install6.getId(), install8.getId(), install9.getId()
      );
    }

    {
      // other tenant
      Map<String, String> resourceLabels = new HashMap<>();
      resourceLabels.put("os", "linux");
      resourceLabels.put("arch", "x64");
      resourceLabels.put("cluster", "prod");

      final List<AgentInstall> matches = agentInstallService
          .getInstallsFromResourceLabels("t-2", resourceLabels);

      final List<UUID> installIds = matches.stream()
          .map(AgentInstall::getId)
          .collect(Collectors.toList());
      assertThat(installIds).containsExactlyInAnyOrder(
          install5.getId()
      );
    }

    {
      // other cluster tag
      Map<String, String> resourceLabels = new HashMap<>();
      resourceLabels.put("os", "linux");
      resourceLabels.put("arch", "x64");
      resourceLabels.put("cluster", "dev");

      final List<AgentInstall> matches = agentInstallService
          .getInstallsFromResourceLabels("t-1", resourceLabels);

      final List<UUID> installIds = matches.stream()
          .map(AgentInstall::getId)
          .collect(Collectors.toList());
      assertThat(installIds).containsExactlyInAnyOrder(
          install4.getId(), install8.getId(), install9.getId()
      );
    }

    {
      // equal label count
      Map<String, String> resourceLabels = new HashMap<>();
      resourceLabels.put("os", "linux");
      resourceLabels.put("cluster", "prod");

      final List<AgentInstall> matches = agentInstallService
          .getInstallsFromResourceLabels("t-1", resourceLabels);

      final List<UUID> installIds = matches.stream()
          .map(AgentInstall::getId)
          .collect(Collectors.toList());
      assertThat(installIds).containsExactlyInAnyOrder(
          install1.getId(), install2.getId(), install3.getId(), install6.getId(), install8.getId(), install9.getId()
      );
    }

    {
      // unknown tenant
      Map<String, String> resourceLabels = new HashMap<>();
      resourceLabels.put("os", "linux");
      resourceLabels.put("arch", "x64");
      resourceLabels.put("cluster", "prod");

      final List<AgentInstall> matches = agentInstallService
          .getInstallsFromResourceLabels("t-other", resourceLabels);

      assertThat(matches).isEmpty();
    }

    {
      // selectors broader than resource
      Map<String, String> resourceLabels = new HashMap<>();
      resourceLabels.put("os", "linux");

      final List<AgentInstall> matches = agentInstallService
          .getInstallsFromResourceLabels("t-1", resourceLabels);

      final List<UUID> installIds = matches.stream()
          .map(AgentInstall::getId)
          .collect(Collectors.toList());
      assertThat(installIds).containsExactlyInAnyOrder(
          install8.getId(), install9.getId()
      );
    }

    {
      // empty resource labels should only get installs with no labels
      Map<String, String> resourceLabels = Collections.emptyMap();

      final List<AgentInstall> matches = agentInstallService
          .getInstallsFromResourceLabels("t-1", resourceLabels);

      final List<UUID> installIds = matches.stream()
          .map(AgentInstall::getId)
          .collect(Collectors.toList());
      assertThat(installIds).containsExactlyInAnyOrder(
          install9.getId()
      );
    }
  }

  @Test
  public void testDefaultLabelSelectorMethod() {
    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);

    Map<String, String> labelSelector = new HashMap();
    labelSelector.put("os", "windows");
    labelSelector.put("cluster", "prod");
    agentInstallRepository.save(
        new AgentInstall()
            .setAgentRelease(release1)
            .setTenantId("t-1")
            .setLabelSelector(labelSelector)
    );

    List<AgentInstall> savedInstalls = agentInstallService.getInstallsFromResourceLabels("t-1", labelSelector);
    assertThat(savedInstalls.size()).isEqualTo(1);
    assertThat(savedInstalls.get(0).getLabelSelectorMethod()).isEqualTo(LabelSelectorMethod.AND);
  }

  @Test
  public void testInstall_noPrior_multiResource() {
    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);

    when(resourceApi.getResourcesWithLabels(eq("t-1"), any(), eq(LabelSelectorMethod.AND)))
        .thenReturn(Arrays.asList(
            new ResourceDTO().setTenantId("t-1").setResourceId("r-1"),
            new ResourceDTO().setTenantId("t-1").setResourceId("r-2")
        ));

    // EXECUTE

    final Map<String, String> labelSelector = Collections.singletonMap("os", "linux");
    final AgentInstall install = agentInstallService.install(
        "t-1",
        new AgentInstallCreate()
            .setAgentReleaseId(release1.getId())
            .setLabelSelector(labelSelector)
            .setLabelSelectorMethod(LabelSelectorMethod.AND)
    );

    // VERIFY

    assertThat(install.getId()).isNotNull();
    assertThat(install.getAgentRelease().getId()).isEqualTo(release1.getId());

    final Optional<AgentInstall> saved = agentInstallRepository.findById(install.getId());
    assertThat(saved).isPresent();
    assertThat(saved.get().getId()).isEqualTo(install.getId());
    assertThat(saved.get().getTenantId()).isEqualTo("t-1");
    assertThat(saved.get().getAgentRelease().getId()).isEqualTo(release1.getId());

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    final List<String> boundResourceIds = new ArrayList<>();
    bindings.forEach(binding -> boundResourceIds.add(binding.getResourceId()));
    assertThat(boundResourceIds).containsExactlyInAnyOrder("r-1", "r-2");

    verify(resourceApi).getResourcesWithLabels("t-1", labelSelector, LabelSelectorMethod.AND);

    verify(boundEventSender)
        .sendTo(eq(OperationType.UPSERT), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue())
        .containsExactlyInAnyOrder(
            new TenantResource("t-1", "r-1"),
            new TenantResource("t-1", "r-2")
        );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testInstall_noPrior_noneSelected() {
    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);

    when(resourceApi.getResourcesWithLabels(eq("t-1"), any(), eq(LabelSelectorMethod.AND)))
        .thenReturn(Collections.emptyList());

    // EXECUTE

    final Map<String, String> labelSelector = Collections.singletonMap("os", "linux");
    final AgentInstall install = agentInstallService.install(
        "t-1",
        new AgentInstallCreate()
            .setAgentReleaseId(release1.getId())
            .setLabelSelector(labelSelector)
            .setLabelSelectorMethod(LabelSelectorMethod.AND)
    );

    // VERIFY

    assertThat(install.getId()).isNotNull();
    assertThat(install.getAgentRelease().getId()).isEqualTo(release1.getId());

    final Optional<AgentInstall> saved = agentInstallRepository.findById(install.getId());
    assertThat(saved).isPresent();
    assertThat(saved.get().getId()).isEqualTo(install.getId());
    assertThat(saved.get().getTenantId()).isEqualTo("t-1");
    assertThat(saved.get().getAgentRelease().getId()).isEqualTo(release1.getId());

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).isEmpty();

    verify(resourceApi).getResourcesWithLabels("t-1", labelSelector, LabelSelectorMethod.AND);

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testInstall_priorOlder() {
    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);
    final AgentRelease release2 = saveRelease("2.0.0", TELEGRAF);

    when(resourceApi.getResourcesWithLabels(eq("t-1"), any(), eq(LabelSelectorMethod.AND)))
        .thenReturn(Collections.singletonList(
            new ResourceDTO().setTenantId("t-1").setResourceId("r-1")
        ));

    final AgentInstall priorInstall = saveInstall(release1, "t-1", LabelSelectorMethod.AND, "os", "linux");

    saveBinding(priorInstall, "r-1");

    // EXECUTE

    final Map<String, String> labelSelector = Collections.singletonMap("os", "linux");
    final AgentInstall agentInstall = agentInstallService.install(
        "t-1",
        new AgentInstallCreate()
            .setAgentReleaseId(release2.getId())
            .setLabelSelector(labelSelector)
            .setLabelSelectorMethod(LabelSelectorMethod.AND)
    );

    // VERIFY

    assertThat(agentInstall.getId()).isNotNull();
    assertThat(agentInstall.getAgentRelease().getId()).isEqualTo(release2.getId());

    final Optional<AgentInstall> saved = agentInstallRepository.findById(agentInstall.getId());
    assertThat(saved).isPresent();
    assertThat(saved.get().getId()).isEqualTo(agentInstall.getId());
    assertThat(saved.get().getTenantId()).isEqualTo("t-1");
    assertThat(saved.get().getAgentRelease().getId()).isEqualTo(release2.getId());

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(agentInstall.getId());
    assertThat(savedBinding.getAgentInstall().getAgentRelease()).isEqualToIgnoringGivenFields(release2,
        "createdTimestamp", "updatedTimestamp");

    verify(resourceApi).getResourcesWithLabels("t-1", labelSelector, LabelSelectorMethod.AND);

    verify(boundEventSender)
        .sendTo(eq(OperationType.UPSERT), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue())
        .containsExactlyInAnyOrder(
            new TenantResource("t-1", "r-1")
        );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testInstall_priorNewer() {
    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);
    final AgentRelease release2 = saveRelease("2.0.0", TELEGRAF);

    when(resourceApi.getResourcesWithLabels(eq("t-1"), any(), eq(LabelSelectorMethod.AND)))
        .thenReturn(Collections.singletonList(
            new ResourceDTO().setTenantId("t-1").setResourceId("r-1")
        ));

    // prior install is release 2.0.0
    final AgentInstall priorInstall = saveInstall(release2, "t-1", LabelSelectorMethod.AND, "os", "linux");

    final BoundAgentInstall binding = saveBinding(priorInstall, "r-1");

    // EXECUTE

    final Map<String, String> labelSelector = Collections.singletonMap("os", "linux");
    final AgentInstall agentInstall = agentInstallService.install(
        "t-1",
        new AgentInstallCreate()
            // but user requested 1.0.0
            .setAgentReleaseId(release1.getId())
            .setLabelSelector(labelSelector)
            .setLabelSelectorMethod(LabelSelectorMethod.AND)
    );

    // VERIFY

    assertThat(agentInstall.getId()).isNotNull();
    assertThat(agentInstall.getAgentRelease().getId()).isEqualTo(release1.getId());

    final Optional<AgentInstall> saved = agentInstallRepository.findById(agentInstall.getId());
    assertThat(saved).isPresent();
    assertThat(saved.get().getId()).isEqualTo(agentInstall.getId());
    assertThat(saved.get().getTenantId()).isEqualTo("t-1");
    assertThat(saved.get().getAgentRelease().getId()).isEqualTo(release1.getId());

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    // should find prior installation
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(priorInstall.getId());
    assertThat(savedBinding.getAgentInstall().getAgentRelease()).isEqualToIgnoringGivenFields(release2,
        "createdTimestamp", "updatedTimestamp");

    verify(resourceApi).getResourcesWithLabels("t-1", labelSelector, LabelSelectorMethod.AND);

    // no bound events sent, since prior install still applies

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testInstall_priorMultipleNewer() {
    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);
    final AgentRelease release2 = saveRelease("2.0.0", TELEGRAF);
    final AgentRelease release3 = saveRelease("3.0.0", TELEGRAF);

    when(resourceApi.getResourcesWithLabels(eq("t-1"), any(), eq(LabelSelectorMethod.AND)))
        .thenReturn(Collections.singletonList(
            new ResourceDTO().setTenantId("t-1").setResourceId("r-1")
        ));

    final AgentInstall install2 = saveInstall(release2, "t-1", LabelSelectorMethod.AND, "os", "linux");
    final AgentInstall install3 = saveInstall(release3, "t-1", LabelSelectorMethod.AND, "os", "linux");

    // simulate multiple bindings to resource...which really shouldn't ever happen, but
    // service can cleanup this case
    saveBinding(install2, "r-1");
    saveBinding(install3, "r-1");

    // EXECUTE

    final Map<String, String> labelSelector = Collections.singletonMap("os", "linux");
    final AgentInstall agentInstall = agentInstallService.install(
        "t-1",
        new AgentInstallCreate()
            // but user requested 1.0.0
            .setAgentReleaseId(release1.getId())
            .setLabelSelector(labelSelector)
            .setLabelSelectorMethod(LabelSelectorMethod.AND)
    );

    // VERIFY

    assertThat(agentInstall.getId()).isNotNull();
    assertThat(agentInstall.getAgentRelease().getId()).isEqualTo(release1.getId());

    // install saved as normal
    final Optional<AgentInstall> saved = agentInstallRepository.findById(agentInstall.getId());
    assertThat(saved).isPresent();
    assertThat(saved.get().getId()).isEqualTo(agentInstall.getId());
    assertThat(saved.get().getTenantId()).isEqualTo("t-1");
    assertThat(saved.get().getAgentRelease().getId()).isEqualTo(release1.getId());

    // but only the newest binding should remain...even binding of install2 should have been pruned
    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    // should find prior installation
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(install3.getId());
    assertThat(savedBinding.getAgentInstall().getAgentRelease()).isEqualToIgnoringGivenFields(release3,
        "createdTimestamp", "updatedTimestamp");

    verify(resourceApi).getResourcesWithLabels("t-1", labelSelector, LabelSelectorMethod.AND);

    // no bound events sent, since prior install still applies

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testInstall_alreadyExists() {
    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);

    when(resourceApi.getResourcesWithLabels(eq("t-1"), any(), eq(LabelSelectorMethod.AND)))
        .thenReturn(Collections.singletonList(
            new ResourceDTO().setTenantId("t-1").setResourceId("r-1")
        ));

    final AgentInstall install1 = saveInstall(release1, "t-1", LabelSelectorMethod.AND, "os", "linux");

    // EXECUTE

    assertThatThrownBy(() -> {
      final Map<String, String> labelSelector = Collections.singletonMap("os", "linux");
      agentInstallService.install(
          "t-1",
          new AgentInstallCreate()
              .setAgentReleaseId(release1.getId())
              .setLabelSelector(labelSelector)
      );
    })
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("AgentInstall with same release and label selector exists");

    // VERIFY

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testInstall_resourceManagementDown() {
    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);

    when(resourceApi.getResourcesWithLabels(eq("t-1"), any(), eq(LabelSelectorMethod.AND)))
        .thenThrow(new ResourceAccessException("I/O error on GET request", new ConnectException()));

    // EXECUTE
    final Map<String, String> labelSelector = Collections.singletonMap("os", "linux");

    assertThatThrownBy(() -> {
        agentInstallService.install(
            "t-1",
            new AgentInstallCreate()
                .setAgentReleaseId(release1.getId())
                .setLabelSelector(labelSelector)
                .setLabelSelectorMethod(LabelSelectorMethod.AND)
        );
      })
        .isInstanceOf(ResourceAccessException.class);

    // VERIFY

    // should have rolled back any DB row creation when binding threw exception
    assertThat(agentInstallRepository.findAll()).isEmpty();

    verify(resourceApi).getResourcesWithLabels("t-1", labelSelector, LabelSelectorMethod.AND);

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testDelete_notFound() {
    // EXECUTE
    final UUID id = UUID.randomUUID();
    try {
      agentInstallService.delete("t-1", id);
      fail("Should have thrown NotFoundException");
    } catch (NotFoundException e) {
      assertThat(e.getMessage()).isEqualTo(
          String.format("No install found for %s on tenant t-1", id));
    }

    // VERIFY
    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testDelete_wrongTenant() {
    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-other", LabelSelectorMethod.AND, "os", "linux");

    // EXECUTE

    try {
      agentInstallService.delete("t-wrong", install.getId());
      fail("Should have thrown NotFoundException");
    } catch (NotFoundException e) {
      assertThat(e.getMessage()).isEqualTo(
          String.format("No install found for %s on tenant t-wrong", install.getId()));
    }

    // VERIFY
    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testDelete_noBindings() {
    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    // EXECUTE
    agentInstallService.delete("t-1", install.getId());

    // VERIFY

    final Optional<AgentInstall> saved = agentInstallRepository.findById(install.getId());
    assertThat(saved).isNotPresent();

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).isEmpty();

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testDelete_withBindings() {
    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    saveBinding(install, "r-1");
    saveBinding(install, "r-2");

    // EXECUTE
    agentInstallService.delete("t-1", install.getId());

    // VERIFY

    final Optional<AgentInstall> saved = agentInstallRepository.findById(install.getId());
    assertThat(saved).isNotPresent();

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).isEmpty();

    verify(boundEventSender)
        .sendTo(eq(OperationType.DELETE), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue())
        .containsExactlyInAnyOrder(
            new TenantResource("t-1", "r-1"),
            new TenantResource("t-1", "r-2")
        );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testFindBoundAgentTypesByResource() {
    final AgentRelease releaseT1 = saveRelease("1.0.0", TELEGRAF);
    final AgentRelease releaseT2 = saveRelease("2.0.0", TELEGRAF);
    final AgentRelease releaseF1 = saveRelease("1.0.0", FILEBEAT);

    final AgentInstall installT1 = saveInstall(releaseT1, "t-1", LabelSelectorMethod.AND, "os", "1");
    final AgentInstall installT2 = saveInstall(releaseT2, "t-1", LabelSelectorMethod.AND, "os", "2");
    final AgentInstall installF1 = saveInstall(releaseF1, "t-1", LabelSelectorMethod.AND, "os", "3");

    saveBinding(installT1, "r-1");
    saveBinding(installT2, "r-1");
    saveBinding(installF1, "r-1");

    saveBinding(installT1, "r-2");

    {
      // resource with multiple agents
      final List<AgentType> types = agentInstallService
          .findBoundAgentTypesByResource("t-1", "r-1");
      assertThat(types).containsExactlyInAnyOrder(TELEGRAF, FILEBEAT);
    }

    {
      // resource with one
      final List<AgentType> types = agentInstallService
          .findBoundAgentTypesByResource("t-1", "r-2");
      assertThat(types).containsExactlyInAnyOrder(TELEGRAF);
    }

    {
      // resource with none
      final List<AgentType> types = agentInstallService
          .findBoundAgentTypesByResource("t-1", "r-other");
      assertThat(types).isEmpty();
    }
  }

  @Test
  public void testHandleResourceEvent_resourceDoesNotExist() {
    when(resourceApi.getByResourceId(any(), any()))
        .thenReturn(null);

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-any")
            .setResourceId("r-any")
    );

    // VERIFY

    verify(resourceApi).getByResourceId("t-any", "r-any");

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_nonEnvoyResource() {
    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(false)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
        );

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-any")
            .setResourceId("r-not-envoy")
    );

    verify(resourceApi).getByResourceId("t-any", "r-not-envoy");

    // VERIFY
    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_labelsChanged_newMatch() {
    Map<String, String> resourceLabels = new HashMap<>();
    resourceLabels.put("os", "linux");
    resourceLabels.put("arch", "amd64");

    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(true)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
                .setLabels(resourceLabels)
        );

    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setLabelsChanged(true)
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(install.getId());

    verify(resourceApi).getByResourceId("t-1", "r-1");

    verify(boundEventSender).sendTo(eq(OperationType.UPSERT), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue()).containsExactlyInAnyOrder(
        new TenantResource("t-1", "r-1")
    );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_labelsChanged_newBindingandEnvoyReattach() {
    Map<String, String> resourceLabels = new HashMap<>();
    resourceLabels.put("os", "linux");
    resourceLabels.put("arch", "amd64");

    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(true)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
                .setLabels(resourceLabels)
        );

    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            // labels changed
            .setLabelsChanged(true)
            // ...AND envoy reattached
            .setReattachedEnvoyId("e-1")
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(install.getId());

    verify(resourceApi).getByResourceId("t-1", "r-1");

    verify(boundEventSender).sendTo(eq(OperationType.UPSERT), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue()).containsExactlyInAnyOrder(
        new TenantResource("t-1", "r-1")
    );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_labelsChanged_existingBindingandEnvoyReattach() {
    Map<String, String> resourceLabels = new HashMap<>();
    resourceLabels.put("os", "linux");
    resourceLabels.put("arch", "amd64");

    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(true)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
                .setLabels(resourceLabels)
        );

    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    saveBinding(install, "r-1");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            // labels changed
            .setLabelsChanged(true)
            // ...AND envoy reattached
            .setReattachedEnvoyId("e-1")
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(install.getId());

    verify(resourceApi).getByResourceId("t-1", "r-1");

    verify(boundEventSender).sendTo(eq(OperationType.UPSERT), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue()).containsExactlyInAnyOrder(
        new TenantResource("t-1", "r-1")
    );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @SuppressWarnings("unused")
  @Test
  public void testHandleResourceEvent_labelsChanged_newConflictingMatches() {
    Map<String, String> resourceLabels = new HashMap<>();
    resourceLabels.put("os", "linux");
    resourceLabels.put("arch", "amd64");

    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(true)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
                .setLabels(resourceLabels)
        );

    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);
    final AgentRelease release2 = saveRelease("2.0.0", TELEGRAF);

    // declare conflicting installs to ensure it picks the newer, install2
    final AgentInstall install1 = saveInstall(release1, "t-1", LabelSelectorMethod.AND, "os", "linux");
    final AgentInstall install2 = saveInstall(release2, "t-1", LabelSelectorMethod.AND, "os", "linux");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setLabelsChanged(true)
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    // picked newer?
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(install2.getId());

    verify(resourceApi).getByResourceId("t-1", "r-1");

    verify(boundEventSender).sendTo(eq(OperationType.UPSERT), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue()).containsExactlyInAnyOrder(
        new TenantResource("t-1", "r-1")
    );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @SuppressWarnings("unused")
  @Test
  public void testHandleResourceEvent_labelsChanged_existingAndLingeringMatches() {
    Map<String, String> resourceLabels = new HashMap<>();
    resourceLabels.put("os", "linux");
    resourceLabels.put("arch", "amd64");

    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(true)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
                .setLabels(resourceLabels)
        );

    final AgentRelease release1 = saveRelease("1.0.0", TELEGRAF);
    final AgentRelease release2 = saveRelease("2.0.0", TELEGRAF);

    // declare conflicting installs to ensure it picks the newer, install2
    final AgentInstall install1 = saveInstall(release1, "t-1", LabelSelectorMethod.AND, "os", "linux");
    final AgentInstall install2 = saveInstall(release2, "t-1", LabelSelectorMethod.AND, "os", "linux");

    // simulate extraneous binding
    saveBinding(install1, "r-1");
    saveBinding(install2, "r-1");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setLabelsChanged(true)
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    // kept only newer?
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(install2.getId());

    verify(resourceApi).getByResourceId("t-1", "r-1");

    // but no event was needed since binding already present

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_labelsChanged_sameBindings() {
    Map<String, String> resourceLabels = new HashMap<>();
    resourceLabels.put("os", "linux");
    resourceLabels.put("arch", "amd64");

    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(true)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
                .setLabels(resourceLabels)
        );

    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    saveBinding(install, "r-1");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setLabelsChanged(true)
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(install.getId());

    verify(resourceApi).getByResourceId("t-1", "r-1");

    // but no event was needed since binding already present

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_labelsChanged_resourceWithoutLabels() {
    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(true)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
                .setLabels(emptyMap())
        );

    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install1 = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    final AgentInstall install2 = saveInstall(release, "t-1", LabelSelectorMethod.AND);
    saveBinding(install2, "r-1");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setLabelsChanged(true)
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    // binding left alone?
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(install2.getId());

    verify(resourceApi).getByResourceId("t-1", "r-1");

    // but no event was needed since binding already present

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_labelsChanged_noLongerMatches() {
    Map<String, String> resourceLabels = new HashMap<>();
    resourceLabels.put("os", "windows");
    resourceLabels.put("arch", "amd64");

    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(true)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
                .setLabels(resourceLabels)
        );

    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    saveBinding(install, "r-1");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setLabelsChanged(true)
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).isEmpty();

    verify(resourceApi).getByResourceId("t-1", "r-1");

    verify(boundEventSender).sendTo(eq(OperationType.DELETE), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue()).containsExactlyInAnyOrder(
        new TenantResource("t-1", "r-1")
    );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_labelsChanged_noneMatch() {
    Map<String, String> resourceLabels = new HashMap<>();
    resourceLabels.put("os", "windows");
    resourceLabels.put("arch", "amd64");

    when(resourceApi.getByResourceId(any(), any()))
        .then(invocationOnMock ->
            new ResourceDTO()
                .setAssociatedWithEnvoy(true)
                .setTenantId(invocationOnMock.getArgument(0))
                .setResourceId(invocationOnMock.getArgument(1))
                .setLabels(resourceLabels)
        );

    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setLabelsChanged(true)
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).isEmpty();

    verify(resourceApi).getByResourceId("t-1", "r-1");

    // no binding event since no match

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_deleted() {
    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    saveBinding(install, "r-1");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setDeleted(true)
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).isEmpty();

    verify(boundEventSender).sendTo(eq(OperationType.DELETE), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue()).containsExactlyInAnyOrder(
        new TenantResource("t-1", "r-1")
    );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  @Test
  public void testHandleResourceEvent_reattach() {
    final AgentRelease release = saveRelease("1.0.0", TELEGRAF);

    final AgentInstall install = saveInstall(release, "t-1", LabelSelectorMethod.AND, "os", "linux");

    saveBinding(install, "r-1");

    // EXECUTE
    agentInstallService.handleResourceEvent(
        new ResourceEvent()
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setReattachedEnvoyId("e-1")
    );

    // VERIFY

    final Iterable<BoundAgentInstall> bindings = boundAgentInstallRepository.findAll();
    assertThat(bindings).hasSize(1);
    final BoundAgentInstall savedBinding = bindings.iterator().next();
    assertThat(savedBinding.getResourceId()).isEqualTo("r-1");
    assertThat(savedBinding.getAgentInstall().getId()).isEqualTo(install.getId());

    verify(boundEventSender).sendTo(eq(OperationType.UPSERT), eq(TELEGRAF), tenantResourcesArg.capture());
    assertThat(tenantResourcesArg.getValue()).containsExactlyInAnyOrder(
        new TenantResource("t-1", "r-1")
    );

    verifyNoMoreInteractions(boundEventSender, resourceApi);
  }

  private AgentRelease saveRelease(String v, AgentType agentType) {
    return agentReleaseRepository.save(
        new AgentRelease()
            .setType(agentType).setVersion(v).setUrl("").setExe("").setLabels(emptyMap())
    );
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

  private BoundAgentInstall saveBinding(AgentInstall install, String resourceId) {
    return boundAgentInstallRepository.save(
        new BoundAgentInstall()
            .setAgentInstall(install)
            .setResourceId(resourceId)
    );
  }

}