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

import com.github.zafarkhaja.semver.Version;
import com.rackspace.salus.acm.entities.AgentInstall;
import com.rackspace.salus.acm.entities.AgentRelease;
import com.rackspace.salus.acm.entities.BoundAgentInstall;
import com.rackspace.salus.acm.repositories.AgentInstallRepository;
import com.rackspace.salus.acm.repositories.AgentReleaseRepository;
import com.rackspace.salus.acm.repositories.BoundAgentInstallRepository;
import com.rackspace.salus.acm.web.model.AgentInstallCreate;
import com.rackspace.salus.acm.web.model.AgentInstallDTO;
import com.rackspace.salus.common.transactions.EnableJpaKafkaTransactions;
import com.rackspace.salus.resource_management.web.client.ResourceApi;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import com.rackspace.salus.telemetry.messaging.OperationType;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.model.NotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;

@Service
@Slf4j
@EnableJpaKafkaTransactions
public class AgentInstallService {

  private final JdbcTemplate jdbcTemplate;
  private final EntityManager em;
  private final AgentReleaseRepository agentReleaseRepository;
  private final AgentInstallRepository agentInstallRepository;
  private final BoundAgentInstallRepository boundAgentInstallRepository;
  private final ResourceApi resourceApi;
  private final BoundEventSender boundEventSender;

  @Autowired
  public AgentInstallService(JdbcTemplate jdbcTemplate,
                             EntityManager entityManager,
                             AgentReleaseRepository agentReleaseRepository,
                             AgentInstallRepository agentInstallRepository,
                             BoundAgentInstallRepository boundAgentInstallRepository,
                             ResourceApi resourceApi,
                             BoundEventSender boundEventSender) {
    this.jdbcTemplate = jdbcTemplate;
    this.em = entityManager;
    this.agentReleaseRepository = agentReleaseRepository;
    this.agentInstallRepository = agentInstallRepository;
    this.boundAgentInstallRepository = boundAgentInstallRepository;
    this.resourceApi = resourceApi;
    this.boundEventSender = boundEventSender;
  }

  @Transactional(value="jpaKafkaTransactionManager")
  public AgentInstallDTO install(String tenantId, AgentInstallCreate in) {
    Assert.notNull(tenantId, "tenantId is required");

    log.debug("Creating install={} for tenant={}", in, tenantId);

    final AgentRelease agentRelease = agentReleaseRepository.findById(in.getAgentReleaseId())
        .orElseThrow(() -> new NotFoundException("Could not find associated agent release"));

    final List<AgentInstall> existing = agentInstallRepository
        .findAllByTenantIdAndAgentRelease_Id(tenantId, in.getAgentReleaseId());

    if (existing.stream()
        .anyMatch(agentInstall -> agentInstall.getLabelSelector().equals(in.getLabelSelector()))) {
      throw new AlreadyExistsException("AgentInstall with same release and label selector exists");
    }

    final AgentInstall agentInstall = new AgentInstall()
        .setAgentRelease(agentRelease)
        .setLabelSelector(in.getLabelSelector())
        .setTenantId(tenantId);

    final AgentInstall saved = agentInstallRepository.saveAndFlush(agentInstall);

    bindInstallToResources(saved);

    log.info("Created agentInstall={}", saved);
    return saved.toDTO();
  }

  @Transactional(value="jpaKafkaTransactionManager")
  public void delete(String tenantId, UUID agentInstallId) {
    final AgentInstall agentInstall = agentInstallRepository.findByIdAndTenantId(agentInstallId, tenantId)
        .orElseThrow(() ->
            new NotFoundException(
                String.format("No install found for %s on tenant %s", agentInstallId, tenantId)));

    final List<BoundAgentInstall> bound = boundAgentInstallRepository
        .findAllByAgentInstall_Id(agentInstallId);

    final List<TenantResource> affectedResourceIds = bound.stream()
        .map(BoundAgentInstall::getResourceId)
        .distinct()
        .map(resourceId -> new TenantResource(tenantId,resourceId))
        .collect(Collectors.toList());

    boundAgentInstallRepository.deleteAll(bound);
    agentInstallRepository.delete(agentInstall);

    log.info("Deleted agentInstall={}", agentInstall);

    if (!affectedResourceIds.isEmpty()) {
      boundEventSender.sendTo(
          OperationType.DELETE, agentInstall.getAgentRelease().getType(), affectedResourceIds);
    }
  }

  void handleResourceEvent(ResourceEvent resourceEvent) {
    log.debug("Handling resourceEvent={}", resourceEvent);

    final boolean reattached = resourceEvent.getReattachedEnvoyId() != null;

    // Evaluate event-scope actions
    if (resourceEvent.isDeleted()) {
      unbindDeletedResource(resourceEvent.getTenantId(), resourceEvent.getResourceId());
    } else if (!resourceEvent.isLabelsChanged() && reattached) {
      handleReattachedEnvoy(resourceEvent.getTenantId(), resourceEvent.getResourceId());
    } else {
      // ...further evaluate actions that require resource lookup

      final ResourceDTO resource = resourceApi
          .getByResourceId(resourceEvent.getTenantId(), resourceEvent.getResourceId());

      if (resource == null) {
        log.warn("Unable to locate resource from event={}", resourceEvent);
      } else if (!resource.isAssociatedWithEnvoy()) {
        log.debug("Ignoring event={} since resource is not associated with envoy", resourceEvent);
      } else if (resourceEvent.isLabelsChanged()) {
        updateBindingToChangedResource(resource, reattached);
      }
      else {
        log.debug("Ignoring event={} due to non-relevant change", resourceEvent);
      }
    }
  }

  List<AgentInstall> getInstallsFromLabels(String tenantId, Map<String, String> labels)
      throws IllegalArgumentException {
    if (labels.size() == 0) {
      throw new IllegalArgumentException("Labels must be provided for search");
    }

    MapSqlParameterSource paramSource = new MapSqlParameterSource();
    paramSource.addValue("tenantId", tenantId);
    StringBuilder builder = new StringBuilder(
        "SELECT agent_installs.id FROM agent_installs JOIN agent_install_label_selectors AS ml WHERE agent_installs.id = ml.agent_install_id AND agent_installs.id IN ");
    builder.append(
        "(SELECT agent_install_id from agent_install_label_selectors WHERE agent_installs.id IN (SELECT id FROM agent_installs WHERE tenant_id = :tenantId) AND ");
    builder.append(
        "agent_installs.id IN (SELECT search_labels.agent_install_id FROM (SELECT agent_install_id, COUNT(*) AS count FROM agent_install_label_selectors GROUP BY agent_install_id) AS total_labels JOIN (SELECT agent_install_id, COUNT(*) AS count FROM agent_install_label_selectors WHERE ");
    int i = 0;
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      if (i > 0) {
        builder.append(" OR ");
      }
      //noinspection StringConcatenationInsideStringBufferAppend
      builder
          .append("(label_selector = :label" + i + " AND label_selector_key = :labelKey" + i + ")");
      paramSource.addValue("label" + i, entry.getValue());
      paramSource.addValue("labelKey" + i, entry.getKey());
      i++;
    }
    builder.append(
        " GROUP BY agent_install_id) AS search_labels WHERE total_labels.agent_install_id = search_labels.agent_install_id AND search_labels.count >= total_labels.count GROUP BY search_labels.agent_install_id)");

    builder.append(") ORDER BY agent_installs.id");
    paramSource.addValue("i", i);

    @SuppressWarnings("ConstantConditions")
    NamedParameterJdbcTemplate namedParameterTemplate = new NamedParameterJdbcTemplate(
        jdbcTemplate.getDataSource());
    final List<UUID> monitorIds = namedParameterTemplate.query(builder.toString(), paramSource,
        (resultSet, rowIndex) -> UUID.fromString(resultSet.getString(1))
    );

    // use JPA to retrieve and resolve the entities and then convert Iterable result to list
    final ArrayList<AgentInstall> results = new ArrayList<>();
    for (AgentInstall agentInstall : agentInstallRepository.findAllById(monitorIds)) {
      results.add(agentInstall);
    }

    return results;
  }

  private void bindInstallToResources(AgentInstall agentInstall) {
    final List<ResourceDTO> resources = resourceApi
        .getResourcesWithLabels(agentInstall.getTenantId(), agentInstall.getLabelSelector());

    log.debug("Found resources={} matching selector of agentInstall={}", resources, agentInstall);

    final List<BoundAgentInstall> newBindings = resources.stream()
        .map(resourceDTO -> new BoundAgentInstall()
            .setAgentInstall(agentInstall)
            .setResourceId(resourceDTO.getResourceId())
        )
        .collect(Collectors.toList());

    final List<TenantResource> affectedResources = saveNewBindings(newBindings);

    if (!affectedResources.isEmpty()) {
      boundEventSender.sendTo(
          OperationType.UPSERT, agentInstall.getAgentRelease().getType(), affectedResources);
    }
  }

  private List<TenantResource> saveNewBindings(List<BoundAgentInstall> newBindings) {
    final List<BoundAgentInstall> bindingsToSave = new ArrayList<>(newBindings.size());

    for (BoundAgentInstall newBinding : newBindings) {
      if (reconcileBinding(newBinding)) {
        bindingsToSave.add(newBinding);
      }
    }

    log.debug("Reconciled newBindings={} into={}", newBindings, bindingsToSave);

    boundAgentInstallRepository.saveAll(bindingsToSave);

    return bindingsToSave.stream()
        .map(boundAgentInstall ->
            new TenantResource(
                boundAgentInstall.getAgentInstall().getTenantId(),
                boundAgentInstall.getResourceId()
            ))
        .collect(Collectors.toList());
  }

  /**
   * Given a not-yet-saved binding, locates any existing bindings for the same
   * tenant-resource-agentType and determines if the given one is newest and should
   * be saved (and existing unbound) or given is not newest and shouldn't be saved.
   * @return true if the given binding should be saved and older bindings were deleted;
   * false if this binding should be ignored
   */
  private boolean reconcileBinding(BoundAgentInstall binding) {

    final List<BoundAgentInstall> others = boundAgentInstallRepository
        .findAllByTenantResourceAgentType(
            binding.getAgentInstall().getTenantId(),
            binding.getResourceId(),
            binding.getAgentInstall().getAgentRelease().getType()
        );

    if (others.isEmpty()) {
      return true;
    }

    log.debug("Reconciling binding={} against existing={}", binding, others);

    final Version ourVersion = versionOf(binding);

    final List<BoundAgentInstall> othersSorted = new ArrayList<>(others);
    othersSorted.sort(Comparator.comparing(AgentInstallService::versionOf));

    final boolean keepOurs = ourVersion.greaterThan(
        versionOf(othersSorted.get(0))
    );

    if (keepOurs) {
      // delete all of the others since they're all older
      boundAgentInstallRepository.deleteAll(others);
    }
    else if (othersSorted.size() > 1) {
      // in case there were overlaps present,
      // delete all but the newest version which is last due to sorting above
      boundAgentInstallRepository.deleteAll(
          othersSorted.subList(0, othersSorted.size()-1)
      );
    }

    return keepOurs;
  }

  private void updateBindingToChangedResource(ResourceDTO resource, boolean reattached) {
    log.debug("Updating bindings to changed resource={} with reattached={}",
        resource, reattached);

    // When querying DB we'll get a potential mixture of agent types and older/current installs
    // ...so group them first by agent type
    final LinkedMultiValueMap<AgentType, AgentInstall> grouped = new LinkedMultiValueMap<>();
    for (AgentInstall agentInstall :
        getInstallsFromLabels(resource.getTenantId(), resource.getLabels())) {
      grouped.add(agentInstall.getAgentRelease().getType(), agentInstall);
    }

    final Map<AgentType, AgentInstall> newestInstalls =
        grouped.entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty())
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    entry ->
                        entry.getValue().stream()
                            .max(Comparator.comparing(AgentInstallService::versionOf))
                            .get()
                ));

    // Before upserting new bindings, remove any bindings that no longer apply to this resource
    // and a specific agent release type
    final List<AgentType> allBoundTypes = findBoundAgentTypesByResource(
        resource.getTenantId(), resource.getResourceId());

    for (AgentType priorBoundAgentType : allBoundTypes) {
      if (!newestInstalls.containsKey(priorBoundAgentType)) {
        unbindByAgentType(resource, priorBoundAgentType);
      }
    }

    // Upsert new bindings
    for (Entry<AgentType, AgentInstall> entry : newestInstalls.entrySet()) {
      upsertBindingToResource(resource, entry.getKey(), entry.getValue(), reattached);
    }
  }

  private void unbindByAgentType(ResourceDTO resource, AgentType agentType) {
    log.debug("Unbinding installs of agentType={} from resource={}", agentType, resource);

    final String tenantId = resource.getTenantId();
    final String resourceId = resource.getResourceId();

    final List<BoundAgentInstall> bindings = boundAgentInstallRepository
        .findAllByTenantResourceAgentType(tenantId, resourceId, agentType);

    boundAgentInstallRepository.deleteAll(bindings);

    boundEventSender.sendTo(OperationType.DELETE, agentType, Collections.singletonList(
        new TenantResource(tenantId, resourceId)
    ));
  }

  List<AgentType> findBoundAgentTypesByResource(String tenandId, String resourceId) {

    return em.createNamedQuery("findBoundAgentTypesByResource", AgentType.class)
        .setParameter("tenantId", tenandId)
        .setParameter("resourceId", resourceId)
        .getResultList();

  }

  private void upsertBindingToResource(ResourceDTO resource, AgentType agentType,
                                       AgentInstall newestAgentInstall, boolean reattached) {
    log.debug("Upserting binding of install={} to resource={}", newestAgentInstall, resource);

    final String tenantId = resource.getTenantId();
    final String resourceId = resource.getResourceId();

    final List<BoundAgentInstall> priorBindings = boundAgentInstallRepository
        .findAllByTenantResourceAgentType(tenantId, resourceId, agentType);

    boolean alreadyBound = false;
    for (BoundAgentInstall priorBinding : priorBindings) {
      if (priorBinding.getAgentInstall().getId().equals(newestAgentInstall.getId())) {
        alreadyBound = true;
      }
      else {
        // just to be sure, clean up an older binding
        boundAgentInstallRepository.delete(priorBinding);
      }
    }

    if (!alreadyBound) {
      boundAgentInstallRepository.save(
          new BoundAgentInstall()
          .setAgentInstall(newestAgentInstall)
          .setResourceId(resourceId)
      );
    }

    // send an event if new binding
    // ...or a reattached envoy needs to be notified of existing binding
    if (!alreadyBound || reattached) {
      boundEventSender.sendTo(OperationType.UPSERT, agentType, Collections.singletonList(
          new TenantResource(tenantId, resourceId)
      ));
    }
  }

  private void unbindDeletedResource(String tenantId, String resourceId) {
    final List<BoundAgentInstall> bindings = boundAgentInstallRepository
        .findAllByTenantResource(
            tenantId, resourceId
        );

    boundAgentInstallRepository.deleteAll(bindings);

    // pick out the agent types of the bindings and send an event for each
    bindings.stream()
        .map(boundAgentInstall -> boundAgentInstall.getAgentInstall().getAgentRelease().getType())
        .distinct()
        .forEach(agentType ->
            boundEventSender.sendTo(OperationType.DELETE, agentType,
                Collections.singletonList(
                    new TenantResource(tenantId, resourceId)
                )
            ));
  }

  private void handleReattachedEnvoy(String tenantId, String resourceId) {
    log.debug("Handling reattachedEnvoy with tenantId={} resourceId={}", tenantId, resourceId);

    // Leverage existing bindings to just re-send an event per agent type for the re-attached
    // envoy resource

    final List<AgentType> boundAgentTypes = findBoundAgentTypesByResource(
        tenantId, resourceId);

    for (AgentType boundAgentType : boundAgentTypes) {
      boundEventSender.sendTo(OperationType.UPSERT, boundAgentType, Collections.singletonList(
          new TenantResource(tenantId, resourceId)
      ));
    }
  }

  private static Version versionOf(BoundAgentInstall boundInstall) {
    return Version
        .valueOf(boundInstall.getAgentInstall().getAgentRelease().getVersion());
  }

  private static Version versionOf(AgentInstall install) {
    return Version.valueOf(install.getAgentRelease().getVersion());
  }
}
