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
 *
 */

package com.rackspace.salus.acm.services;

import com.rackspace.salus.acm.web.model.AgentInstallCreate;
import com.rackspace.salus.common.config.MetricNames;
import com.rackspace.salus.common.config.MetricTags;
import com.rackspace.salus.common.config.MetricTagValues;
import com.rackspace.salus.common.util.SpringResourceUtils;
import com.rackspace.salus.resource_management.web.client.ResourceApi;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.telemetry.entities.AgentInstall;
import com.rackspace.salus.telemetry.entities.AgentRelease;
import com.rackspace.salus.telemetry.entities.BoundAgentInstall;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import com.rackspace.salus.telemetry.messaging.OperationType;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.repositories.AgentInstallRepository;
import com.rackspace.salus.telemetry.repositories.AgentReleaseRepository;
import com.rackspace.salus.telemetry.repositories.BoundAgentInstallRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import com.rackspace.salus.telemetry.repositories.ResourceRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;

@Service
@Slf4j
public class AgentInstallService {

  private final JdbcTemplate jdbcTemplate;
  private final EntityManager em;
  private final AgentReleaseRepository agentReleaseRepository;
  private final AgentInstallRepository agentInstallRepository;
  private final BoundAgentInstallRepository boundAgentInstallRepository;
  private final ResourceApi resourceApi;
  private final BoundEventSender boundEventSender;
  private final String labelMatchQuery;
  private final String labelMatchORQuery;
  private final ResourceRepository resourceRepository;

  MeterRegistry meterRegistry;

  // metrics counters
  private final Counter.Builder agentInstallSuccess;

  @Autowired
  public AgentInstallService(JdbcTemplate jdbcTemplate,
                             EntityManager entityManager,
                             AgentReleaseRepository agentReleaseRepository,
                             AgentInstallRepository agentInstallRepository,
                             BoundAgentInstallRepository boundAgentInstallRepository,
                             ResourceApi resourceApi, MeterRegistry meterRegistry,
                             BoundEventSender boundEventSender,
                             ResourceRepository resourceRepository) throws IOException {
    this.jdbcTemplate = jdbcTemplate;
    this.em = entityManager;
    this.agentReleaseRepository = agentReleaseRepository;
    this.agentInstallRepository = agentInstallRepository;
    this.boundAgentInstallRepository = boundAgentInstallRepository;
    this.resourceApi = resourceApi;
    this.boundEventSender = boundEventSender;
    this.resourceRepository = resourceRepository;
    labelMatchQuery = SpringResourceUtils.readContent("sql-queries/agent_installs_label_matching_query.sql");
    labelMatchORQuery = SpringResourceUtils.readContent("sql-queries/agent_installs_label_matching_OR_query.sql");

    this.meterRegistry = meterRegistry;
    agentInstallSuccess = Counter.builder(MetricNames.SERVICE_OPERATION_SUCCEEDED)
        .tag(MetricTags.SERVICE_METRIC_TAG,"AgentInstall");
  }

  @Transactional
  public AgentInstall install(String tenantId, AgentInstallCreate in) {
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
        .setTenantId(tenantId)
        .setLabelSelectorMethod(in.getLabelSelectorMethod());

    final AgentInstall saved = agentInstallRepository.save(agentInstall);

    bindInstallToResources(saved);

    log.info("Created agentInstall={}", saved);
    agentInstallSuccess
        .tags(MetricTags.OPERATION_METRIC_TAG,"install",MetricTags.OBJECT_TYPE_METRIC_TAG,"agent")
        .register(meterRegistry).increment();
    return saved;
  }

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
    agentInstallSuccess
        .tags(MetricTags.OPERATION_METRIC_TAG, MetricTagValues.REMOVE_OPERATION,MetricTags.OBJECT_TYPE_METRIC_TAG,"agent")
        .register(meterRegistry).increment();
  }

  @Transactional
  public void deleteAllAgentInstallsForTenant(String tenantId) {
    final List<BoundAgentInstall> bound = boundAgentInstallRepository
        .findAllByTenant(tenantId);

    final List<TenantResource> affectedResourceIds = bound.stream()
        .map(BoundAgentInstall::getResourceId)
        .distinct()
        .map(resourceId -> new TenantResource(tenantId,resourceId))
        .collect(Collectors.toList());

    boundAgentInstallRepository.deleteAll(bound);
    agentInstallRepository.deleteAllByTenantId(tenantId);

    if (!affectedResourceIds.isEmpty()) {
      boundEventSender.sendTo(
          OperationType.DELETE, null, affectedResourceIds);
    }
    agentInstallSuccess
        .tags(MetricTags.OPERATION_METRIC_TAG,"deleteAll",MetricTags.OBJECT_TYPE_METRIC_TAG,"agent")
        .register(meterRegistry).increment();
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

      final ResourceDTO resource = findResourceByTenantIdAndResourceId(resourceEvent.getTenantId(),
          resourceEvent.getResourceId());

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

  List<AgentInstall> getInstallsFromResourceLabels(String tenantId, Map<String, String> resourceLabels)
      throws IllegalArgumentException {
    if(resourceLabels == null || resourceLabels.isEmpty()) {
      return agentInstallRepository.findByTenantIdAndLabelSelectorIsNull(tenantId);
    }

    MapSqlParameterSource paramSource = new MapSqlParameterSource();
    paramSource.addValue("tenantId", tenantId);
    StringBuilder builder = new StringBuilder();
    int i = 0;
    for (Map.Entry<String, String> entry : resourceLabels.entrySet()) {
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

    paramSource.addValue("i", i);

    @SuppressWarnings("ConstantConditions")
    NamedParameterJdbcTemplate namedParameterTemplate = new NamedParameterJdbcTemplate(
        jdbcTemplate.getDataSource());
    final List<UUID> monitorIds = namedParameterTemplate.query(String.format(labelMatchQuery, builder.toString()), paramSource,
        (resultSet, rowIndex) -> UUID.fromString(resultSet.getString(1))
    );

    final List<UUID> monitorOrIds = namedParameterTemplate.query(String.format(labelMatchORQuery, builder.toString()), paramSource,
        (resultSet, rowIndex) -> UUID.fromString(resultSet.getString(1))
    );

    monitorIds.addAll(monitorOrIds);

    // use JPA to retrieve and resolve the entities and then convert Iterable result to list
    final ArrayList<AgentInstall> results = new ArrayList<>();
    for (AgentInstall agentInstall : agentInstallRepository.findAllById(monitorIds)) {
      results.add(agentInstall);
    }

    return results;
  }

  private void bindInstallToResources(AgentInstall agentInstall) {
    final List<ResourceDTO> resources = resourceApi
        .getResourcesWithLabels(agentInstall.getTenantId(), agentInstall.getLabelSelector(), agentInstall.getLabelSelectorMethod());

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

    final ComparableVersion ourVersion = versionOf(binding);

    final List<BoundAgentInstall> othersSorted = new ArrayList<>(others);
    othersSorted.sort(Comparator.comparing(AgentInstallService::versionOf));

    // test if our version is greater than the existing/other version
    final boolean keepOurs = ourVersion.compareTo(versionOf(othersSorted.get(0))) > 0;

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
        getInstallsFromResourceLabels(resource.getTenantId(), resource.getLabels())) {
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

  private static ComparableVersion versionOf(BoundAgentInstall boundInstall) {
    return new ComparableVersion(boundInstall.getAgentInstall().getAgentRelease().getVersion());
  }

  private static ComparableVersion versionOf(AgentInstall install) {
    return new ComparableVersion(install.getAgentRelease().getVersion());
  }

  public ResourceDTO findResourceByTenantIdAndResourceId(String tenantId, String resourceId) {
    return resourceRepository.findByTenantIdAndResourceId(tenantId, resourceId)
        .map(resource -> new ResourceDTO(resource, null))
        .orElse(null);
  }
}
