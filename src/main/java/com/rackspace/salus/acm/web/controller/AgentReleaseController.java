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

package com.rackspace.salus.acm.web.controller;

import com.rackspace.salus.acm.services.AgentReleaseService;
import com.rackspace.salus.acm.web.model.AgentReleaseCreate;
import com.rackspace.salus.acm.web.model.AgentReleaseDTO;
import com.rackspace.salus.telemetry.entities.AgentRelease;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.PagedContent;
import com.rackspace.salus.telemetry.repositories.AgentReleaseRepository;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.validation.Valid;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@Api(value = "Agent release operations", authorizations = {
    @Authorization(value = "repose_auth",
        scopes = {
            @AuthorizationScope(scope = "write:agent_release", description = "modify Agent Releases"),
            @AuthorizationScope(scope = "read:agent_release", description = "read your Agent Releases"),
            @AuthorizationScope(scope = "delete:agent_release", description = "delete your Agent Releases")
        })
})
public class AgentReleaseController {

  private final AgentReleaseRepository agentReleaseRepository;
  private final AgentReleaseService agentReleaseService;

  @Autowired
  public AgentReleaseController(AgentReleaseRepository agentReleaseRepository,
                                AgentReleaseService agentReleaseService) {
    this.agentReleaseRepository = agentReleaseRepository;
    this.agentReleaseService = agentReleaseService;
  }

  /**
   * Get available agent releases
   * @param tenantId the authenticated tenant ID; however, the results of this operation do not vary by tenant
   * @param agentType if provided, only releases of that agent type are returned and releases are
   *  always sorted with newest-version-first
   * @param pageable specifies the page and page size to retrieve
   * @return the requested agent releases
   */
  @GetMapping("/tenant/{tenantId}/agent-releases")
  @ApiOperation(value = "Get available agent releases")
  public PagedContent<AgentReleaseDTO> getAgentReleasesForTenant(@PathVariable String tenantId,
                                                                 @RequestParam(value = "type", required = false) AgentType agentType,
                                                                 Pageable pageable) {
    // tenantId isn't actually used, but it present to keep a consitent request path structure
    // across other APIs

    if (agentType != null) {
      return getAgentReleasesForType(agentType, pageable);
    } else {
      return PagedContent.fromPage(
          agentReleaseRepository.findAll(pageable)
              .map(AgentReleaseDTO::new)
      );
    }
  }

  private PagedContent<AgentReleaseDTO> getAgentReleasesForType(AgentType agentType,
                                                                Pageable pageable) {
    // retrieve the full list since sorting by version can't be done DB-side
    final List<AgentRelease> all = agentReleaseRepository.findAllByType(agentType);

    // sort and narrow to requested page
    final List<AgentReleaseDTO> content = all.stream()
        // ...type inference needs help here due to .reversed()
        .sorted(Comparator.<AgentRelease, ComparableVersion>comparing(
            agentRelease -> new ComparableVersion(agentRelease.getVersion())
        ).reversed())
        // ...offset uses page size and number
        .skip(pageable.getOffset())
        .limit(pageable.getPageSize())
        .map(AgentReleaseDTO::new)
        .collect(Collectors.toList());

    final int totalPages = (int) Math.ceil((double) all.size() / pageable.getPageSize());

    return new PagedContent<AgentReleaseDTO>()
        .setContent(content)
        .setNumber(pageable.getPageNumber())
        .setTotalElements(all.size())
        .setTotalPages(totalPages)
        .setFirst(pageable.getPageNumber() == 0)
        .setLast(pageable.getPageNumber() >= totalPages-1);
  }

  @GetMapping("/tenant/{tenantId}/agent-releases/{agentReleaseId}")
  @ApiOperation(value = "Get a specific agent release")
  public AgentReleaseDTO getAgentReleaseForTenant(@PathVariable String tenantId,
                                                  @PathVariable UUID agentReleaseId,
                                                  Pageable pageable) {
    // tenantId isn't actually used, but it present to keep a consitent request path structure
    // across other APIs

    return new AgentReleaseDTO(
        agentReleaseRepository.findById(agentReleaseId)
            .orElseThrow(() -> new NotFoundException("Unable to find agent release")));
  }

  @GetMapping("/admin/agent-releases")
  @ApiOperation(value = "Get available agent releases")
  public PagedContent<AgentReleaseDTO> getAgentReleases(Pageable pageable) {

    return PagedContent.fromPage(
        agentReleaseRepository.findAll(pageable)
            .map(AgentReleaseDTO::new)
    );

  }

  @GetMapping("/admin/agent-releases/{agentReleaseId}")
  @ApiOperation(value = "Get a specific agent release")
  public AgentReleaseDTO getAgentRelease(@PathVariable UUID agentReleaseId) {

    return new AgentReleaseDTO(
        agentReleaseRepository.findById(agentReleaseId)
            .orElseThrow(() -> new NotFoundException("Unable to find agent release")));
  }

  @PostMapping("/admin/agent-releases")
  @ResponseStatus(HttpStatus.CREATED)
  @ApiOperation(value = "Declare a new agent release")
  public AgentReleaseDTO declareAgentRelease(@RequestBody @Valid AgentReleaseCreate in) {
    return new AgentReleaseDTO(agentReleaseService.create(in));
  }

  @DeleteMapping("/admin/agent-releases/{agentReleaseId}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @ApiOperation(value = "Delete an agent release")
  public void delete(@PathVariable UUID agentReleaseId) {
    agentReleaseService.delete(agentReleaseId);
  }
}
