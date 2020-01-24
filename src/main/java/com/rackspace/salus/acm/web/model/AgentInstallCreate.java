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

package com.rackspace.salus.acm.web.model;

import com.rackspace.salus.telemetry.model.LabelSelectorMethod;
import java.util.Map;
import java.util.UUID;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class AgentInstallCreate {

  @NotNull
  UUID agentReleaseId;

  @NotNull
  Map<String, String> labelSelector;

  @NotNull
  LabelSelectorMethod labelSelectorMethod = LabelSelectorMethod.AND;
}
