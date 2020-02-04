SELECT agent_installs.id
FROM   agent_installs LEFT OUTER JOIN agent_install_label_selectors AS ail ON agent_installs.id = ail.agent_install_id
WHERE  agent_installs.label_selector_method = 'OR'
AND agent_installs.tenant_id = :tenantId
AND (ail.agent_install_id IS NULL OR agent_installs.id IN
(
  SELECT  inner_ails.agent_install_id
  FROM     agent_install_label_selectors AS inner_ails
  WHERE    %s
  GROUP BY inner_ails.agent_install_id
  HAVING COUNT(*) >= 1
))
ORDER BY agent_installs.id