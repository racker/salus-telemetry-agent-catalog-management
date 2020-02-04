SELECT agent_installs.id
FROM   agent_installs
LEFT OUTER JOIN   agent_install_label_selectors AS ail ON agent_installs.id = ail.agent_install_id
WHERE  agent_installs.label_selector_method = 'AND'
AND    agent_installs.tenant_id = :tenantId
AND    (ail.agent_install_id IS NULL OR agent_installs.id IN
(
  SELECT search_labels.agent_install_id
  FROM
  (
    SELECT   agent_install_id,
       COUNT(*) AS count
    FROM     agent_install_label_selectors
    GROUP BY agent_install_id
  )
  AS total_labels
  JOIN
  (
    SELECT   agent_install_id,
      COUNT(*) AS count
    FROM     agent_install_label_selectors
    WHERE    %s
    GROUP BY agent_install_id
  )
  AS search_labels
  WHERE    total_labels.agent_install_id = search_labels.agent_install_id
  AND      search_labels.count >= total_labels.count
  GROUP BY search_labels.agent_install_id
))
ORDER BY agent_installs.id