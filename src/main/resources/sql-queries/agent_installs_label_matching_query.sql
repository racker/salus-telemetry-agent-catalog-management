SELECT agent_installs.id
FROM   agent_installs
JOIN   agent_install_label_selectors AS ail
WHERE  agent_installs.id = ail.agent_install_id
AND    agent_installs.label_selector_method = 'AND'
AND    agent_installs.id IN
(
  SELECT agent_install_id
  FROM   agent_install_label_selectors
  WHERE  agent_installs.id IN
  (
    SELECT id
    FROM   agent_installs
    WHERE  tenant_id = :tenantId
  )
  AND    agent_installs.id IN
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
  )
)
ORDER BY agent_installs.id