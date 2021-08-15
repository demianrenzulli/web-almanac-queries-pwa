#standardSQL
# SW properties by url
CREATE TEMPORARY FUNCTION getSWRegistrationProperties(swRegistrationPropertiesInfo STRING)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {
  var swRegistrationProperties = Object.values(JSON.parse(swRegistrationPropertiesInfo));
  if (typeof swRegistrationProperties != 'string') {
    swRegistrationProperties = swRegistrationProperties.toString();
  }
  swRegistrationProperties = swRegistrationProperties.trim().split(',');
  return Array.from(new Set(swRegistrationProperties));
} catch (e) {
  return [e];
}
''';
SELECT
  client,
  COUNT(0) AS total,
  COUNTIF(LOWER(sw_registration_properties) LIKE '%navigationpreload.enable%') AS navigationpreload_enable,
  COUNTIF(LOWER(sw_registration_properties) LIKE '%navigationpreload.disable%') AS navigationpreload_disable,
  COUNTIF(LOWER(sw_registration_properties) LIKE '%navigationpreload.setheadervalue%') AS navigationpreload_setheadervalue,
  COUNTIF(LOWER(sw_registration_properties) LIKE '%navigationpreload.getstate') AS navigationpreload_getstate,
  COUNTIF(LOWER(sw_registration_properties) LIKE '%pushmanager.getsubscription') AS pushManager_getsubscription,
  COUNTIF(LOWER(sw_registration_properties) LIKE '%pushmanager.permissionstate') AS pushManager_permissionstate,
  COUNTIF(LOWER(sw_registration_properties) LIKE '%pushmanager.subscribe') AS pushManager_subscribe,
  COUNTIF(LOWER(sw_registration_properties) LIKE '%sync.register') AS sync_register,
  COUNTIF(LOWER(sw_registration_properties) LIKE '%sync.gettags') AS sync_gettags
FROM
(
    SELECT
    _TABLE_SUFFIX AS client,
    url,
    sw_registration_properties
    FROM
    `httparchive.sample_data.pages_*`,
    --`httparchive.pages.2021_07_01_*`,
    UNNEST(getSWRegistrationProperties(JSON_EXTRACT(payload, '$._pwa.swRegistrationPropertiesInfo'))) AS sw_registration_properties
    WHERE
    JSON_EXTRACT(payload, '$._pwa') != "[]" AND
    JSON_EXTRACT(payload, '$._pwa.swRegistrationPropertiesInfo') != "[]"
    GROUP BY url, client, sw_registration_properties
    ORDER BY url ASC
)
GROUP BY
client
ORDER BY
client