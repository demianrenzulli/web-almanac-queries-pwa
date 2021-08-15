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