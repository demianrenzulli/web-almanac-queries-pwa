#standardSQL
# SW properties by url
CREATE TEMPORARY FUNCTION getSWProperties(swPropertiesInfo STRING)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {
  var swProperties = Object.values(JSON.parse(swPropertiesInfo));
  if (typeof swProperties != 'string') {
    swProperties = swProperties.toString();
  }
  swProperties = swProperties.trim().split(',');
  return Array.from(new Set(swProperties));
} catch (e) {
  return [e];
}
''';
SELECT
  _TABLE_SUFFIX AS client,
  url,
  sw_properties
FROM
  `httparchive.sample_data.pages_*`,
  --`httparchive.pages.2021_07_01_*`,
  UNNEST(getSWProperties(JSON_EXTRACT(payload, '$._pwa.swPropertiesInfo'))) AS sw_properties
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  JSON_EXTRACT(payload, '$._pwa.swPropertiesInfo') != "[]"
GROUP BY url, client, sw_properties
ORDER BY url ASC