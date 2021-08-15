#standardSQL
# SW objects by url
CREATE TEMPORARY FUNCTION getSWObjects(swObjectsInfo STRING)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {
  var swObjects = Object.values(JSON.parse(swObjectsInfo));

  if (typeof swObjects != 'string') {
    swObjects = swObjects.toString();
  }

  swObjects = swObjects.trim().split(',');
  return Array.from(new Set(swObjects));
} catch (e) {
  return [e];
}
''';
SELECT
  _TABLE_SUFFIX AS client,
  url,
  sw_objects
FROM
  `httparchive.sample_data.pages_*`,
  --`httparchive.pages.2021_07_01_*`,
  UNNEST(getSWObjects(JSON_EXTRACT(payload, '$._pwa.swObjectsInfo'))) AS sw_objects
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  JSON_EXTRACT(payload, '$._pwa.swObjectsInfo') != "[]"
GROUP BY url, client, sw_objects
ORDER BY url ASC