#standardSQL
# SW events by url
CREATE TEMPORARY FUNCTION getSWEvents(swEventListenersInfo STRING)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {
  var swEvents = Object.values(JSON.parse(swEventListenersInfo));

  if (typeof swEvents != 'string') {
    swEvents = swEvents.toString();
  }

  swEvents = swEvents.trim().split(',');
  return Array.from(new Set(swEvents));
} catch (e) {
  return [e];
}
''';
SELECT
  _TABLE_SUFFIX AS client,
  url,
  sw_events
FROM
  `httparchive.sample_data.pages_*`,
  --`httparchive.pages.2021_07_01_*`,
  UNNEST(getSWEvents(JSON_EXTRACT(payload, '$._pwa.swEventListenersInfo'))) AS sw_events
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  JSON_EXTRACT(payload, '$._pwa.swEventListenersInfo') != "[]"
GROUP BY url, client, sw_events
ORDER BY url ASC