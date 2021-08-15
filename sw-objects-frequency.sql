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
  client,
  COUNT(0) AS freq,
  COUNTIF(LOWER(sw_objects) LIKE '%clients.get%') AS clients_get,
  COUNTIF(LOWER(sw_objects) LIKE '%clients.matchall%') AS clients_matchAll,
  COUNTIF(LOWER(sw_objects) LIKE '%clients.openwindow%') AS clients_openWindow,
  COUNTIF(LOWER(sw_objects) LIKE '%clients.claim%') AS clients_claim,
  COUNTIF(LOWER(sw_objects) LIKE '%client.postmessage%') AS client_postMessage,
  COUNTIF(LOWER(sw_objects) LIKE '%client.id%') AS client_id,
  COUNTIF(LOWER(sw_objects) LIKE '%client.type%') AS client_type,
  COUNTIF(LOWER(sw_objects) LIKE '%client.url%') AS client_url,
  COUNTIF(LOWER(sw_objects) LIKE '%caches.match%') AS caches_match,
  COUNTIF(LOWER(sw_objects) LIKE '%caches.has%') AS caches_has,
  COUNTIF(LOWER(sw_objects) LIKE '%caches.open%') AS caches_open,
  COUNTIF(LOWER(sw_objects) LIKE '%caches.delete%') AS caches_delete,
  COUNTIF(LOWER(sw_objects) LIKE '%caches.keys%') AS caches_keys,
  COUNTIF(LOWER(sw_objects) LIKE '%cache.match%') AS cache_match,
  COUNTIF(LOWER(sw_objects) LIKE '%cache.matchall%') AS cache_matchall,
  COUNTIF(LOWER(sw_objects) LIKE '%cache.add%') AS cache_add,
  COUNTIF(LOWER(sw_objects) LIKE '%cache.addall%') AS cache_addall,
  COUNTIF(LOWER(sw_objects) LIKE '%cache.put%') AS cache_put,
  COUNTIF(LOWER(sw_objects) LIKE '%cache.keys%') AS cache_keys
FROM
(
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
)
GROUP BY client