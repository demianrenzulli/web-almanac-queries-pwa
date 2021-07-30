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
  client,
  COUNT(0) AS total,
  COUNTIF(LOWER(sw_events) LIKE '%install%') AS install,
  COUNTIF(LOWER(sw_events) LIKE '%activate%') AS activate,
  COUNTIF(LOWER(sw_events) LIKE '%push%') AS push,
  COUNTIF(LOWER(sw_events) LIKE '%notificationclick%') AS notificationclick,
  COUNTIF(LOWER(sw_events) LIKE '%notificationclose%') AS notificationclose,
  COUNTIF(LOWER(sw_events) LIKE '%sync%') AS sync,
  COUNTIF(LOWER(sw_events) LIKE '%canmakepayment%') AS canmakepayment,
  COUNTIF(LOWER(sw_events) LIKE '%paymentrequest%') AS paymentrequest,
  COUNTIF(LOWER(sw_events) LIKE '%periodicsync%') AS periodicsync,
  COUNTIF(LOWER(sw_events) LIKE '%backgroundfetchsuccess%') AS backgroundfetchsuccess,
  COUNTIF(LOWER(sw_events) LIKE '%backgroundfetchfailure%') AS backgroundfetchfailure,
  COUNTIF(LOWER(sw_events) LIKE '%backgroundfetchabort%') AS backgroundfetchabort,
  COUNTIF(LOWER(sw_events) LIKE '%backgroundfetchclick%') AS backgroundfetchclick
FROM
(
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
)
GROUP BY
client
ORDER BY
client