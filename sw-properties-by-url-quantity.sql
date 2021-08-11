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
  COUNTIF(LOWER(sw_properties) LIKE '%install%') AS install,
  COUNTIF(LOWER(sw_properties) LIKE '%activate%') AS activate,
  COUNTIF(LOWER(sw_properties) LIKE '%push%') AS push,
  COUNTIF(LOWER(sw_properties) LIKE '%notificationclick%') AS notificationclick,
  COUNTIF(LOWER(sw_properties) LIKE '%notificationclose%') AS notificationclose,
  COUNTIF(LOWER(sw_properties) LIKE '%sync%') AS sync,
  COUNTIF(LOWER(sw_properties) LIKE '%canmakepayment%') AS canmakepayment,
  COUNTIF(LOWER(sw_properties) LIKE '%paymentrequest%') AS paymentrequest,
  COUNTIF(LOWER(sw_properties) LIKE '%periodicsync%') AS periodicsync,
  COUNTIF(LOWER(sw_properties) LIKE '%backgroundfetchsuccess%') AS backgroundfetchsuccess,
  COUNTIF(LOWER(sw_properties) LIKE '%backgroundfetchfailure%') AS backgroundfetchfailure,
  COUNTIF(LOWER(sw_properties) LIKE '%backgroundfetchabort%') AS backgroundfetchabort,
  COUNTIF(LOWER(sw_properties) LIKE '%backgroundfetchclick%') AS backgroundfetchclick
FROM
  `httparchive.sample_data.pages_*`,
  --`httparchive.pages.2021_07_01_*`,
  UNNEST(getSWProperties(JSON_EXTRACT(payload, '$._pwa.swPropertiesInfo'))) AS sw_properties
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  JSON_EXTRACT(payload, '$._pwa.swPropertiesInfo') != "[]"
GROUP BY url, client
ORDER BY url ASC