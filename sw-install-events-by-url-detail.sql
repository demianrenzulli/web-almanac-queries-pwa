#standardSQL
# SW install events by url
CREATE TEMPORARY FUNCTION getInstallEvents(windowEventListeners ARRAY<STRING>, windowPropertiesInfo ARRAY<STRING>)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {
  var installEvents = windowEventListeners.concat(windowPropertiesInfo);
  return Array.from(new Set(installEvents));
} catch (e) {
  return [e];
}
''';
CREATE TEMPORARY FUNCTION parseField(field STRING)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {
    if(field == '[]' || field == '') {
        return [];
    }

    var parsedField = Object.values(JSON.parse(field));

    if (typeof parsedField != 'string') {
        parsedField = parsedField.toString();
    }

    parsedField = parsedField.trim().split(',');
    return parsedField;
} catch (e) {
  return [e];
}
''';
SELECT
  _TABLE_SUFFIX AS client,
  url,
  install_events
FROM
  `httparchive.sample_data.pages_*`,
  --`httparchive.pages.2021_07_01_*`,
  UNNEST(getInstallEvents(parseField(JSON_EXTRACT(payload, '$._pwa.windowEventListenersInfo')), parseField(JSON_EXTRACT(payload, '$._pwa.windowPropertiesInfo')))) AS install_events
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  (JSON_EXTRACT(payload, '$._pwa.windowEventListenersInfo') != "[]" OR JSON_EXTRACT(payload, '$._pwa.windowPropertiesInfo') != "[]")
GROUP BY url, client, install_events
ORDER BY url ASC