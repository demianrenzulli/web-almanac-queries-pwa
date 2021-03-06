#standardSQL
# SW install events
# TODO add filter to eliminate false positives (e.g Youtube)
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

    var jsonObject = JSON.parse(field);

    /* Remove entries with libraries that cause false positives (e.g. Youtube) */
    var objectKeys = Object.keys(jsonObject);
    if (typeof objectKeys != 'string') {
        objectKeys = objectKeys.toString();
    }
    objectKeys = objectKeys.trim().split(',');
    for(var i = 0; i < objectKeys.length; i++) {
        if(objectKeys[i].toLowerCase().includes('youtube')) {
            delete jsonObject[objectKeys[i]];
        }
    }

    var objectValues = Object.values(jsonObject);
    if (typeof objectValues != 'string') {
        objectValues = objectValues.toString();
    }
    return objectValues.trim().split(',');
} catch (e) {
  return [e];
}
''';
SELECT
  _TABLE_SUFFIX AS client,
  install_event,
  COUNT(DISTINCT url) AS freq,
  total,
  COUNT(DISTINCT url) / total AS pct
FROM
    `httparchive.sample_data.pages_*`,
    --`httparchive.pages.2021_07_01_*`,
    UNNEST(getInstallEvents(parseField(JSON_EXTRACT(payload, '$._pwa.windowEventListenersInfo')), parseField(JSON_EXTRACT(payload, '$._pwa.windowPropertiesInfo')))) AS install_event
JOIN
  (
    SELECT
      _TABLE_SUFFIX,
      COUNT(0) AS total
    FROM
    `httparchive.sample_data.pages_*`
      -- `httparchive.pages.2021_07_01_*`
    WHERE
      JSON_EXTRACT(payload, '$._pwa') != "[]"
    GROUP BY
      _TABLE_SUFFIX
  )
USING (_TABLE_SUFFIX)
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  (JSON_EXTRACT(payload, '$._pwa.windowEventListenersInfo') != "[]" OR JSON_EXTRACT(payload, '$._pwa.windowPropertiesInfo') != "[]") AND 
  install_event != '' AND install_event != '[]'
GROUP BY
  client,
  total,
  install_event
ORDER BY
  freq / total DESC,
  client