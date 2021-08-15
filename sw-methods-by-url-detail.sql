#standardSQL
# SW install events by url
CREATE TEMPORARY FUNCTION getSWMethods(swMethods ARRAY<STRING>)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {
  return Array.from(new Set(swMethods));
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
  sw_methods
FROM
  `httparchive.sample_data.pages_*`,
  --`httparchive.pages.2021_07_01_*`,
  UNNEST(getSWMethods(parseField(JSON_EXTRACT(payload, '$._pwa.swMethodsInfo')))) AS sw_methods
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  JSON_EXTRACT(payload, '$._pwa.swMethodsInfo') != "[]"
GROUP BY url, client, sw_methods
ORDER BY url ASC