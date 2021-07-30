#standardSQL
# Popular PWA libraries
CREATE TEMPORARY FUNCTION getSWLibraries(importScriptsInfo STRING)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {

  /* 'importScriptsInfo' returns an array of libraries that might import other libraries
     The final array of libraries comes from the combination of both */
  var ObjKeys = Object.keys(JSON.parse(importScriptsInfo));  
  var ObjValues = Object.values(JSON.parse(importScriptsInfo));

  if (typeof ObjKeys == 'string') {
    ObjKeys = [ObjKeys];
  }
  if (typeof ObjValues == 'string') {
    ObjValues = [ObjValues];
  }

  var libraries = ObjKeys.concat(ObjValues);

  /* Replacing spaces and commas */
  for (var i = 0; i < libraries.length; i++) {
      libraries[i] = libraries[i].toString().trim().replace(/'/g, "");
  }

  /* Creating a Set to eliminate duplicates and transforming back to an array to respect the function signature */
  return Array.from(new Set(libraries));
} catch (e) {
  return [e];
}
''';
SELECT
  client,
  COUNT(0) AS total,
  COUNTIF(LOWER(libraries) LIKE '%workbox%') AS workbox,
  COUNTIF(LOWER(libraries) LIKE '%sw-toolbox%') AS sw_toolbox,
  COUNTIF(LOWER(libraries) LIKE '%firebase%') AS firebase,
  COUNTIF(LOWER(libraries) LIKE '%onesignalsdk%') AS OneSignalSDK,
  COUNTIF(LOWER(libraries) LIKE '%najva%') AS najva,
  COUNTIF(LOWER(libraries) LIKE '%upush%') AS upush,
  COUNTIF(LOWER(libraries) LIKE '%cache-polyfill%') AS cache_polyfill,
  COUNTIF(LOWER(libraries) LIKE '%analytics-helper%') AS analytics_helper,
  COUNTIF(LOWER(libraries) LIKE '%recaptcha%') AS recaptcha,
  COUNTIF(LOWER(libraries) LIKE '%pwabuilder%') AS pwabuilder,
  COUNTIF(LOWER(libraries) LIKE '%importscript%' AND
    LOWER(libraries) NOT LIKE '%workbox%' AND
    LOWER(libraries) NOT LIKE '%sw-toolbox%' AND
    LOWER(libraries) NOT LIKE '%firebase%' AND
    LOWER(libraries) NOT LIKE '%onesignalsdk%' AND
    LOWER(libraries) NOT LIKE '%najva%' AND
    LOWER(libraries) NOT LIKE '%upush%' AND
    LOWER(libraries) NOT LIKE '%cache-polyfill%' AND
    LOWER(libraries) NOT LIKE '%analytics-helper%'AND 
    LOWER(libraries) NOT LIKE '%recaptcha%' AND
    LOWER(libraries) NOT LIKE '%pwabuilder%') AS importscript_nolib,
  COUNTIF(LOWER(libraries) NOT LIKE '%importscript%' AND
    LOWER(libraries) NOT LIKE '%workbox%' AND
    LOWER(libraries) NOT LIKE '%sw-toolbox%' AND
    LOWER(libraries) NOT LIKE '%firebase%' AND
    LOWER(libraries) NOT LIKE '%onesignalsdk%' AND
    LOWER(libraries) NOT LIKE '%najva%' AND
    LOWER(libraries) NOT LIKE '%upush%' AND
    LOWER(libraries) NOT LIKE '%cache-polyfill.js%' AND
    LOWER(libraries) NOT LIKE '%analytics-helper.js%' AND
    LOWER(libraries) NOT LIKE '%recaptcha%' AND
    LOWER(libraries) NOT LIKE '%pwabuilder%') AS none_of_the_above
FROM
 (
  SELECT
    _TABLE_SUFFIX AS client,
    url,
    libraries,
  FROM
    `httparchive.sample_data.pages_*`,
    --`httparchive.pages.2021_07_01_*`,
    UNNEST(getSWLibraries(JSON_EXTRACT(payload, '$._pwa.importScriptsInfo'))) AS libraries
  WHERE
    JSON_EXTRACT(payload, '$._pwa') != "[]" AND
    JSON_EXTRACT(payload, '$._pwa.importScriptsInfo') != "[]"
)
GROUP BY
client
ORDER BY
client