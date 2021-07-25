#standardSQL
# Popular PWA libraries by url (detail)
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
  url,
  libraries,
FROM
  `httparchive.sample_data.pages_*`,
  --`httparchive.pages.2021_07_01_*`,
  UNNEST(getSWLibraries(JSON_EXTRACT(payload, '$._pwa.importScriptsInfo'))) AS libraries
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  JSON_EXTRACT(payload, '$._pwa.importScriptsInfo') != "[]"
ORDER BY url ASC