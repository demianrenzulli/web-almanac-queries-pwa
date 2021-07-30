#standardSQL
#Workbox packages by url (detail)
CREATE TEMPORARY FUNCTION getWorkboxVersions(workboxInfo STRING)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {
 
  var workboxPackageMethods = Object.values(JSON.parse(workboxInfo));

  if (typeof workboxPackageMethods == 'string') {  
    workboxPackageMethods = [workboxPackageMethods];
  }

  var workboxVersions = [];

  /* Replacing spaces and commas */
  for (var i = 0; i < workboxPackageMethods.length; i++) {
      var workboxItems = workboxPackageMethods[i].toString().trim().split(',');

      for(var j = 0; j < workboxItems.length; j++) {
        var workboxItem = workboxItems[j];
        var firstColonIndex = workboxItem.indexOf(':');
        if(firstColonIndex > -1) {
          var workboxVersion = workboxItem.trim().substring(workboxItem.indexOf(':', firstColonIndex + 1));
          workboxVersions.push(workboxVersion);
        }
      }
  }

  return Array.from(new Set(workboxVersions));
} catch (e) {
  return [e];
}
''';
SELECT
  _TABLE_SUFFIX AS client,
  url,
  workbox_versions,
FROM
  `httparchive.sample_data.pages_*`,
  --`httparchive.pages.2021_07_01_*`,
  UNNEST(getWorkboxVersions(JSON_EXTRACT(payload, '$._pwa.workboxInfo'))) AS workbox_versions
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  JSON_EXTRACT(payload, '$._pwa.workboxInfo') != "[]"
ORDER BY url ASC