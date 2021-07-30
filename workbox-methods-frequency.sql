#standardSQL
#Workbox methods by url (detail)
CREATE TEMPORARY FUNCTION getWorkboxMethods(workboxInfo STRING)
RETURNS ARRAY<STRING> LANGUAGE js AS '''
try {
 
  var workboxPackageMethods = Object.values(JSON.parse(workboxInfo));

  if (typeof workboxPackageMethods == 'string') {  
    workboxPackageMethods = [workboxPackageMethods];
  }

  var workboxMethods = [];

  /* Replacing spaces and commas */
  for (var i = 0; i < workboxPackageMethods.length; i++) {
      var workboxItems = workboxPackageMethods[i].toString().trim().split(',');

      for(var j = 0; j < workboxItems.length; j++) {
        if(workboxItems[j].indexOf(':') == -1) {
          workboxMethods.push(workboxItems[j].trim());
        }
      }
  }

  return Array.from(new Set(workboxMethods));
} catch (e) {
  return [e];
}
''';
SELECT
  _TABLE_SUFFIX AS client,
  workbox_method,
  COUNT(0) AS freq
FROM
  `httparchive.sample_data.pages_*`,
  --`httparchive.pages.2021_07_01_*`,
  UNNEST(getWorkboxMethods(JSON_EXTRACT(payload, '$._pwa.workboxInfo'))) AS workbox_method
WHERE
  JSON_EXTRACT(payload, '$._pwa') != "[]" AND
  JSON_EXTRACT(payload, '$._pwa.workboxInfo') != "[]"
GROUP BY workbox_method, client