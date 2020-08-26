function syncFieldsWithVersion(val) {
  let version = window.dagVersions[val] || null;
  if (version == null) {
    console.warn("Selected version not fetched!");
    return;
  }
  for (const [field, val] of Object.entries(version)) {
    query = $(`.conf-field#${field}`);
    if ("boolean" === typeof val) query[0].checked = val;
    else query.val(val);
  }
}

$("#live_version").change((event) => {
  let val = $(event.target).val();
  syncFieldsWithVersion(val);
});
