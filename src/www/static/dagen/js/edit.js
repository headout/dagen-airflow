function syncFieldsWithVersion(val) {
  let version = window.dagVersions[val] || null;
  if (version == null) {
    console.warn("Selected version not fetched!");
    return;
  }
  for (const [field, val] of Object.entries(version)) {
    $(`.conf-field#${field}`).val(val);
  }
}

$("#live_version").change((event) => {
  let val = $(event.target).val();
  syncFieldsWithVersion(val);
});
