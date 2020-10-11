function syncFieldsWithVersion(val) {
  let version = window.dagVersions[val] || null;
  if (version == null) {
    console.warn("Selected version not fetched!");
    return;
  }
  for (const [field, val] of Object.entries(version)) {
    query = $(`.conf-field[name="${field}"]`);
    if ("boolean" === typeof val) query[0].checked = val;
    else {
      query.val(val);
      // manually trigger the 'change' event of select input
      if (query.is("select")) query.trigger("change");
    }
  }
}

$("#live_version").change((event) => {
  let val = $(event.target).val();
  syncFieldsWithVersion(val);
});
