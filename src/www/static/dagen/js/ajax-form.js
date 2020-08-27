$(function () {
  if (Swal === undefined) console.error("Include SweetAlert 2 script");
  initAjaxFormPosts();
});

function initAjaxFormPosts() {
  if ($("form.form-actions.ajax-form").length > 0) {
    $("form.form-actions.ajax-form").each(function () {
      const $form = $(this).closest("form");

      const postAjaxHandler = () =>
        postData(
          $form.attr("action"),
          $form.attr("method") || "POST",
          $form.serialize()
        )
          .then((data) => {
            return data;
          })
          .catch(({ status, statusText }) => {
            Swal.showValidationMessage(
              `Request errored with ${status}: ${statusText}`
            );
          });

      const refreshAfterTimeout = () =>
        setTimeout(() => window.location.reload(), 2000);

      $form.find(".btn.submit").bind("click", function (e) {
        // do not submit the form
        e.preventDefault();

        let btn = $(this);
        let confirmTitle = btn.attr("data-confirm-title");
        let confirmDescription = btn.attr("data-confirm-description");
        let confirmBtn = btn.attr("data-confirm-button");
        let confirmLvl = btn.attr("data-confirm-level");
        let showConfirm =
          confirmTitle || confirmDescription || confirmBtn || confirmLvl;
        if (showConfirm) {
          Swal.fire({
            title: confirmTitle || "",
            text: confirmDescription || "",
            icon: confirmLvl,
            showCancelButton: true,
            confirmButtonText: confirmBtn || "Yes, confirm",
            showLoaderOnConfirm: true,
            preConfirm: postAjaxHandler,
            allowOutsideClick: () => !Swal.isLoading(),
          }).then((result) => {
            if (result.value) {
              Swal.fire({
                title: "Success!",
                text: JSON.stringify(result.value),
              });
              refreshAfterTimeout();
            }
          });
        } else {
          postAjaxHandler().then(refreshAfterTimeout);
        }
      });
    });
  }
}

/* Generic POST function*/
function postData(url, method, model) {
  return $.ajax({
    url: url,
    type: "POST",
    data: model,
  });
}
