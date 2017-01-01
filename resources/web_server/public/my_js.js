function getCookie(name) {
  var value = "; " + document.cookie;
  var parts = value.split("; " + name + "=");
  if (parts.length == 2) return parts.pop().split(";").shift();
}

$(document).ready(function() {
    $("#request_time").text(getCookie("request_time") + " ms");
});

// Load responsive tables
$(document).ready(function() {
    $('.table-responsive').each(function(index) {
        $( this ).DataTable({ responsive: true })
    });
});
