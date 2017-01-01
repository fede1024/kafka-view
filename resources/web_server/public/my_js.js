function getCookie(name) {
  var value = "; " + document.cookie;
  var parts = value.split("; " + name + "=");
  if (parts.length == 2) return parts.pop().split(";").shift();
}

$(document).ready(function() {
    console.log(document.cookie);
    $("#request_time").text(getCookie("request_time") + " ms");
    //$("#request_time").text(document.cookie);
});

jQuery.fn.dataTable.ext.type.order['file-size-pre'] = function ( data ) {
    var matches = data.match( /^(\d+(?:\.\d+)?)\s*([a-z]+)/i );
    var multipliers = {
        b:  1,
        kb: 1000,
        kib: 1024,
        mb: 1000000,
        mib: 1048576,
        gb: 1000000000,
        gib: 1073741824,
        tb: 1000000000000,
        tib: 1099511627776,
        pb: 1000000000000000,
        pib: 1125899906842624
    };
    if (matches) {
        var multiplier = multipliers[matches[2].toLowerCase()];
        return parseFloat( matches[1] ) * multiplier;
    } else {
        return -1;
    };
};

// Load responsive tables
$(document).ready(function() {
    $('.load-datatable').each(function(index) {
        $( this ).dataTable({
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "columnDefs": [
                { "type": "file-size", "targets": 2 }
            ]
        });
    });
});

$(document).ready(function(){
    $('[data-toggle="tooltip"]').tooltip();
});
