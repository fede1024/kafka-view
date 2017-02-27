$(document).ready(function() {
    var url = "/meta/request_time/" + $("#request_id").html() + "/"
    $.ajax({url: url, success: function(result){
        $("#request_time").html(result);
    }});
});

jQuery.fn.dataTable.ext.type.order['file-size-pre'] = function (data) {
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

jQuery.fn.dataTable.ext.type.order['my-numeric-pre'] = function (data) {
    var matches = data.match( /^(\d+(?:\.\d+)?)/ );
    if (matches) {
        return parseFloat(matches[1]);
    } else {
        return -1;
    };
};

jQuery.fn.dataTable.ext.type.order['my-err-pre'] = function (data) {
    if (data.indexOf("times") !== -1) {
        return 2; // Error
    } else {
        return 0; // Ok
    };
};

// Load responsive tables
$(document).ready(function() {
    $('#datatable-broker').each(function(index) {
        $(this).dataTable({
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": {
              "search": "Regex search:"
            },
            "columnDefs": [
                { "targets": [2, 3], "type": "my-numeric" }
            ]
        });
    });
    $('#datatable-topic').each(function(index) {
        $(this).dataTable({
            "search": { "regex": true},
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": {
              "search": "Regex search:"
            },
            "columnDefs": [
                { "type": "my-numeric",  "targets": [0] }
                // { "orderable": false, "targets": [4] },
                // { "searchable": false, "targets": [0, 1, 2, 3, 4] },
                // { "type": "my-error", "targets": [1] }
            ]
        });
        $(this).parents('.loader-parent-marker').children('.table-loader-marker').css({"display": "none"});
        $(this).css({"display": "table"})
    });
    $('#datatable-consumer').each(function(index) {
        $(this).dataTable({
            "search": { "regex": true},
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": {
              "search": "Regex search:"
            },
            "columnDefs": [ ]
        });
        $(this).parents('.loader-parent-marker').children('.table-loader-marker').css({"display": "none"});
        $(this).css({"display": "table"})
    });
    $('#datatable-groups').each(function(index) {
        $(this).dataTable({
            "search": { "regex": true},
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": {
              "search": "Regex search:"
            },
            "columnDefs": [
                { "type": "my-numeric",  "targets": [2] }
            ]
        });
        $(this).parents('.loader-parent-marker').children('.table-loader-marker').css({"display": "none"});
        $(this).css({"display": "table"})
    });
});

$(document).ready(function(){
    $('[data-toggle="tooltip"]').tooltip();
});
