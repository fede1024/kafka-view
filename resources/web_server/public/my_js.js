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

function formatBigNumber(bytes, decimals, suffix) {
   if (suffix === undefined) {
       suffix = "";
   }
   if (decimals === undefined) {
       decimals = 3;
   }
   if(bytes == 0) return '0 ' + suffix;
   var k = 1000;
   var sizes = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
   var i = Math.floor(Math.log(bytes) / Math.log(k));
   return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i] + suffix;
}

function formatBytes(bytes, decimals, suffix) {
   if (suffix === undefined) {
       suffix = "";
   }
   if (decimals === undefined) {
       decimals = 3;
   }
   if(bytes == 0) return '0 B' + suffix;
   var k = 1024;
   var sizes = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
   var i = Math.floor(Math.log(bytes) / Math.log(k));
   return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + ' ' + sizes[i] + suffix;
}

function topic_to_url(cluster_id, cell) {
    var topic_name = cell.innerHTML;
    var url = "/clusters/" + cluster_id + "/topic/" + topic_name;
    var link = $('<a>', { text: topic_name, title: 'Topic page', href: url });
    $(cell).html(link);
}

function group_to_url(cluster_id, cell) {
    var group_name = cell.innerHTML;
    var url = "/clusters/" + cluster_id + "/group/" + group_name;
    var link = $('<a>', { text: group_name, title: 'Group page', href: url });
    $(cell).html(link);
}

function error_to_graphic(cell) {
    var error_code = cell.innerHTML;
    if (error_code) {
        var symbol = $('<i>', { class: 'fa fa-times fa-fw', style: 'color: red' });
    } else {
        var symbol = $('<i>', { class: 'fa fa-check fa-fw', style: 'color: green' });
    }
    $(cell).html(symbol);
}

function bytes_to_human(cell, suffix) {
    var bytes = parseInt(cell.innerHTML);
    $(cell).html(formatBytes(bytes, 1, suffix));
}

function big_num_to_human(cell, suffix) {
    var bytes = parseInt(cell.innerHTML);
    $(cell).html(formatBigNumber(bytes, 1, suffix));
}

// Load responsive tables
$(document).ready(function() {
    $('#datatable-topic-ajax').each(function(index) {
        $(this).DataTable({
            "search": { "regex": true},
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": { "search": "Regex search:" },
            "columnDefs": [ ],
            "deferRender": true,
            "createdRow": function(row, data, index) {
                var cluster_id = $(this).attr("data-param");
                topic_to_url(cluster_id, $(row).children()[0]);
                error_to_graphic($(row).children()[2]);
                bytes_to_human($(row).children()[3], "/s");
                big_num_to_human($(row).children()[4], "m/s");
            }
        });
    });
    $('#datatable-offset-ajax').each(function(index) {
        $(this).DataTable({
            "search": { "regex": true},
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": { "search": "Regex search:" },
            "columnDefs": [ ],
            "deferRender": true,
            "createdRow": function(row, data, index) {
                var cluster_id = $(this).attr("data-param");
                group_to_url(cluster_id, $(row).children()[0]);
            }
        });
    });
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
                //{ "type": "my-numeric",  "targets": [0] }
                // { "orderable": false, "targets": [4] },
                // { "searchable": false, "targets": [0, 1, 2, 3, 4] },
                // { "type": "my-error", "targets": [1] }
            ]
        });
        $(this).parents('.loader-parent-marker').children('.table-loader-marker').css({"display": "none"});
        $(this).css({"display": "table"})
    });
    $('#datatable-topology').each(function(index) {
        $(this).dataTable({
            "search": { "regex": true},
            "lengthMenu": [ [10, 50, -1], [10, 50, "All"] ],
            "language": { "search": "Regex search:" }
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
