jQuery.fn.dataTable.ext.type.order['num-or-str-pre'] = function (data) {
    var matches = data.match( /^(\d+(?:\.\d+)?)/ );
    if (matches) {
        return parseFloat(matches[1]);
    } else {
        return -1;
    };
};

// jQuery.fn.dataTable.ext.type.order['my-err-pre'] = function (data) {
//     if (data.indexOf("times") !== -1) {
//         return 2; // Error
//     } else {
//         return 0; // Ok
//     };
//};

function formatToHuman(value, decimals, suffix, k, sizes) {
   if (suffix === undefined) {
       suffix = "";
   }
   if (decimals === undefined) {
       decimals = 3;
   }
   if(value == 0) {
       var i = 0;
       var result = 0;
   } else {
       var i = Math.floor(Math.log(value) / Math.log(k));
       var result = parseFloat((value / Math.pow(k, i)).toFixed(decimals));
   }
   return $('<span>', { text: result + sizes[i] + suffix, title: value }).tooltip();
}

function bytes_to_human(cell, suffix) {
    var bytes = parseInt(cell.innerHTML);
    var sizes = [' B', ' KiB', ' MiB', ' GiB', ' TiB', ' PiB'];
    $(cell).html(formatToHuman(bytes, 1, suffix, 1024, sizes));
}

function big_num_to_human(cell, suffix) {
    var bytes = parseInt(cell.innerHTML);
    var sizes = [' ', ' K', ' M', ' G'];
    $(cell).html(formatToHuman(bytes, 1, suffix, 1000, sizes));
}

function broker_to_url(cluster_id, cell) {
    var broker_name = cell.innerHTML;
    var url = "/cluster/" + cluster_id + "/broker/" + broker_name;
    var link = $('<a>', { text: broker_name, title: 'Broker page', href: url });
    $(cell).html(link);
}

function topic_to_url(cluster_id, cell) {
    var topic_name = cell.innerHTML;
    var url = "/cluster/" + cluster_id + "/topic/" + topic_name;
    var link = $('<a>', { text: topic_name, title: 'Topic page', href: url });
    $(cell).html(link);
}

function group_to_url(cluster_id, cell) {
    var group_name = cell.innerHTML;
    var url = "/cluster/" + cluster_id + "/group/" + group_name;
    var link = $('<a>', { text: group_name, title: 'Group page', href: url });
    $(cell).html(link);
}

function error_to_graphic(cell) {
    var error_code = cell.innerHTML;
    if (error_code) {
        var symbol = $('<i>', { class: 'fa fa-times fa-fw', style: 'color: red', title: error_code });
    } else {
        var symbol = $('<i>', { class: 'fa fa-check fa-fw', style: 'color: green', title: 'No error' });
    }
    symbol.tooltip();
    $(cell).html(symbol);
}

$(document).ready(function() {
    $('#datatable-brokers-ajax').each(function(index) {
        $(this).DataTable({
            "search": { "regex": true},
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": { "search": "Regex search:" },
            "columnDefs": [ ],
            "processing": true,
            "deferRender": true,
            "stateSave": true,
            "createdRow": function(row, data, index) {
                var cluster_id = $(this).attr("data-param");
                broker_to_url(cluster_id, $(row).children()[0]);
                bytes_to_human($(row).children()[2], "/s");
                big_num_to_human($(row).children()[3], "msg/s");
            }
        });
    });
    $('#datatable-topics-ajax').each(function(index) {
        $(this).DataTable({
            "search": { "regex": true},
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": { "search": "Regex search:" },
            "processing": true,
            "columnDefs": [ ],
            "deferRender": true,
            "stateSave": true,
            "createdRow": function(row, data, index) {
                var cluster_id = $(this).attr("data-param");
                topic_to_url(cluster_id, $(row).children()[0]);
                error_to_graphic($(row).children()[2]);
                bytes_to_human($(row).children()[3], "/s");
                big_num_to_human($(row).children()[4], "msg/s");
            }
        });
    });
    $('#datatable-groups-ajax').each(function(index) {
        $(this).DataTable({
            "search": { "regex": true},
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": { "search": "Regex search:" },
            "columnDefs": [ ],
            "processing": true,
            "deferRender": true,
            stateSave: true,
            "createdRow": function(row, data, index) {
                var cluster_id = $(this).attr("data-param");
                group_to_url(cluster_id, $(row).children()[0]);
            }
        });
    });
    $('#datatable-topology-ajax').each(function(index) {
        $(this).DataTable({
            "search": { "regex": true},
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": { "search": "Regex search:" },
            "columnDefs": [ ],
            "processing": true,
            "deferRender": true,
            stateSave: true,
            "createdRow": function(row, data, index) {
                var cluster_id = $(this).attr("data-param");
                broker_to_url(cluster_id, $(row).children()[1]);
                error_to_graphic($(row).children()[4]);
            }
        });
    });
    $('#datatable-group-members-ajax').each(function(index) {
        $(this).DataTable({
            "search": { "regex": true},
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": { "search": "Regex search:" },
            "columnDefs": [ ],
            "processing": true,
            "deferRender": true,
            stateSave: true
        });
    });
    $('#datatable-group-offsets-ajax').each(function(index) {
        var table = $(this).DataTable({
            "search": { "regex": true},
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "language": { "search": "Regex search:" },
            "columnDefs": [
                { "className": "dt-body-right", "targets": [ 1, 2, 3, 4, 5 ] },
                { "type": "num-or-str", "targets": [ 5 ] }
            ],
            "processing": true,
            "deferRender": true,
            stateSave: true,
            "createdRow": function(row, data, index) {
                var cluster_id = $(this).attr("data-param");
                topic_to_url(cluster_id, $(row).children()[0]);
            }
        });
        setInterval( function () {
            table.ajax.reload();
        }, 20000 );
    });
    $('#datatable-topic-search-ajax').each(function(index) {
        $(this).DataTable({
            "searching": false,
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "pageLength": 50,
            "language": { "search": "Regex search:" },
            "columnDefs": [ ],
            "processing": true,
            "deferRender": true,
            stateSave: true
        });
    });
});

$(document).ready(function(){
    $('[data-toggle="tooltip"]').tooltip();
    $(window).resize();
});
