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
   if (value == 0) {
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
    if (bytes == -1) {
        $(cell).html("Unknown");
    } else {
        $(cell).html(formatToHuman(bytes, 1, suffix, 1024, sizes));
    }
}

function big_num_to_human(cell, suffix) {
    var value = parseInt(cell.innerHTML);
    var sizes = [' ', ' K', ' M', ' G'];
    if (value == -1) {
        $(cell).html("Unknown");
    } else {
        $(cell).html(formatToHuman(value, 1, suffix, 1000, sizes));
    }
}

function broker_to_url(cluster_id, cell) {
    var broker_name = cell.innerHTML;
    var url = "/clusters/" + cluster_id + "/brokers/" + broker_name;
    var link = $('<a>', { text: broker_name, title: 'Broker page', href: url });
    $(cell).html(link);
}

function topic_to_url(cluster_id, cell) {
    var topic_name = cell.innerHTML;
    var url = "/clusters/" + cluster_id + "/topics/" + topic_name;
    var link = $('<a>', { text: topic_name, title: 'Topic page', href: url });
    $(cell).html(link);
}

function group_to_url(cluster_id, cell) {
    var group_name = cell.innerHTML;
    var url = "/clusters/" + cluster_id + "/groups/" + group_name;
    var link = $('<a>', { text: group_name, title: 'Group page', href: url });
    $(cell).html(link);
}

function cluster_to_url(cell) {
    var cluster_id = cell.innerHTML;
    var url = "/clusters/" + cluster_id;
    var link = $('<a>', { text: cluster_id, title: 'Cluster page', href: url });
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
            "columnDefs": [
                { "className": "dt-body-right", "targets": [ 2, 3 ] }
            ],
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
            "columnDefs": [
                { "className": "dt-body-right", "targets": [ 1, 2, 3, 4 ] }
            ],
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
            "columnDefs": [
                { "className": "dt-body-right", "targets": [ 2, 3 ] }
            ],
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
            "columnDefs": [
                { "className": "dt-body-right", "targets": [ 2, 3, 4, 5 ] }
            ],
            "processing": true,
            "deferRender": true,
            "stateSave": true,
            "createdRow": function(row, data, index) {
                var cluster_id = $(this).attr("data-param");
                var row = $(row).children();
                topic_to_url(row[0].innerHTML, row[1]);
                cluster_to_url(row[0]);
                error_to_graphic(row[3]);
                bytes_to_human(row[4], "/s");
                big_num_to_human(row[5], "msg/s");
            }
        });
    });
    $('#datatable-group-search-ajax').each(function(index) {
        $(this).DataTable({
            "searching": false,
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "pageLength": 50,
            "columnDefs": [
                { "className": "dt-body-right", "targets": [ 3, 4 ] }
            ],
            "processing": true,
            "deferRender": true,
            "stateSave": true,
            "createdRow": function(row, data, index) {
                var row = $(row).children();
                group_to_url(row[0].innerHTML, row[1]);
                cluster_to_url(row[0]);
            }
        });
    });
    $('#datatable-internals-cache-brokers-ajax').each(function(index) {
        var table = $(this).DataTable({
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "pageLength": 50,
            "processing": true,
            "deferRender": true,
            "stateSave": true
        });
        setInterval( function () {
            table.ajax.reload();
        }, 20000 );
    });
    $('#datatable-internals-cache-metrics-ajax').each(function(index) {
        var table = $(this).DataTable({
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, 200, -1], [10, 50, 200, "All"] ],
            "pageLength": 50,
            "processing": true,
            "deferRender": true,
            "stateSave": true
        });
        setInterval( function () {
            table.ajax.reload();
        }, 20000 );
    });
    $('#datatable-internals-live-consumers-ajax').each(function(index) {
        var table = $(this).DataTable({
            "ajax": $(this).attr("data-url"),
            "lengthMenu": [ [10, 50, -1], [10, 50, "All"] ],
            "pageLength": 10,
            "processing": true,
            "deferRender": true,
            "stateSave": true
        });
        setInterval( function () {
            table.ajax.reload();
        }, 20000 );
    });
});

function truncate(string, max_len) {
   if (string.length > max_len)
      return string.substring(0,max_len) + '...';
   else
      return string;
}

function isScrolledToBottom(div) {
    var div = div[0];
    return div.scrollHeight - div.clientHeight <= div.scrollTop + 1;
}

function scroll_to_bottom(div) {
    var div = div[0];
    div.scrollTop = div.scrollHeight - div.clientHeight;
}

var max_msg_count = 1000;
var max_msg_length = 1024;
var poll_interval = 1000;

function background_tailer(cluster_id, topic_name, tailer_id) {
  var url = '/api/test/' + cluster_id + '/' + topic_name + '/' + tailer_id;
  $.ajax({
    url: url,
    success: function(data) {
      var div_tailer = $('div.topic_tailer');
      var bottom = isScrolledToBottom(div_tailer);
      messages = JSON.parse(data);
      for (var i = 0; i < messages.length; i++) {
        var message = messages[i];
        var p = $("<p>", {class: "message"});
        p.append(truncate(message[2], max_msg_length));
        div_tailer.append(p);
      }
      if (bottom)
          scroll_to_bottom(div_tailer);
      var message_count = div_tailer.children().length;
      if (message_count > max_msg_count)
          div_tailer.children().slice(0, message_count - max_msg_count).remove();
    },
    error: function(data) {
      console.log("error");
    },
    complete: function() {
      // Schedule the next request when the current one's complete
      setTimeout(function(){background_tailer(cluster_id, topic_name, tailer_id)}, poll_interval);
    }
  });
}

// Load topic tailers
$(document).ready(function() {
    $('.topic_tailer').each(function(index) {
        var cluster_id = $(this).attr("data-cluster");
        var topic_name = $(this).attr("data-topic");
        var tailer_id = $(this).attr("data-tailer");
        background_tailer(cluster_id, topic_name, tailer_id);
    });
});

$(document).ready(function(){
    $('[data-toggle="tooltip"]').tooltip();
    $(window).resize();
});
