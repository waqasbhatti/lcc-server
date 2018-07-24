/*global $, jQuery, math, moment, oboe */

/*
lcc-server.js - Waqas Bhatti (wbhatti@astro.princeton.edu) - Jun 2018
License: MIT. See the LICENSE file for details.

This contains JS to drive the LCC server interface.

*/

var lcc_ui = {

    // this holds intervals for lazy background checking
    intervals: {},

    // alert types: 'primary', 'secondary', 'success', 'danger', 'warning',
    //              'info', 'light', 'dark'
    alert_box: function(message, alert_type) {

        var alert = '<div class="mt-2 alert alert-' + alert_type +
            ' alert-dismissible fade show"' +
            ' role="alert">' + message +
            '<button type="button" class="close" data-dismiss="alert" ' +
            'aria-label="Close"><span aria-hidden="true">&times;</span>' +
            '</button></div>';

        // can stack multiple alerts
        $('#alert-box').append(alert);

    },


    // this wires up all the controls
    action_setup: function () {

        // bind the form submit for the cone search
        $('#conesearch-form').on('submit', function (event) {

            event.preventDefault();
            lcc_search.do_conesearch();

        });

        // bind the form submit for the cone search
        $('#ftsquery-form').on('submit', function (event) {

            event.preventDefault();
            lcc_search.do_ftsquery();

        });

        // bind the form submit for the cone search
        $('#columnsearch-form').on('submit', function (event) {

            event.preventDefault();
            lcc_search.do_columnsearch();

        });

        // bind the form submit for the cone search
        $('#xmatch-form').on('submit', function (event) {

            event.preventDefault();
            lcc_search.do_xmatch();

        });

        // bind the lcc-filtertarget so an Enter clicks the add-filter button
        $('.lcc-filtertarget').on('keyup', function (evt) {

            if (evt.key == 'Enter') {

                // get the searchtype and click the appropriate add button
                var searchtype = $(this).attr('data-searchtype');
                $('#' + searchtype + '-filter-add').click();

            }

        });

        // bind the conesearch-filter-add button
        $('.lcc-filter-add').on('click', function (evt) {

            evt.preventDefault();

            // get which search type this is and look up the appropriate select
            // and value boxes
            var target = $(this).attr('data-searchtype');
            var filter_col_elem = $('#' + target + '-filtercolumn-select');
            var filter_type_elem = $('#' + target + '-filtercondition-select');
            var filter_val_elem = $('#' + target + '-filtertarget');

            // look up what those controls say
            var filter_col = filter_col_elem.val();
            var filter_opstr = filter_type_elem.val();
            var filter_op = null;

            if (filter_opstr == 'lt') {
                filter_op = '&lt;';
            }
            else if (filter_opstr == 'gt') {
                filter_op = '&gt;';
            }
            else if (filter_opstr == 'le') {
                filter_op = '&le;';
            }
            else if (filter_opstr == 'ge') {
                filter_op = '&gt;';
            }
            else if (filter_opstr == 'eq') {
                filter_op = '&equals;';
            }
            else if (filter_opstr == 'ne') {
                filter_op = '&ne;';
            }
            else if (filter_opstr == 'ct') {
                filter_op = 'contains';
            }

            // look up the dtype of the column
            var filter_dtype = lcc_search.coldefs[filter_col]['dtype']
                .replace('<','');

            var filter_val = filter_val_elem.val();
            var filter_check = false;

            // check if the filter val and operator matches the expected dtype

            // float
            if (filter_dtype.indexOf('f') != -1) {

                filter_check = (
                    !(isNaN(parseFloat(filter_val.trim()))) &&
                        filter_opstr != 'ct'
                );

            }

            // integer
            else if (filter_dtype.indexOf('i') != -1) {

                filter_check = (
                    !(isNaN(parseInt(filter_val.trim()))) &&
                        filter_opstr != 'ct'
                );

            }

            // string
            else if (filter_val.trim().length > 0 &&
                     ((filter_opstr == 'eq') ||
                      (filter_opstr == 'ne') ||
                      (filter_opstr == 'ct')) ) {

                filter_check = true;

            }

            if (filter_check) {

                // check if the filter bucket is empty
                var filterbucket_elem = $('#' + target + '-filterbucket');
                var filterbucket_nitems = filterbucket_elem.children().length;
                var filter_card_joiner = null;

                if (filterbucket_nitems > 0) {

                    filter_card_joiner =
                        '<select class="mr-3 lcc-filterbucket-chainer">' +
                        '<option value="and" selected>and</option>' +
                        '<option value="or">or</option></select> ';
                }
                else {
                    filter_card_joiner = '';
                }

                // generate the card for this filter
                var filter_card = '<div class="card filterbucket-card mt-1 mx-1" ' +
                    'data-target="' +
                    target.replace('"','').replace("'",'').trim() +
                    '" data-column="' +
                    filter_col.replace('"','').replace("'",'').trim() +
                    '" data-operator="' +
                    filter_opstr.replace('"','').replace("'",'').trim() +
                    '" data-filterval="' +
                    filter_val.replace('"','').replace("'",'').trim() +
                    '" data-dtype="' +
                    filter_dtype.replace('"','').replace("'",'').trim() +
                    '">' +
                    '<div class="card-body">' +
                    filter_card_joiner +
                    '<code>' +
                    filter_col + ' ' + filter_op + ' ' + filter_val + '</code>' +
                    '</div>' +
                    '<div class="card-footer text-right">' +
                    '<a href="#" ' +
                    'class="btn btn-outline-danger btn-sm lcc-filterbucket-remove">' +
                    'Remove filter</a></div>' +
                    '</div>';

                filterbucket_elem.append(filter_card);

            }

            else {
                var msg = target + ', ' +
                    'column: ' +
                    filter_col + ' requires dtype: ' +
                    filter_dtype + ' for its value. ' +
                    ' (current value is: <strong>' +
                    filter_val + '</strong>, current filter op is: ' +
                    '<strong>' + filter_op + '</strong>)';

                lcc_ui.alert_box(msg, 'danger');
            }

        });

        // bind the filter-delete button
        $('.tab-pane').on('click', '.lcc-filterbucket-remove', function(e) {

            e.preventDefault();

            // find our parent
            var thiscard = $(this).parents('.filterbucket-card');

            // kill them
            $(thiscard).remove();

        });

        // bind the lcc-datasets-open link
        $('.lcc-datasets-tabopen').on('click', function (evt) {
            evt.preventDefault();
            $('#datasets-tab').click();
        });

    },


    // this parses the filter control results for any searchtype it is pointed
    // to. returns an SQL string that can be validated by the backend
    parse_column_filters: function(target) {

        var filterbucket_elem = $('#' + target + '-filterbucket');
        var filterbucket_items = filterbucket_elem.children();
        var filterbucket_nitems = filterbucket_items.length;

        var filters = [];

        filterbucket_items.each( function (i, e) {

            // get this card's vals
            var col = $(this).attr('data-column');
            var oper = $(this).attr('data-operator');
            var fval = $(this).attr('data-filterval');
            var dtype = $(this).attr('data-dtype');

            if (dtype.indexOf('U') != -1) {
                fval = "'" + fval + "'";
            }

            var thisfilter = '(' + col + ' ' + oper + ' ' + fval + ')';
            // check if this card has a chainer operator
            var chain_op = $(this)
                .children('div.card-body')
                .children('select').val();

            if (chain_op != undefined && i > 0) {
                thisfilter = chain_op + ' (' + col +
                    ' ' + oper + ' ' + fval + ')';
            }

            filters.push(thisfilter);

        });

        return filters.join(' ');

    },


    get_recent_datasets: function(nrecent, highlight) {

        var geturl = '/api/datasets';
        var getparams = {nsets: nrecent};

        // clear out the recent queries box to keep things fresh
        $('#lcc-datasets-tablerows').empty();

        $.getJSON(geturl, getparams, function (data) {

            var status = data.status;
            var result = data.result;
            var message = data.message;

            // if something broke, alert the user
            if (status != 'ok') {
                lcc_ui.alert_box(message, 'danger');
            }


            // otherwise, fill in the datasets table
            else {

                var rowind = 0;

                for (rowind; rowind < result.length; rowind++) {

                    // setid and queried collections
                    var setid = result[rowind]['setid'];
                    var queriedcolls = result[rowind]['queried_collections'];

                    // number of objects
                    var nobjects = result[rowind]['nobjects'];

                    // query type and params
                    var query_type = result[rowind]['query_type'];
                    var query_params = result[rowind]['query_params'];

                    // product download links
                    var dataset_fpath = result[rowind]['dataset_fpath'];
                    var dataset_shasum = result[rowind]['dataset_shasum'];

                    var lczip_fpath = result[rowind]['lczip_fpath'];
                    var lczip_shasum = result[rowind]['lczip_shasum'];

                    var pfzip_fpath = result[rowind]['pfzip_fpath'];
                    var pfzip_shasum = result[rowind]['pfzip_shasum'];

                    var cpzip_fpath = result[rowind]['cpzip_fpath'];
                    var cpzip_shasum = result[rowind]['cpzip_shasum'];

                    // last updated
                    var lastupdated = result[rowind]['last_updated'];
                    var createdon = result[rowind]['created_on'];

                    //
                    // Set ID column
                    //
                    var table_setid = '<td>' +
                        '<a rel="nofollow" href="/set/' +
                        setid + '">' +
                        setid + '</a><br>' +
                        'collections used: <code>' +
                        queriedcolls + '</code>' +
                        '</td>';

                    //
                    // Objects column
                    //
                    var table_nobjects = '<td>' +
                        nobjects +
                        '</td>';

                    //
                    // Query column
                    //
                    var table_query = '<td>' +
                        'query type: <code>' + query_type + '</code><br>' +
                        'query params: <code>' + query_params + '</code>' +
                        '</td>';

                    //
                    // Products column
                    //
                    var dataset_download = '';
                    var lczip_download = '';
                    var pfzip_download = '';
                    var cpzip_download = '';

                    if (dataset_fpath != null) {
                        dataset_download = '<a rel="nofollow" ' +
                            'href="' + dataset_fpath +
                            '" title="download search results pickle">' +
                            'dataset pickle' +
                            '</a> <span data-toggle="tooltip" title="' +
                            dataset_shasum + '">' +
                            '[SHA256]</span>';
                    }

                    if (lczip_fpath != null) {
                        lczip_download = '<a rel="nofollow" ' +
                            'href="' + lczip_fpath +
                            '" title="download light curves ZIP">' +
                            'light curve ZIP' +
                            '</a> <span data-toggle="tooltip" title="' +
                            lczip_shasum + '">' +
                            '[SHA256]</span>';
                    }

                    if (pfzip_fpath != null) {
                        pfzip_download = '<a rel="nofollow" ' +
                            'href="' + pfzip_fpath +
                            '" title="download period-finding results ZIP">' +
                            'period-finding result pickles ZIP' +
                            '</a> <span data-toggle="tooltip" title="' +
                            pfzip_shasum + '">' +
                            '[SHA256]</span>';
                    }

                    if (cpzip_fpath != null) {
                        cpzip_download = '<a rel="nofollow" ' +
                        'href="' + cpzip_fpath +
                            '" title="download checkplot pickles ZIP">' +
                            'checkplot pickles ZIP' +
                            '</a> <span data-toggle="tooltip" title="' +
                            cpzip_shasum + '">' +
                            '[SHA256]</span>';
                    }

                    // format the column
                    var table_downloadlinks = '<td>' +
                        dataset_download + '<br>' +
                        lczip_download + '<br>' +
                        pfzip_download + '<br>' +
                        cpzip_download + '</td>';


                    //
                    // Last updated columns
                    //
                    var table_lastupdated = '<td>' +
                        'Created: <span data-toggle="tooltip" ' +
                        'title="' + createdon + 'Z">' +
                        moment(createdon + 'Z').fromNow() +
                        '</span><br>' +
                        'Updated: <span data-toggle="tooltip" ' +
                        'title="' + lastupdated + 'Z">' +
                        moment(lastupdated + 'Z').fromNow() +
                        '</span>' +
                        '</td>';

                    //
                    // finally, add this row
                    //
                    var setrow = '<tr>' +
                        table_setid +
                        table_nobjects +
                        table_query +
                        table_downloadlinks +
                        table_lastupdated +
                        '</tr>';

                    if (highlight != undefined &&
                        highlight != null &&
                        highlight == setid) {

                        setrow = '<tr class="table-primary">' +
                            table_setid +
                            table_nobjects +
                            table_query +
                            table_downloadlinks +
                            table_lastupdated +
                            '</tr>';

                    }
                    $('#lcc-datasets-tablerows').append(setrow);

                }

            }

            // at the end, activate the tooltips
            $('[data-toggle="tooltip"]').tooltip();

        }).fail(function (xhr) {
            var message = 'could not get list of recent ' +
                'datasets from the LCC server backend';

            lcc_ui.alert_box(message, 'danger');

        });

    },


    // this gets the latest LC collections
    get_lc_collections: function() {

        var geturl = '/api/collections';

        // clear out the recent queries box to keep things fresh
        $('#lcc-collection-tablerows').empty();

        $.getJSON(geturl, function (data) {

            var status = data.status;
            var result = data.result;
            var message = data.message;

            // if something broke, alert the user
            if (status != 'ok') {
                lcc_ui.alert_box(message, 'danger');
            }

            // otherwise, fill in the collections table
            else {

                var collections = result.collections;

                // store this so we can refer to it later
                lcc_search.collections = collections;

                var collection_ids = collections.collection_id;

                // we'll also store the available columns and their definitions
                lcc_search.columns = available_columns;
                lcc_search.coldefs = collections['columnjson'][0];


                //we use all available columns to figure out the common cols and
                //also the per collection special cols
                var available_columns = result.available_columns;

                var indexed_columns = result.available_indexed_columns;
                var fts_columns = result.available_fts_columns;

                lcc_search.indexcols = indexed_columns;
                lcc_search.ftscols = fts_columns;

                var coll_idx = 0;

                for (coll_idx; coll_idx < collection_ids.length; coll_idx++) {

                    //
                    // name column
                    //
                    var db_collid = collections.db_collection_id[coll_idx];
                    var collname = collections.name[coll_idx];
                    var table_column_name = '<td width="80">' +
                        collname + ' (<code>' + db_collid + '</code>)' +
                        '</td>';

                    //
                    // description column
                    //
                    var desc = collections.description[coll_idx];
                    var nobjects = collections.nobjects[coll_idx];
                    var table_column_desc = '<td width="100">' +
                        desc + '<br><br>Number of objects: <code>' +
                        nobjects +
                        '</code></td>';

                    //
                    // extent column
                    //
                    var minra = collections.minra[coll_idx];
                    var maxra = collections.maxra[coll_idx];
                    var mindecl = collections.mindecl[coll_idx];
                    var maxdecl = collections.maxdecl[coll_idx];

                    var center_ra = math.format((minra + maxra)/2.0,5);
                    var center_decl = math.format((mindecl + maxdecl)/2.0,5);
                    minra = math.format(minra,5);
                    maxra = math.format(maxra,5);
                    mindecl = math.format(mindecl,5);
                    maxdecl = math.format(maxdecl,5);

                    var table_column_coords = '<td width="100">' +
                        'center: <code>(' +
                        center_ra + ', ' + center_decl +
                        ')</code><br>' +
                        'SE: <code>(' +
                        maxra + ', ' + mindecl +
                        ')</code><br>' +
                        'NW: <code>(' + minra + ', ' + maxdecl + ')</code>' +
                        '</td>';

                    //
                    // coldesc column
                    //

                    // get the column list for this collection
                    var columns =
                        collections.columnlist[coll_idx].split(',').sort();

                    // get the indexed columns for the collection
                    var indexedcols =
                        collections.indexedcols[coll_idx].split(',').sort();

                    // get the FTS columns for this collection
                    var ftscols =
                        collections.ftsindexedcols[coll_idx].split(',').sort();

                    var colind = 0;

                    // we'll make an list with three sections
                    // 1. indexed columns
                    // 2. full-text search enabled columns
                    // 3. other columns
                    var formatted_colspec = [];

                    for (colind; colind < columns.length; colind++) {

                        var thiscol = columns[colind];

                        var thiscol_title =
                            collections.columnjson[coll_idx][thiscol]['title'];
                        var thiscol_desc =
                            collections.columnjson[coll_idx][thiscol][
                                'description'
                            ];

                        if (thiscol_title != null && thiscol_desc != null) {

                            var col_popover = '<span class="pop-span" ' +
                                'data-toggle="popover" ' +
                                'data-placement="top" ' +
                                'data-title="' + thiscol_title + '" ' +
                                'data-content="' + thiscol_desc + '" ' +
                                'data-html="true">' + thiscol + '</span>';

                            if (ftscols.indexOf(thiscol) != -1) {
                                formatted_colspec.push(
                                    '<span class="fts-col">' +
                                        col_popover + '</span>'
                                );
                            }
                            else if (indexedcols.indexOf(thiscol) != -1) {
                                formatted_colspec.push(
                                    '<span class="kdtree-col">' +
                                        col_popover + '</span>'
                                );
                            }
                            else {
                                formatted_colspec.push(col_popover);
                            }

                        }

                    }
                    var formatted_column_list = formatted_colspec.join(', ');

                    //
                    // build this table row
                    //
                    var table_row = '<tr>' +
                        table_column_name +
                        table_column_desc +
                        table_column_coords +
                        '<td width="250"><code>' +
                        formatted_column_list +
                        '</code></td>' +
                        '</tr>';
                    $('#lcc-collection-tablerows').append(table_row);

                }

                // update the collection select boxes
                var collection_selectboxes = $('.lcc-collection-select');

                collection_selectboxes.each(function (e, i) {

                    var thisbox = $(this);

                    thisbox.append('<option value="' +
                                   db_collid +
                                   '">' +
                                   collname +
                                   '</option>');

                });

                // update the column select boxes
                var column_selectboxes = $('.lcc-column-select');

                column_selectboxes.each(function (e, i) {

                    var thisbox = $(this);
                    var column_ind = 0;
                    for (column_ind; column_ind < columns.length; column_ind++) {
                        thisbox.append('<option value="' +
                                       columns[column_ind] +
                                       '">' +
                                       columns[column_ind] +
                                       '</option>');
                    }

                });

                // update the filter column select boxes
                var filter_selectboxes = $('.lcc-filtercolumn-select');

                filter_selectboxes.each(function (e, i) {

                    var thisbox = $(this);
                    var column_ind = 0;
                    for (column_ind; column_ind < columns.length; column_ind++) {
                        thisbox.append('<option value="' +
                                       columns[column_ind] +
                                       '">' +
                                       columns[column_ind] +
                                       '</option>');
                    }

                });

                // update the filter column select boxes
                var filter_sortboxes = $('.lcc-sortcolumn-select');

                filter_sortboxes.each(function (e, i) {

                    var thisbox = $(this);
                    var column_ind = 0;
                    var done_with_selected = false;

                    for (column_ind; column_ind < columns.length; column_ind++) {

                        // special: we'll sort by sdssr by default if it's there
                        // if it's not, we'll sort by objectid

                        if ((columns[column_ind] == 'sdssr') &&
                            !done_with_selected) {

                            thisbox.append('<option value="' +
                                           columns[column_ind] +
                                           '" selected>' +
                                           columns[column_ind] +
                                           '</option>');
                            done_with_selected = true;

                        }
                        else if ((columns[column_ind] == 'sdssr') &&
                                 !done_with_selected) {

                            thisbox.append('<option value="' +
                                           columns[column_ind] +
                                           '" selected>' +
                                           columns[column_ind] +
                                           '</option>');
                            done_with_selected = true;

                        }
                        else {
                            thisbox.append('<option value="' +
                                           columns[column_ind] +
                                           '">' +
                                           columns[column_ind] +
                                           '</option>');

                        }
                    }

                });


                // update the FTS query FTS column list
                $('#ftsquery-column-list')
                    .html(fts_columns.sort().join(', '));

                // update the indexed column list
                $('#columnsearch-indexed-columnlist')
                    .html(indexed_columns.sort().join(', '));
            }

            // at the end, activate the tooltips and popovers
            $('[data-toggle="tooltip"]').tooltip();
            $('[data-toggle="popover"]').popover();

        }).fail(function (xhr) {
            var message = 'could not get list of recent ' +
                'LC collections from the LCC server backend';

            lcc_ui.alert_box(message, 'danger');

        });

    }

};


// this contains functions to drive the search controls and send the requests to
// the backend
var lcc_search = {

    // this holds the current collections
    collections: null,

    // this holds the current columns
    columns: null,

    // this holds the current column definitions
    coldefs: null,

    // this holds the FTS index columns
    ftscols: null,

    // this holds the indexed columns
    indexedcols: null,

    // this runs an FTS query
    do_columnsearch: function () {

        var proceed = false;

        // get the collections to use
        var collections = $('#columnsearch-collection-select').val();

        if (collections.length == 0) {
            collections = null;
        }

        // get the columns to retrieve
        var columns = $('#columnsearch-column-select').val();

        // get the ispublic parameter
        var ispublic = $('#columnsearch-ispublic').prop('checked');

        if (ispublic) {
            ispublic = 1;
        }
        else {
            ispublic = 0;
        }

        // parse the extra filters
        var filters = lcc_ui.parse_column_filters('columnsearch');

        // if there are no filters, we won't be fetching the entire catalog
        if (filters.length == 0) {
            filters = null;
        }
        else {
            proceed = true;
        }

        // get the sort column and order
        var sortcol = $('#columnsearch-sortcolumn-select').val();
        var sortorder = $('#columnsearch-sortorder-select').val();

        var geturl = '/api/columnsearch';
        var getparams = {result_ispublic: ispublic,
                         collections: collections,
                         columns: columns,
                         filters: filters,
                         sortcol: sortcol,
                         sortorder: sortorder};

        console.log(getparams);
        getparams = $.param(getparams);
        geturl = geturl + '?' + getparams;

        if (proceed) {

            // disable the submit button until we return
            // FIXME: may need to turn off handler as well
            $('#columnsearch-submit').attr('disabled',true);

            // flash the query status icon
            $('#search-notification-icon').addClass('animated flash infinite');

            // add a badge with number of currently running queries to the query
            // status tab
            var nrun = parseInt($('#lcc-qstatus-run').attr('data-nrun'));
            nrun = nrun + 1;
            $('#lcc-qstatus-run').attr('data-nrun',nrun);
            $('#lcc-qstatus-run').html('<span class="badge badge-primary">' +
                                           nrun +
                                           '</span>');


            // we'll use oboe to fire the query and listen on events that fire
            // when we detect a 'message' key in the JSON
            oboe(geturl)
                .node('{message}', function (msgdata) {

                    var status_color = '';

                    // for now, we'll just write each status message to the
                    // status tab
                    if (msgdata.status == 'failed') {
                        status_color = 'class="table-danger"';
                    }
                    else if (msgdata.status == 'background') {
                        status_color = 'class="table-warning"';
                    }
                    else if (msgdata.status == 'queued') {
                        status_color = 'class="table-active"';
                    }
                    else if (msgdata.status == 'ok') {
                        status_color = 'class="table-primary"';
                    }

                    // format the time
                    var msgtime = moment(msgdata.time).toString();
                    var msg_setid = 'unknown';

                    // if there's a setid, use it
                    if (msgdata.result != null && 'setid' in msgdata.result) {
                        msg_setid = msgdata.result.setid;
                    }

                    $('#query-status-stream').prepend(
                        '<tr ' + status_color + '>' +
                            '<td>' + msgtime + '</td>' +
                            '<td>' + msg_setid + '</td>' +
                            '<td>' + msgdata.status + '</td>' +
                            '<td><code>' + msgdata.message + '</code></td>' +
                            '</tr>'
                    );

                    // re-enable the submit button until we return
                    // FIXME: may need to turn on handler as well
                    $('#columnsearch-submit').attr('disabled',false);

                    // when we finish, handle the UI teardown
                    if (msgdata.status == 'ok') {

                        // check how many queries are running
                        var nrun =
                            parseInt($('#lcc-qstatus-run')
                                     .attr('data-nrun'));
                        nrun = nrun - 1;
                        $('#lcc-qstatus-run')
                            .attr('data-nrun',nrun);

                        // turn off the flashing if no queries are currently
                        // running
                        if (nrun == 0 || nrun < 0) {

                            $('#search-notification-icon')
                                .removeClass('animated flash infinite');
                            $('#lcc-qstatus-run').empty();

                        }

                        // otherwise, just subtract one from the status
                        else {
                            $('#lcc-qstatus-run').html(
                                '<span class="badge badge-primary">' +
                                    nrun +
                                    '</span>'
                            );
                        }

                        // if the query is public, flash the dataset tab
                        if (ispublic) {

                            // hit the /api/datasets URL to update the datasets
                            // also highlight the row with our query result in it
                            lcc_ui.get_recent_datasets(25, msgdata.result.setid);

                            // bounce the recent datasets tab if the dataset is done
                            $('#datasets-tab-icon').addClass('animated bounce');
                            window.setTimeout(function () {
                                $('#datasets-tab-icon').removeClass('animated bounce');
                            }, 3000);

                        }

                    }

                    // if this query moved from running to background, then
                    // handle the UI change
                    // we'll dim the status card for this
                    else if (msgdata.status == 'background') {

                        // check how many queries are running
                        nrun = parseInt($('#lcc-qstatus-run')
                                        .attr('data-nrun'));
                        nrun = nrun - 1;
                        $('#lcc-qstatus-run')
                            .attr('data-nrun',nrun);

                        // turn off the flashing if no queries are currently
                        // running
                        if (nrun == 0 || nrun < 0) {

                            $('#search-notification-icon')
                                .removeClass('animated flash infinite');
                            $('#lcc-qstatus-run').empty();

                        }

                        // otherwise, just subtract one from the status
                        else {
                            $('#lcc-qstatus-run').html(
                                '<span class="badge badge-primary">' +
                                    nrun +
                                    '</span>'
                            );
                        }

                        // update number of queries in background
                        var nback =
                            parseInt($('#lcc-qstatus-back')
                                     .attr('data-nback'));
                        nback = nback + 1;
                        $('#lcc-qstatus-back')
                            .attr('data-nback',nback);

                        $('#lcc-qstatus-back').html(
                            '<span class="badge badge-warning">' +
                                nback +
                                '</span>'
                        );

                        // notify the user that the query is in the background
                        var alertmsg = 'Query ' +
                            msgdata.result.setid +
                            ' was moved to ' +
                            'a background queue ' +
                            ' after 15 seconds. ' +
                            'Results will appear at ' +
                            '<a rel="noopener noreferrer"' +
                            'target="_blank" href="/set/' +
                            msgdata.result.setid + '">its dataset page.</a>';

                        lcc_ui.alert_box(alertmsg, 'warning');

                        // remove the background icon from the queue status
                        // after 5 seconds of displaying the alert
                        window.setTimeout(function () {

                            // check how many queries are background
                            var nback =
                                parseInt($('#lcc-qstatus-back')
                                         .attr('data-nback'));
                            nback = nback - 1;
                            $('#lcc-qstatus-back')
                                .attr('data-nback',nback);

                            // turn off the flashing if no queries are currently
                            // backning
                            if (nback == 0 || nback < 0) {

                                $('#search-notification-icon')
                                    .removeClass('animated flash infinite');
                                $('#lcc-qstatus-back').empty();

                            }

                            // otherwise, just subtract one from the status
                            else {
                                $('#lcc-qstatus-back').html(
                                    '<span class="badge badge-warning">' +
                                        nback +
                                        '</span>'
                                );
                            }

                        }, 7500);

                    }

                    // if this query failed, then handle the UI change
                    // we'll make  the status card red for this
                    else if (msgdata.status == 'failed') {

                        // notify the user that the query is in the background
                        alertmsg = msgdata.message;
                        lcc_ui.alert_box(alertmsg, 'danger');

                        // check how many queries are running
                        nrun = parseInt($('#lcc-qstatus-run')
                                        .attr('data-nrun'));
                        nrun = nrun - 1;
                        $('#lcc-qstatus-run')
                            .attr('data-nrun',nrun);

                        // turn off the flashing if no queries are currently
                        // running
                        if (nrun == 0 || nrun < 0) {

                            $('#search-notification-icon')
                                .removeClass('animated flash infinite');
                            $('#lcc-qstatus-run').empty();

                        }

                        // otherwise, just subtract one from the status
                        else {
                            $('#lcc-qstatus-run').html(
                                '<span class="badge badge-primary">' +
                                    nrun +
                                    '</span>'
                            );
                        }

                    }

                })
                .fail(function (err) {

                    // err has thrown, statusCode, body, jsonBody
                    console.log(err.statusCode);
                    console.log(err.body);

                    // stop the blinky!
                    $('#search-notification-icon')
                        .removeClass('animated flash infinite');

                    $('#lcc-qstatus-back').empty();
                    $('#lcc-qstatus-run').empty();

                    // alert the user that the query failed
                    lcc_ui.alert_box(
                        'Query failed because the search backend ' +
                            'crashed with an error code: <code>' +
                            err.statusCode +
                            '</code>! See the console for ' +
                            'more details...', 'danger'
                    );


                });

        }
        else {
            var error_message =
                "No valid column filters were found for the column search query.";
            lcc_ui.alert_box(error_message, 'danger');
        }

    },

    // this runs an FTS query
    do_ftsquery: function () {

        var proceed = false;

        // get the collections to use
        var collections = $('#ftsquery-collection-select').val();

        if (collections.length == 0) {
            collections = null;
        }

        // get the columns to retrieve
        var columns = $('#ftsquery-column-select').val();

        // get the coord parameter
        var ftstext = $('#ftsquery-query').val().trim();
        if (ftstext.length > 0) {
            proceed = true;
        }

        // get the ispublic parameter
        var ispublic = $('#ftsquery-ispublic').prop('checked');

        if (ispublic) {
            ispublic = 1;
        }
        else {
            ispublic = 0;
        }

        // parse the extra filters
        var filters = lcc_ui.parse_column_filters('ftsquery');
        if (filters.length == 0) {
            filters = null;
        }

        var geturl = '/api/ftsquery';
        var getparams = {ftstext: ftstext,
                         result_ispublic: ispublic,
                         collections: collections,
                         columns: columns,
                         filters: filters};


        console.log(getparams);
        getparams = $.param(getparams);
        geturl = geturl + '?' + getparams;

        if (proceed) {

            // disable the submit button until we return
            // FIXME: may need to turn off handler as well
            $('#ftsquery-submit').attr('disabled',true);

            // flash the query status icon
            $('#search-notification-icon').addClass('animated flash infinite');

            // add a badge with number of currently running queries to the query
            // status tab
            var nrun = parseInt($('#lcc-qstatus-run').attr('data-nrun'));
            nrun = nrun + 1;
            $('#lcc-qstatus-run').attr('data-nrun',nrun);
            $('#lcc-qstatus-run').html('<span class="badge badge-primary">' +
                                           nrun +
                                           '</span>');


            // we'll use oboe to fire the query and listen on events that fire
            // when we detect a 'message' key in the JSON
            oboe(geturl)
                .node('{message}', function (msgdata) {

                    var status_color = '';

                    // for now, we'll just write each status message to the
                    // status tab
                    if (msgdata.status == 'failed') {
                        status_color = 'class="table-danger"';
                    }
                    else if (msgdata.status == 'background') {
                        status_color = 'class="table-warning"';
                    }
                    else if (msgdata.status == 'queued') {
                        status_color = 'class="table-active"';
                    }
                    else if (msgdata.status == 'ok') {
                        status_color = 'class="table-primary"';
                    }

                    // format the time
                    var msgtime = moment(msgdata.time).toString();
                    var msg_setid = 'unknown';

                    // if there's a setid, use it
                    if (msgdata.result != null && 'setid' in msgdata.result) {
                        msg_setid = msgdata.result.setid;
                    }

                    $('#query-status-stream').prepend(
                        '<tr ' + status_color + '>' +
                            '<td>' + msgtime + '</td>' +
                            '<td>' + msg_setid + '</td>' +
                            '<td>' + msgdata.status + '</td>' +
                            '<td><code>' + msgdata.message + '</code></td>' +
                            '</tr>'
                    );

                    // re-enable the submit button until we return
                    // FIXME: may need to turn on handler as well
                    $('#ftsquery-submit').attr('disabled',false);

                    // when we finish, handle the UI teardown
                    if (msgdata.status == 'ok') {

                        // check how many queries are running
                        var nrun =
                            parseInt($('#lcc-qstatus-run')
                                     .attr('data-nrun'));
                        nrun = nrun - 1;
                        $('#lcc-qstatus-run')
                            .attr('data-nrun',nrun);

                        // turn off the flashing if no queries are currently
                        // running
                        if (nrun == 0 || nrun < 0) {

                            $('#search-notification-icon')
                                .removeClass('animated flash infinite');
                            $('#lcc-qstatus-run').empty();

                        }

                        // otherwise, just subtract one from the status
                        else {
                            $('#lcc-qstatus-run').html(
                                '<span class="badge badge-primary">' +
                                    nrun +
                                    '</span>'
                            );
                        }

                        // if the query is public, flash the dataset tab
                        if (ispublic) {

                            // hit the /api/datasets URL to update the datasets
                            // also highlight the row with our query result in it
                            lcc_ui.get_recent_datasets(25, msgdata.result.setid);

                            // bounce the recent datasets tab if the dataset is done
                            $('#datasets-tab-icon').addClass('animated bounce');
                            window.setTimeout(function () {
                                $('#datasets-tab-icon').removeClass('animated bounce');
                            }, 3000);

                        }

                    }

                    // if this query moved from running to background, then
                    // handle the UI change
                    // we'll dim the status card for this
                    else if (msgdata.status == 'background') {

                        // check how many queries are running
                        nrun =
                            parseInt($('#lcc-qstatus-run')
                                     .attr('data-nrun'));
                        nrun = nrun - 1;
                        $('#lcc-qstatus-run')
                            .attr('data-nrun',nrun);

                        // turn off the flashing if no queries are currently
                        // running
                        if (nrun == 0 || nrun < 0) {

                            $('#search-notification-icon')
                                .removeClass('animated flash infinite');
                            $('#lcc-qstatus-run').empty();

                        }

                        // otherwise, just subtract one from the status
                        else {
                            $('#lcc-qstatus-run').html(
                                '<span class="badge badge-primary">' +
                                    nrun +
                                    '</span>'
                            );
                        }

                        // update number of queries in background
                        var nback =
                            parseInt($('#lcc-qstatus-back')
                                     .attr('data-nback'));
                        nback = nback + 1;
                        $('#lcc-qstatus-back')
                            .attr('data-nback',nback);

                        $('#lcc-qstatus-back').html(
                            '<span class="badge badge-warning">' +
                                nback +
                                '</span>'
                        );

                        // notify the user that the query is in the background
                        var alertmsg = 'Query ' +
                            msgdata.result.setid +
                            ' was moved to ' +
                            'a background queue ' +
                            ' after 15 seconds. ' +
                            'Results will appear at ' +
                            '<a rel="noopener noreferrer"' +
                            'target="_blank" href="/set/' +
                            msgdata.result.setid + '">its dataset page.</a>';

                        lcc_ui.alert_box(alertmsg, 'warning');

                        // remove the background icon from the queue status
                        // after 5 seconds of displaying the alert
                        window.setTimeout(function () {

                            // check how many queries are background
                            var nback =
                                parseInt($('#lcc-qstatus-back')
                                         .attr('data-nback'));
                            nback = nback - 1;
                            $('#lcc-qstatus-back')
                                .attr('data-nback',nback);

                            // turn off the flashing if no queries are currently
                            // backning
                            if (nback == 0 || nback < 0) {

                                $('#search-notification-icon')
                                    .removeClass('animated flash infinite');
                                $('#lcc-qstatus-back').empty();

                            }

                            // otherwise, just subtract one from the status
                            else {
                                $('#lcc-qstatus-back').html(
                                    '<span class="badge badge-warning">' +
                                        nback +
                                        '</span>'
                                );
                            }

                        }, 7500);

                    }

                    // if this query failed, then handle the UI change
                    // we'll make  the status card red for this
                    else if (msgdata.status == 'failed') {

                        // notify the user that the query is in the background
                        alertmsg = msgdata.message;
                        lcc_ui.alert_box(alertmsg, 'danger');

                        // check how many queries are running
                        nrun =
                            parseInt($('#lcc-qstatus-run')
                                     .attr('data-nrun'));
                        nrun = nrun - 1;
                        $('#lcc-qstatus-run')
                            .attr('data-nrun',nrun);

                        // turn off the flashing if no queries are currently
                        // running
                        if (nrun == 0 || nrun < 0) {

                            $('#search-notification-icon')
                                .removeClass('animated flash infinite');
                            $('#lcc-qstatus-run').empty();

                        }

                        // otherwise, just subtract one from the status
                        else {
                            $('#lcc-qstatus-run').html(
                                '<span class="badge badge-primary">' +
                                    nrun +
                                    '</span>'
                            );
                        }

                    }

                })
                .fail(function (err) {

                    // err has thrown, statusCode, body, jsonBody
                    console.log(err.statusCode);
                    console.log(err.body);

                    // stop the blinky!
                    $('#search-notification-icon')
                        .removeClass('animated flash infinite');

                    $('#lcc-qstatus-back').empty();
                    $('#lcc-qstatus-run').empty();

                    // alert the user that the query failed
                    lcc_ui.alert_box(
                        'Query failed because the search backend ' +
                            'crashed with an error code: <code>' +
                            err.statusCode +
                            '</code>! See the console for ' +
                            'more details...', 'danger'
                    );


                });

        }
        else {
            var error_message =
                "No query text found in the FTS query text box.";
            lcc_ui.alert_box(error_message, 'danger');
        }

    },

    // this runs a cone search query
    do_conesearch: function() {

        var proceed = false;

        // get the collections to use
        var collections = $('#conesearch-collection-select').val();

        if (collections.length == 0) {
            collections = null;
        }

        // get the columns to retrieve
        var columns = $('#conesearch-column-select').val();

        // get the coord parameter
        var coords = $('#conesearch-query').val().trim();
        if (coords.length > 0) {
            proceed = true;
        }

        // get the ispublic parameter
        var ispublic = $('#conesearch-ispublic').prop('checked');

        if (ispublic) {
            ispublic = 1;
        }
        else {
            ispublic = 0;
        }

        // parse the extra filters
        var filters = lcc_ui.parse_column_filters('conesearch');
        if (filters.length == 0) {
            filters = null;
        }

        var geturl = '/api/conesearch';
        var getparams = {coords: coords,
                         result_ispublic: ispublic,
                         collections: collections,
                         columns: columns,
                         filters: filters};


        console.log(getparams);
        getparams = $.param(getparams);
        geturl = geturl + '?' + getparams;

        if (proceed) {

            // disable the submit button until we return
            // FIXME: may need to turn off handler as well
            $('#conesearch-submit').attr('disabled',true);

            // flash the query status icon
            $('#search-notification-icon').addClass('animated flash infinite');

            // add a badge with number of currently running queries to the query
            // status tab
            var nrun = parseInt($('#lcc-qstatus-run').attr('data-nrun'));
            nrun = nrun + 1;
            $('#lcc-qstatus-run').attr('data-nrun',nrun);
            $('#lcc-qstatus-run').html('<span class="badge badge-primary">' +
                                           nrun +
                                           '</span>');


            // we'll use oboe to fire the query and listen on events that fire
            // when we detect a 'message' key in the JSON
            oboe(geturl)
                .node('{message}', function (msgdata) {

                    var status_color = '';

                    // for now, we'll just write each status message to the
                    // status tab
                    if (msgdata.status == 'failed') {
                        status_color = 'class="table-danger"';
                    }
                    else if (msgdata.status == 'background') {
                        status_color = 'class="table-warning"';
                    }
                    else if (msgdata.status == 'queued') {
                        status_color = 'class="table-active"';
                    }
                    else if (msgdata.status == 'ok') {
                        status_color = 'class="table-primary"';
                    }

                    // format the time
                    var msgtime = moment(msgdata.time).toString();
                    var msg_setid = 'unknown';

                    // if there's a setid, use it
                    if (msgdata.result != null && 'setid' in msgdata.result) {
                        msg_setid = msgdata.result.setid;
                    }

                    $('#query-status-stream').prepend(
                        '<tr ' + status_color + '>' +
                            '<td>' + msgtime + '</td>' +
                            '<td>' + msg_setid + '</td>' +
                            '<td>' + msgdata.status + '</td>' +
                            '<td><code>' + msgdata.message + '</code></td>' +
                            '</tr>'
                    );

                    // re-enable the submit button until we return
                    // FIXME: may need to turn on handler as well
                    $('#conesearch-submit').attr('disabled',false);

                    // when we finish, handle the UI teardown
                    if (msgdata.status == 'ok') {

                        // check how many queries are running
                        var nrun =
                            parseInt($('#lcc-qstatus-run')
                                     .attr('data-nrun'));
                        nrun = nrun - 1;
                        $('#lcc-qstatus-run')
                            .attr('data-nrun',nrun);

                        // turn off the flashing if no queries are currently
                        // running
                        if (nrun == 0 || nrun < 0) {

                            $('#search-notification-icon')
                                .removeClass('animated flash infinite');
                            $('#lcc-qstatus-run').empty();

                        }

                        // otherwise, just subtract one from the status
                        else {
                            $('#lcc-qstatus-run').html(
                                '<span class="badge badge-primary">' +
                                    nrun +
                                    '</span>'
                            );
                        }

                        // if the query is public, flash the dataset tab
                        if (ispublic) {

                            // hit the /api/datasets URL to update the datasets
                            // also highlight the row with our query result in it
                            lcc_ui.get_recent_datasets(25, msgdata.result.setid);

                            // bounce the recent datasets tab if the dataset is done
                            $('#datasets-tab-icon').addClass('animated bounce');
                            window.setTimeout(function () {
                                $('#datasets-tab-icon').removeClass('animated bounce');
                            }, 3000);

                        }

                    }

                    // if this query moved from running to background, then
                    // handle the UI change
                    // we'll dim the status card for this
                    else if (msgdata.status == 'background') {

                        // check how many queries are running
                        nrun =
                            parseInt($('#lcc-qstatus-run')
                                     .attr('data-nrun'));
                        nrun = nrun - 1;
                        $('#lcc-qstatus-run')
                            .attr('data-nrun',nrun);

                        // turn off the flashing if no queries are currently
                        // running
                        if (nrun == 0 || nrun < 0) {

                            $('#search-notification-icon')
                                .removeClass('animated flash infinite');
                            $('#lcc-qstatus-run').empty();

                        }

                        // otherwise, just subtract one from the status
                        else {
                            $('#lcc-qstatus-run').html(
                                '<span class="badge badge-primary">' +
                                    nrun +
                                    '</span>'
                            );
                        }

                        // update number of queries in background
                        var nback =
                            parseInt($('#lcc-qstatus-back')
                                     .attr('data-nback'));
                        nback = nback + 1;
                        $('#lcc-qstatus-back')
                            .attr('data-nback',nback);

                        $('#lcc-qstatus-back').html(
                            '<span class="badge badge-warning">' +
                                nback +
                                '</span>'
                        );

                        // notify the user that the query is in the background
                        var alertmsg = 'Query ' +
                            msgdata.result.setid +
                            ' was moved to ' +
                            'a background queue ' +
                            ' after 15 seconds. ' +
                            'Results will appear at ' +
                            '<a rel="noopener noreferrer"' +
                            'target="_blank" href="/set/' +
                            msgdata.result.setid + '">its dataset page.</a>';

                        lcc_ui.alert_box(alertmsg, 'warning');

                        // remove the background icon from the queue status
                        // after 5 seconds of displaying the alert
                        window.setTimeout(function () {

                            // check how many queries are background
                            var nback =
                                parseInt($('#lcc-qstatus-back')
                                         .attr('data-nback'));
                            nback = nback - 1;
                            $('#lcc-qstatus-back')
                                .attr('data-nback',nback);

                            // turn off the flashing if no queries are currently
                            // backning
                            if (nback == 0 || nback < 0) {

                                $('#search-notification-icon')
                                    .removeClass('animated flash infinite');
                                $('#lcc-qstatus-back').empty();

                            }

                            // otherwise, just subtract one from the status
                            else {
                                $('#lcc-qstatus-back').html(
                                    '<span class="badge badge-warning">' +
                                        nback +
                                        '</span>'
                                );
                            }

                        }, 7500);

                    }

                    // if this query failed, then handle the UI change
                    // we'll make  the status card red for this
                    else if (msgdata.status == 'failed') {

                        // notify the user that the query is in the background
                        alertmsg = msgdata.message;
                        lcc_ui.alert_box(alertmsg, 'danger');

                        // check how many queries are running
                        nrun =
                            parseInt($('#lcc-qstatus-run')
                                     .attr('data-nrun'));
                        nrun = nrun - 1;
                        $('#lcc-qstatus-run')
                            .attr('data-nrun',nrun);

                        // turn off the flashing if no queries are currently
                        // running
                        if (nrun == 0 || nrun < 0) {

                            $('#search-notification-icon')
                                .removeClass('animated flash infinite');
                            $('#lcc-qstatus-run').empty();

                        }

                        // otherwise, just subtract one from the status
                        else {
                            $('#lcc-qstatus-run').html(
                                '<span class="badge badge-primary">' +
                                    nrun +
                                    '</span>'
                            );
                        }

                    }

                })
                .fail(function (err) {

                    // err has thrown, statusCode, body, jsonBody
                    console.log(err.statusCode);
                    console.log(err.body);

                    // stop the blinky!
                    $('#search-notification-icon')
                        .removeClass('animated flash infinite');

                    $('#lcc-qstatus-back').empty();
                    $('#lcc-qstatus-run').empty();

                    // alert the user that the query failed
                    lcc_ui.alert_box(
                        'Query failed because the search backend ' +
                            'crashed with an error code: <code>' +
                            err.statusCode +
                            '</code>! See the console for ' +
                            'more details...', 'danger'
                    );

                });

        }
        else {
            var error_message =
                "Some of the arguments for cone search " +
                "are missing or incorrect!";
            lcc_ui.alert_box(error_message, 'danger');
        }

    }

};


// this contains functions to deal with rendering datasets
var lcc_datasets = {

    // this function gets the dataset from the backend and enters a refresh loop
    // if the response indicates the dataset isn't available yet.  if the
    // dataset becomes available, it will load the JSON and render the dataset's
    // header and table rows
    get_dataset: function (setid, refresh) {

        // set the loading indicators
        var load_indicator = $('#setload-indicator');
        if (load_indicator.text() == '') {

            $('#setload-indicator').html(
                '<span class="text-warning">waiting for dataset... ' +
                    '</span>'
            );

            $('#setload-icon').html(
                '<img src="/static/images/twotone-sync-24px.svg' +
                    '" class="animated flash infinite">'
            );

        }

        var geturl = '/set/' + setid;
        var getparams = {json: 1,
                         strformat: 1};

        // here we do the retrieval
        $.getJSON(geturl, getparams, function (data) {

            var status = data.status;

            // if status is 'complete', we'll load the data
            if (status == 'complete') {

                //////////////////////////////
                // fill in the header first //
                //////////////////////////////


                // 2a. created
                var created_on = data.created;
                created_on = created_on + ' <strong>(' +
                    moment(created_on).fromNow() + ')<strong>';
                $('#dataset-createdon').html(created_on);

                // 2b. lastupdated
                var last_updated = data.updated;
                last_updated = last_updated + ' <strong>(' +
                    moment(last_updated).fromNow() + ')<strong>';
                $('#dataset-lastupdated').html(last_updated);

                // 3. status
                $('#dataset-status').html('<span class="text-success">' +
                                          status +
                                          '</span>');

                // 4. searchtype
                $('#dataset-searchtype').html('<code>' + data.searchtype +
                                              '</code>');

                // 5. searchargs
                $('#dataset-searchargs').html('<pre>' +
                                              JSON.stringify(data.searchargs,
                                                             null,
                                                             2) +
                                              '</pre>');

                // get the row status if there is one
                var rowstatus = data.rowstatus || ' ';
                // 6. nobjects
                $('#dataset-nobjects').html('<code>' +
                                             data.nobjects +
                                             '</code> '+ rowstatus);

                // 7. collections
                $('#dataset-collections').html('<code>' +
                                               data.collections.join(', ') +
                                               '</code>');

                // 8. setpickle
                $('#dataset-setpickle')
                    .html('<a ref="nofollow" href="' +
                          data.dataset_pickle + '">download file</a>');
                // 9. picklesha
                $('#dataset-picklesha')
                    .html('SHA256: <code>' + data.dataset_shasum + '</code>');

                // 10. setcsv
                $('#dataset-setcsv')
                    .html('<a ref="nofollow" href="' +
                          data.dataset_csv + '">download file</a>');
                // 11. csvsha
                $('#dataset-csvsha')
                    .html('SHA256: <code>' + data.csv_shasum + '</code>');

                // 12. lczip
                $('#dataset-lczip')
                    .html('<a ref="nofollow" href="' +
                          data.lczip + '">download file</a>');
                // 13. lcsha
                $('#dataset-lcsha')
                    .html('SHA256: <code>' + data.lczip_shasum + '</code>');

                if (data.pfzip != null) {
                    // 14. pfzip
                    $('#dataset-pfzip')
                        .html('<a ref="nofollow" href="' +
                              data.pfzip + '">download file</a>');
                    // 15. pfsha
                    $('#dataset-pfsha')
                        .html('SHA256: <code>' + data.pfzip_shasum + '</code>');
                }
                else {
                    // 14. pfzip
                    $('#dataset-pfzip')
                        .html('not available');
                    // 15. pfsha
                    $('#dataset-pfsha')
                        .empty();
                }

                if (data.cpzip != null) {
                    // 16. cpzip
                    $('#dataset-cpzip')
                        .html('<a ref="nofollow" href="' +
                              data.cpzip + '">download file</a>');
                    // 17. cpsha
                    $('#dataset-cpsha')
                        .html('SHA256: <code>' + data.cpzip_shasum + '</code>');

                }
                else {
                    // 18. cpzip
                    $('#dataset-cpzip')
                        .html('not available');
                    // 19. cpsha
                    $('#dataset-cpsha')
                        .empty();
                }


                /////////////////////////////////////
                // fill in the column descriptions //
                /////////////////////////////////////

                var colind = 0;
                var columns = data.columns;
                var collections = data.collections;
                var coldesc = data.coldesc;

                var coldef_rows = [];

                for (colind; colind < columns.length; colind++) {

                    var this_col = columns[colind];

                    var this_title = coldesc[this_col]['title'];
                    var this_desc = coldesc[this_col]['desc'];
                    var this_dtype = coldesc[this_col]['dtype'];

                    // add the columndef
                    var this_row = '<tr>' +
                        '<td width="100"><code>' + this_col + '</code></td>' +
                        '<td width="150">' + this_title + '</td>' +
                        '<td width="350">' + this_desc + '</td>' +
                        '<td width="100"><code>' +
                        this_dtype.replace('<','&lt;') +
                        '</code>' +
                        '</td>' +
                        '</tr>';
                    coldef_rows.push(this_row);

                    // also add the column to the header of the datatable
                    $('#lcc-datatable-header').append(
                        '<th>' + this_col + '</th>'
                    );
                }

                // finish up the column defs and write them to the table
                coldef_rows = coldef_rows.join('');
                $('#table-datarows').html(coldef_rows);

                ////////////////////////////////////////
                // finally, fill in the dataset table //
                ////////////////////////////////////////

                var rowind = 0;
                var datarows_elem = $('#lcc-datatable-datarows');

                // clear the table first
                datarows_elem.empty();

                // check if there are too many rows
                // if so, only draw the first 3000
                var max_rows = 3000;

                if (data.nobjects > max_rows) {
                    $('#dataset-nobjects').html(data.nobjects +
                                                ' (showing only top 3000)');
                }
                else {
                    max_rows = data.nobjects;
                    $('#dataset-nobjects').html(data.nobjects);
                }

                for (rowind; rowind < max_rows; rowind++) {

                    datarows_elem.append('<tr><td>' +
                                         data.rows[rowind].join('</td><td>') +
                                         '</td></tr>');

                }

                // 6. nobjects
                $('#dataset-nobjects').html('<code>' +
                                            data.nobjects +
                                            '</code>');

                // clear out the loading indicators at the end
                $('#setload-icon').empty();
                $('#setload-indicator').empty();

            }

            // if status is not 'complete', we enter a loop based on the
            // specified refresh interval and hit the backend again
            else if (status == 'in progress') {

                // 2a. created
                created_on = data.created;
                created_on = created_on + ' <strong>(' +
                    moment(created_on).fromNow() + ')<strong>';
                $('#dataset-createdon').html(created_on);

                // 2b. lastupdated
                last_updated = data.updated;
                last_updated = last_updated + ' <strong>(' +
                    moment(last_updated).fromNow() + ')<strong>';
                $('#dataset-lastupdated').html(last_updated);

                // 3. status
                $('#dataset-status').html('<span class="text-warning">' +
                                          status +
                                          '</span>');

                // 4. searchtype
                $('#dataset-searchtype').html('<code>' + data.searchtype +
                                              '</code>');

                // 5. searchargs
                $('#dataset-searchargs').html(
                    '<code>not available yet...</code>'
                );

                // 6. nobjects
                // get the row status if there is one
                rowstatus = data.rowstatus || ' ';
                $('#dataset-nobjects').html('<code>' +
                                             data.nobjects +
                                             '</code> '+ rowstatus);


                // 7. collections
                $('#dataset-collections').html('<code>' +
                                               data.collections.join(', ') +
                                               '</code>');

                // now wait for the next loop
                window.setTimeout(function () {

                    // call us again after the timeout expires
                    lcc_datasets.get_dataset(setid, refresh);

                }, refresh*1000.0);

            }

            // anything else is weird and broken
            else {

                var message = 'could not retrieve the dataset ' +
                    'from the LCC server backend';

                lcc_ui.alert_box(message, 'danger');

                // clear out the loading indicators at the end
                $('#setload-icon').empty();
                $('#setload-indicator').empty();

            }


        }).fail(function (xhr) {

            var message = 'could not retrieve the dataset ' +
                'from the LCC server backend';

            lcc_ui.alert_box(message, 'danger');

            // clear out the loading indicators at the end
            $('#setload-icon').empty();
            $('#setload-indicator').empty();

        });

    }

};
