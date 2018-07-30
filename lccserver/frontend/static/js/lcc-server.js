/*global $, jQuery, moment, oboe */

/*
lcc-server.js - Waqas Bhatti (wbhatti@astro.princeton.edu) - Jun 2018
License: MIT. See the LICENSE file for details.

This contains JS to drive the LCC server interface.

*/

var lcc_ui = {

    // this holds intervals for lazy background checking
    intervals: {},

    // this holds previous sort column values for add/removing them from the col
    // retrieval list
    prev_sort_cols: {},

    // this holds the currently active filters per search type
    active_filter_cols: {},

    // debounce function to slow down mindless clicking on buttons the backend
    // APIs can probably handle it, but it just wastes time/resources taken
    // straight from: https://davidwalsh.name/essential-javascript-functions

    // Returns a function, that, as long as it continues to be invoked, will not
    // be triggered. The function will be called after it stops being called for
    // N milliseconds. If `immediate` is passed, trigger the function on the
    // leading edge, instead of the trailing.
    debounce: function (func, wait, immediate) {
        var timeout;
        return function() {
            var context = this, args = arguments;
            var later = function() {
                timeout = null;
                if (!immediate) func.apply(context, args);
            };
            var callNow = immediate && !timeout;
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
            if (callNow) func.apply(context, args);
        };
    },

    // alert types: 'primary', 'secondary', 'success', 'danger', 'warning',
    //              'info', 'light', 'dark'
    alert_box: function(message, alert_type) {

        // get the current time in a nice format
        var now = moment().format('h:mm:ss A');

        // figure out the icon to display based on the type of alert

        // this is the default icon
        var icon = '/static/images/twotone-announcement-24px.svg';

        // this is the danger icon - used whenever something breaks
        if (alert_type == 'danger') {
            icon = '/static/images/twotone-error-24px.svg';
        }
        // this is the info icon
        else if (alert_type == 'info') {
            icon = '/static/images/twotone-info-24px.svg';
        }
        // this is the secondary icon - we use this to ask a question about
        // missing inputs
        else if (alert_type == 'secondary') {
            icon = '/static/images/twotone-help-24px.svg';
        }
        // this is the warning icon - we use this for background queries
        else if (alert_type == 'warning') {
            icon = '/static/images/twotone-watch_later-24px.svg';
        }
        // this is the success icon - we use this for completed queries
        else if (alert_type == 'primary') {
            icon = '/static/images/twotone-check_circle-24px.svg';
        }

        var alert = '<div class="mt-1 alert alert-' + alert_type +
            ' alert-dismissible fade show"' +
            ' role="alert">' +
            '<img class="mr-2 icon-svg" src="' + icon + '">' +
            '<strong class="mr-2">' +
            now + '</strong><span class="mr-2">' + message +
            '<button type="button" class="close" data-dismiss="alert" ' +
            'aria-label="Close"><span aria-hidden="true">&times;</span>' +
            '</button></div>';

        // can stack multiple alerts
        $('#alert-box').append(alert);

    },

    // this updates all of the column associated controls whenever there's an
    // update needed
    update_column_associated_controls: function (columns,
                                                 indexed_columns,
                                                 fts_columns) {

        // update the column select boxes
        var column_selectboxes = $('.lcc-column-select');

        column_selectboxes.each(function (e, i) {

            var thisbox = $(this);

            // clear it out
            thisbox.empty();

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

            // clear it out
            thisbox.empty();

            var column_ind = 0;
            for (column_ind; column_ind < columns.length; column_ind++) {
                thisbox.append('<option value="' +
                               columns[column_ind] +
                               '">' +
                               columns[column_ind] +
                               '</option>');
            }

        });

        // update the sort column select boxes
        var sort_selectboxes = $('.lcc-sortcolumn-select');

        sort_selectboxes.each(function (e, i) {

            var thisbox = $(this);

            // clear it out
            thisbox.empty();

            var column_ind = 0;
            var done_with_selected = false;

            for (column_ind; column_ind < columns.length; column_ind++) {

                thisbox.append('<option value="' +
                               columns[column_ind] +
                               '">' +
                               columns[column_ind] +
                               '</option>');

            }

            // find either the sdssr column or the objectid column in the
            // columns to select them as a default sort col
            var sdssr_ok = columns.find(function (elem) {
                return elem == 'sdssr';
            });
            var objectid_ok = columns.find(function (elem) {
                return elem == 'objectid';
            });

            if (sdssr_ok) {
                thisbox.children('option[value="sdssr"]').attr('selected',true);
            }
            else if (objectid_ok) {
                thisbox.children('option[value="objectid"]').attr('selected',true);
            }


        });

        // update the FTS query FTS column list
        $('#ftsquery-column-list')
            .html(fts_columns.sort().join(', '));

        // update the indexed column list
        $('#columnsearch-indexed-columnlist')
            .html(indexed_columns.sort().join(', '));
    },


    // this wires up all the controls
    action_setup: function () {

        // bind the form submit for the cone search
        $('#conesearch-form').on('submit', function (event) {

            event.preventDefault();
            lcc_ui.debounce(lcc_search.do_conesearch(), 250);

        });

        // bind the form submit for the cone search
        $('#ftsquery-form').on('submit', function (event) {

            event.preventDefault();
            lcc_ui.debounce(lcc_search.do_ftsquery(), 250);

        });

        // bind the form submit for the cone search
        $('#columnsearch-form').on('submit', function (event) {

            event.preventDefault();
            lcc_ui.debounce(lcc_search.do_columnsearch(), 250);

        });

        // bind the form submit for the cone search
        $('#xmatch-form').on('submit', function (event) {

            event.preventDefault();
            lcc_ui.debounce(lcc_search.do_xmatch(), 250);

        });

        // bind the change event on lcc-collection-select select boxes
        // everywhere so the list of the columns shown in .lcc-column-select
        // boxes is always up to date
        $('.lcc-collection-select').on('change', function (evt) {

            var current_val = $(this).val();
            var coll_ind;

            // for each of the values, go through and get their available
            // columns
            var available_columns = [];
            var available_indexcols = [];
            var available_ftscols = [];

            // if the current value of the select is ['all'], then the only
            // available columns are the ones common to all collections. this
            // from an intersection operation run by the backend at
            // /api/collections
            if (current_val.length == 1 && current_val[0] == 'all') {

                available_columns = lcc_search.columns.sort();
                available_indexcols = lcc_search.indexcols.sort();
                available_ftscols = lcc_search.ftscols.sort();

                lcc_ui.update_column_associated_controls(
                    available_columns,
                    available_indexcols,
                    available_ftscols
                );

            }

            // otherwise, we have to go through the current collections and get
            // the intersections, i.e. the common columns
            else {

                // make sure to remove the 'all' value from the array of current
                // collections
                current_val = current_val.filter(function (elem) {
                    return elem != 'all';
                });

                // get the actual collection associated lists of columns,
                // indexedcols, and ftscols from the current_val of the select
                // box
                var curr_columns = [];
                var curr_indexedcols = [];
                var curr_ftscols = [];

                current_val.forEach(function (elem, ind, arr) {

                    coll_ind = lcc_ui.collections.db_collection_id.indexOf(elem);
                    curr_columns.push(lcc_ui.collections.columnlist[coll_ind]);
                    curr_indexedcols.push(lcc_ui.collections.indexedcols[coll_ind]);
                    curr_ftscols.push(lcc_ui.collections.ftsindexedcols[coll_ind]);

                });


                // first, we map a function to turn all the column lists from
                // all collections into sets
                var cols = curr_columns.map(function (elem) {
                    return new Set(elem.split(','));
                });

                // next, we'll have to do a reduce operation on the intersection
                // of all sets of columns available.
                available_columns = cols
                    .reduce(function (acc, curr, currind, prev) {
                        var intersect = new Set();
                        for (let elem of acc) {
                            if (curr.has(elem)) {
                                intersect.add(elem);
                            }
                        }
                        return intersect;
                    });

                // I feel so clever after getting that working...
                // now, let's do it again for indexed and fts cols

                // map indexedcols
                var indexedcols = curr_indexedcols.map(function (elem) {
                    return new Set(elem.split(','));
                });

                // reduce to intersection
                available_indexcols = indexedcols
                    .reduce(function (acc, curr, currind, prev) {
                        var intersect = new Set();
                        for (let elem of acc) {
                            if (curr.has(elem)) {
                                intersect.add(elem);
                            }
                        }
                        return intersect;
                    });


                // map ftscols
                var ftscols = curr_ftscols.map(function (elem) {
                    return new Set(elem.split(','));
                });

                // reduce to intersection
                available_ftscols = ftscols
                    .reduce(function (acc, curr, currind, prev) {
                        var intersect = new Set();
                        for (let elem of acc) {
                            if (curr.has(elem)) {
                                intersect.add(elem);
                            }
                        }
                        return intersect;
                    });


                // now that we have a list of available columns we can use,
                // add these to all of the column select boxes
                lcc_ui.update_column_associated_controls(
                    Array.from(available_columns),
                    Array.from(available_indexcols),
                    Array.from(available_ftscols)
                );

            }

            // IMPORTANT: clear out all the filter buckets because the backend
            // will ignore non-existent columns that might get left behind if
            // the filter bucket remains unchanged after the collection change
            // is fired.
            // FIXME: fix this on the backend too
            $('.lcc-filterbucket').empty();

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
            // this is done in two steps
            // 1. look up the currently active

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

            // integer (usually counts of some sort, so we enforce !< 0)
            else if (filter_dtype.indexOf('i') != -1) {

                filter_check = (
                    !(isNaN(parseInt(filter_val.trim()))) &&
                        (filter_opstr != 'ct') &&
                        !(parseInt(filter_val.trim()) < 0)
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
                        '<select class="custom-select ' +
                        'lcc-filterbucket-chainer">' +
                        '<option value="and" selected>and</option>' +
                        '<option value="or">or</option></select> ';
                }
                else {
                    filter_card_joiner = '';
                }

                // generate the card for this filter
                var filter_card = '<div class="card filterbucket-card ' +
                    'mt-1 mx-1 p-1" ' +
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
                    '<div class="card-body d-flex align-items-center p-2">' +
                    '<div class="mr-auto">' +
                    filter_card_joiner +
                    '</div>' +
                    '<div class="mx-auto"><code><span class="text-primary">' +
                    filter_col + '</span> ' + filter_op + ' <strong>' +
                    filter_val + '</strong></code>' +
                    '</div>' +
                    '<div class="ml-auto">' +
                    '<a href="#" title="remove this filter" ' +
                    'class="btn btn-outline-danger btn-sm p-1 ' +
                    'ml-auto lcc-filterbucket-remove" ' +
                    'data-target="' +  target + '"' +
                    'data-colrem="' + filter_col + '">' +
                    '<img src="/static/images/twotone-clear-24px.svg"></a>' +
                    '</div>' +
                    '</div>' +
                    '</div>';

                filterbucket_elem.append(filter_card);

                // clear the current value box so it's empty for the next item
                filter_val_elem.val('');

            }

            else {

                var friendly_dtype = 'string';
                if (filter_dtype.indexOf('f') != -1) {
                    friendly_dtype = 'float';
                }
                else if (filter_dtype.indexOf('i') != -1) {
                    friendly_dtype = 'int';
                }

                var friendly_filterval = filter_val;
                if (!filter_val || filter_val.length == 0) {
                    friendly_filterval = 'None';
                }

                var msg = 'Column: <span class="text-primary"><strong>' +
                    filter_col + '</strong></span> ' +
                    'requires dtype: ' +
                    '<span class="text-info"><strong>' +
                    friendly_dtype + '</strong></span>. ' +
                    'The current filter operator: ' +
                    '<span class="text-primary"><strong>' +
                    filter_op + '</strong></span> or ' +
                    'operand: <span class="text-primary"><strong>' +
                    friendly_filterval +
                    '</strong></span> are not compatible.';

                lcc_ui.alert_box(msg, 'secondary');
            }

        });

        // bind the filter-delete button
        $('.tab-pane').on('click', '.lcc-filterbucket-remove', function(e) {

            e.preventDefault();

            var filter_col = $(this).attr('data-colrem');
            var target = $(this).attr('data-target');

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


        // handle the example coordlist link
        $('#xmatch-example').on('click', function (evt) {

            evt.preventDefault();

            $('#xmatch-query').val(lcc_search.coordlist_placeholder)
                .focus()
                .blur();

        });



    },


    // this parses the filter control results for any searchtype it is pointed
    // to. returns an SQL string that can be validated by the backend.
    parse_column_filters: function(target) {

        var filterbucket_elem = $('#' + target + '-filterbucket');
        var filterbucket_items = filterbucket_elem.children();
        var filterbucket_nitems = filterbucket_items.length;

        var filters = [];
        var filter_cols = [];

        // go through each of the filter items and parse them
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
                .children('div')
                .children('div.mr-auto')
                .children('select').val();

            if (chain_op != undefined && i > 0) {
                thisfilter = chain_op + ' (' + col +
                    ' ' + oper + ' ' + fval + ')';
            }

            filters.push(thisfilter);
            filter_cols.push(col);

        });

        return [filters.join(' '), new Set(filter_cols)];

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
                        setid + '</a></td>';

                    //
                    // Objects column
                    //
                    var table_nobjects = '<td>' +
                        nobjects +
                        '</td>';
                    //
                    // Query column
                    //
                    var table_query = '<td width="350">' +
                        '<code><details><summary>' + query_type +
                        '</summary><pre>' +
                        JSON.stringify(JSON.parse(query_params),null,2) +
                        '</pre></details></code>' +
                        'collections used: <code>' +
                        queriedcolls + '</code>' +
                        '</td>';
                    table_query = table_query
                        .replace('sqlite_','')
                        .replace('postgres_','');

                    //
                    // Products column
                    //
                    var dataset_download = '';
                    var lczip_download = '';
                    var pfzip_download = '';
                    var cpzip_download = '';

                    if (dataset_fpath != null) {
                        dataset_download = '<a download rel="nofollow" ' +
                            'href="' + dataset_fpath +
                            '" title="download search results pickle">' +
                            'dataset pickle' +
                            '</a> <span data-toggle="tooltip" title="' +
                            dataset_shasum + '">' +
                            '[SHA256]</span>';
                    }

                    if (lczip_fpath != null) {
                        lczip_download = '<a download rel="nofollow" ' +
                            'href="' + lczip_fpath +
                            '" title="download light curves ZIP">' +
                            'light curve ZIP' +
                            '</a> <span data-toggle="tooltip" title="' +
                            lczip_shasum + '">' +
                            '[SHA256]</span>';
                    }

                    if (pfzip_fpath != null) {
                        pfzip_download = '<a download rel="nofollow" ' +
                            'href="' + pfzip_fpath +
                            '" title="download period-finding results ZIP">' +
                            'period-finding result pickles ZIP' +
                            '</a> <span data-toggle="tooltip" title="' +
                            pfzip_shasum + '">' +
                            '[SHA256]</span>';
                    }

                    if (cpzip_fpath != null) {
                        cpzip_download = '<a download rel="nofollow" ' +
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

                // store this so we can refer to it later
                var collections = result.collections;
                lcc_ui.collections = collections;

                var collection_ids = collections.collection_id;
                lcc_search.collections = collections;
                var coll_idx;

                // THESE ARE INTERSECTIONS ACROSS ALL COLLECTIONS SO ONLY THE
                // COMMON COLUMNS ACROSS COLLECTIONS
                var available_columns = result.available_columns.sort();
                var indexed_columns = result.available_indexed_columns.sort();
                var fts_columns = result.available_fts_columns.sort();

                // we'll also store the available columns and their definitions
                lcc_search.columns = available_columns;

                //we use all available columns to figure out the common cols and
                //also the per collection special cols
                lcc_search.indexcols = indexed_columns;
                lcc_search.ftscols = fts_columns;

                // select all of the collection selectboxes so we can update
                // them for all collections
                var collection_selectboxes = $('.lcc-collection-select');

                // now process each collection
                // add it to the list of collections on the collections tab
                for (coll_idx = 0;
                     coll_idx < collection_ids.length;
                     coll_idx++) {

                    //
                    // name column
                    //
                    var db_collid = collections.db_collection_id[coll_idx];
                    var collname = collections.name[coll_idx];
                    var table_column_name = '<td width="80">' +
                        collname + ' (<code>' + db_collid + '</code>)' +
                        '</td>';

                    // update the collection select boxes
                    collection_selectboxes.each(function (e, i) {

                        var thisbox = $(this);

                        thisbox.append('<option value="' +
                                       db_collid +
                                       '">' +
                                       collname +
                                       '</option>');

                    });

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

                    var center_ra = (minra + maxra)/2.0;
                    var center_decl = (mindecl + maxdecl)/2.0;
                    center_ra = center_ra.toFixed(2);
                    center_decl = center_decl.toFixed(2);

                    minra = minra.toFixed(2);
                    maxra = maxra.toFixed(2);
                    mindecl = mindecl.toFixed(2);
                    maxdecl = maxdecl.toFixed(2);

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

                    // add each column for this collection to the output
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
                                    '<span class="kdtree-col">' +
                                        col_popover + '</span>'
                                );
                            }
                            // indexed columns are not interesting, because
                            // everyone expects fast searches on any column and
                            // we indexed pretty much every column anyway. let's
                            // get rid of <span class="kdtree-col"></span> for
                            // these for now
                            else if (indexedcols.indexOf(thiscol) != -1) {
                                formatted_colspec.push(col_popover);
                            }
                            else {
                                formatted_colspec.push(col_popover);
                            }

                        }

                        // at the end, check if a column by this name exists in
                        // the lcc_search.coldefs key and put it in there if it
                        // doesn't. we will not update the key if it does exist
                        // FIXME: this has the potential to miss updated
                        // columns, but hopefully column definitions for the
                        // same column names don't change between collections
                        // (or if they do, the change is backported to the
                        // previous collection)
                        if (! (thiscol in lcc_search.coldefs) ) {
                            lcc_search.coldefs[thiscol] =
                                collections.columnjson[coll_idx][thiscol];
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

                // update the column select boxes
                lcc_ui.update_column_associated_controls(
                    available_columns,
                    indexed_columns,
                    fts_columns
                );

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
    coldefs: {},

    // this holds the FTS index columns
    ftscols: null,

    // this holds the indexed columns
    indexedcols: null,

    // this is used for the coordlist to show example formats
    coordlist_placeholder: "# example object and coordinate list\n" +
        "# objectid ra dec\n" +
        "aaa 289.99698 44.99839\n" +
        "bbb 293.358 -23.206\n" +
        "ccc 294.197 +23.181\n" +
        "ddd 19 25 27.9129 +42 47 03.693\n" +
        "eee 19:25:27 -42:47:03.21\n" +
        "# .\n" +
        "# .\n" +
        "# .\n" +
        "# etc. lines starting with '#' will be ignored\n" +
        "# (max 5000 objects)",

    // this variable contains the coordinate list after the file upload is
    // parsed and validated. will be passed to the backend via POST
    uploaded_coordlist: [],

    // this holds the input from the xmatchquery-xmatch textarea
    coordlist_contents: "",

    // regexes to match lines of the uploaded xmatch objects
    decimal_regex: /^(\w+)\s(\d{1,3}\.{0,1}\d*)\s([+-]{0,1}\d{1,2}\.{0,1}\d*)$/,

    sexagesimal_regex: /^(\w+)\s(\d{1,2}[ :]\d{2}[ :]\d{2}\.{0,1}\d*)\s([+-]{0,1}\d{1,2}[: ]\d{2}[: ]\d{2}\.{0,1}\d*)$/g,


    // this runs an FTS query
    do_xmatch: function () {

        var proceed_step1 = false;
        var proceed_step2 = false;

        // get the collections to use
        var collections = $('#xmatch-collection-select').val();

        if (collections.length == 0) {
            collections = null;
        }

        // get the columns to retrieve
        var columns = $('#xmatch-column-select').val();

        // get the xmatch input
        var xmatchtext = $('#xmatch-query').val().trim();
        if (xmatchtext.length > 0) {
            proceed_step1 = true;
        }

        // get the xmatch distance param
        var xmatchdistance = parseFloat($('#xmatch-matchradius').val().trim());
        if (xmatchdistance != undefined &&
            !isNaN(xmatchdistance) &&
            xmatchdistance > 0.0) {
            proceed_step2 = true;
        }
        else {
            proceed_step2 = false;
        }

        // get the ispublic parameter
        var ispublic = $('#xmatch-ispublic').prop('checked');

        if (ispublic) {
            ispublic = 1;
        }
        else {
            ispublic = 0;
        }

        // parse the extra filters
        var [filters, filter_cols] = lcc_ui.parse_column_filters('xmatch');

        if (filters.length == 0) {
            filters = null;
        }
        // add any filter enabled columns to the columns to retrieve
        else {
            for (let thisfilt of filter_cols) {
                if (columns.indexOf(thisfilt) == -1) {
                    columns.push(thisfilt);
                }
            }
        }

        // get the value of the _xsrf token
        var _xsrf = $('#xmatch-form > input[type="hidden"]').val();


        var posturl = '/api/xmatch';
        var postparams = {xmq: xmatchtext,
                          xmd: xmatchdistance,
                          _xsrf:_xsrf,
                          result_ispublic: ispublic,
                          collections: collections,
                          columns: columns,
                          filters: filters};

        postparams = $.param(postparams);
        posturl = posturl + '?' + postparams;

        if (proceed_step1 && proceed_step2) {

            // disable the submit button until we return
            // FIXME: may need to turn off handler as well
            $('#xmatch-submit').attr('disabled',true);

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
            oboe({url: posturl, method: 'POST'})
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
                    $('#xmatch-submit').attr('disabled',false);

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

                        // inform the user their query finished
                        var alertmsg = 'Query <code>' + msg_setid +
                            '</code> finished successfully. <strong>' +
                            + msgdata.result.nobjects +
                            '</strong> matched objects found. ' +
                            '<a target="_blank" ' +
                            'rel="nofollow noreferer noopener" href="' +
                            msgdata.result.seturl +
                            '">Result dataset is ready to view.</a>';

                        // set the alert
                        lcc_ui.alert_box(alertmsg, 'primary');

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

                        // check if the query went to the background or if it
                        // actually decided to give up LC zipping because too
                        // many LCs were requested
                        if (msgdata.message.indexOf("won't generate") != -1) {
                            alertmsg = 'Query <code>' +
                                msgdata.result.setid +
                                '</code> is complete, but there are &gt; ' +
                                '20,000 LCs to collect so no ZIP file was ' +
                                'generated. Try refining your query, or see ' +
                                '<a target="_blank" ' +
                                'rel="nofollow noreferer noopener" href="' +
                                msgdata.result.seturl + '">its dataset page</a> for a ' +
                                'CSV that lists all objects and download links ' +
                                'for their individual light curves.';
                        }

                        else {
                            // notify the user that the query is in the background
                            alertmsg = 'Query <code>' +
                                msgdata.result.setid +
                                '</code> now in background queue. ' +
                                'Results will appear at ' +
                                '<a rel="nofollow noopener noreferrer"' +
                                'target="_blank" href="/set/' +
                                msgdata.result.setid + '">its dataset page</a> ' +
                                'when done.';

                        }

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
                        // but failed
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
                        'Query failed! Search backend ' +
                            'cancelled it because of an exception.',
                        'danger'
                    );


                });

        }
        else {
            var error_message =
                "No query text found in the cross-match object list input box.";
            lcc_ui.alert_box(error_message, 'secondary');
        }

    },



    // this runs a full column search
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
        var [filters, filter_cols] = lcc_ui.parse_column_filters('columnsearch');

        if (filters.length == 0) {
            filters = null;
        }
        // add any filter enabled columns to the columns to retrieve
        else {
            for (let thisfilt of filter_cols) {
                if (columns.indexOf(thisfilt) == -1) {
                    columns.push(thisfilt);
                }
            }
        }

        // if there are no filters, we won't be fetching the entire catalog
        if (filters == null || filters.length == 0) {
            filters = null;
            lcc_ui.alert_box("No column filters were specified, " +
                             "not going to fetch entire collection tables.",
                             'secondary');
            proceed = false;
        }
        else {
            proceed = true;
        }

        // get the sort column and order
        var sortcol = $('#columnsearch-sortcolumn-select').val();
        var sortorder = $('#columnsearch-sortorder-select').val();

        // also, add the sortby column to the retrieval column list

        var sortcol_in_columns = columns.find(function (elem) {
            return elem == sortcol;
        });

        if (!sortcol_in_columns) {
            columns.push(sortcol);
        }

        var geturl = '/api/columnsearch';
        var getparams = {result_ispublic: ispublic,
                         collections: collections,
                         columns: columns,
                         filters: filters,
                         sortcol: sortcol,
                         sortorder: sortorder};

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

                        // inform the user their query finished
                        var alertmsg = 'Query <code>' + msg_setid +
                            '</code> finished successfully. <strong>' +
                            + msgdata.result.nobjects +
                            '</strong> matched objects found. ' +
                            '<a target="_blank" ' +
                            'rel="nofollow noreferrer noopener" href="' +
                            msgdata.result.seturl +
                            '">Result dataset is ready to view.</a>';

                        // set the alert
                        lcc_ui.alert_box(alertmsg, 'primary');

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

                        // check if the query went to the background or if it
                        // actually decided to give up LC zipping because too
                        // many LCs were requested
                        if (msgdata.message.indexOf("won't generate") != -1) {
                            alertmsg = 'Query <code>' +
                                msgdata.result.setid +
                                '</code> is complete, but there are &gt; ' +
                                '20,000 LCs to collect so no ZIP file was ' +
                                'generated. Try refining your query, or see ' +
                                '<a target="_blank" ' +
                                'rel="nofollow noreferer noopener" href="' +
                                msgdata.result.seturl + '">its dataset page</a> for a ' +
                                'CSV that lists all objects and download links ' +
                                'for their individual light curves.';
                        }

                        else {
                            // notify the user that the query is in the background
                            alertmsg = 'Query <code>' +
                                msgdata.result.setid +
                                '</code> now in background queue. ' +
                                'Results will appear at ' +
                                '<a rel="nofollow noopener noreferrer"' +
                                'target="_blank" href="/set/' +
                                msgdata.result.setid + '">its dataset page</a> ' +
                                'when done.';

                        }

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
                        // but failed!
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
                        'Query failed! Search backend ' +
                            'cancelled it because of an exception.',
                        'danger'
                    );


                });

        }
        else {
            var error_message =
                "No valid column filters were found for the column search query.";
            lcc_ui.alert_box(error_message, 'secondary');
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
        var [filters, filter_cols] = lcc_ui.parse_column_filters('ftsquery');

        if (filters.length == 0) {
            filters = null;
        }
        // add any filter enabled columns to the columns to retrieve
        else {
            for (let thisfilt of filter_cols) {
                if (columns.indexOf(thisfilt) == -1) {
                    columns.push(thisfilt);
                }
            }
        }

        var geturl = '/api/ftsquery';
        var getparams = {ftstext: ftstext,
                         result_ispublic: ispublic,
                         collections: collections,
                         columns: columns,
                         filters: filters};


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

                        // inform the user their query finished
                        var alertmsg = 'Query <code>' + msg_setid +
                            '</code> finished successfully. <strong>' +
                            + msgdata.result.nobjects +
                            '</strong> matched objects found. ' +
                            '<a target="_blank" ' +
                            'rel="nofollow noreferrer noopener" href="' +
                            msgdata.result.seturl +
                            '">Result dataset is ready to view.</a>';

                        // set the alert
                        lcc_ui.alert_box(alertmsg, 'primary');

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

                        // check if the query went to the background or if it
                        // actually decided to give up LC zipping because too
                        // many LCs were requested
                        if (msgdata.message.indexOf("won't generate") != -1) {
                            alertmsg = 'Query <code>' +
                                msgdata.result.setid +
                                '</code> is complete, but there are &gt; ' +
                                '20,000 LCs to collect so no ZIP file was ' +
                                'generated. Try refining your query, or see ' +
                                '<a target="_blank" ' +
                                'rel="nofollow noopener noreferer" href="' +
                                msgdata.result.seturl + '">its dataset page</a> for a ' +
                                'CSV that lists all objects and download links ' +
                                'for their individual light curves.';
                        }

                        else {
                            // notify the user that the query is in the background
                            alertmsg = 'Query <code>' +
                                msgdata.result.setid +
                                '</code> now in background queue. ' +
                                'Results will appear at ' +
                                '<a rel="nofollow noopener noreferrer"' +
                                'target="_blank" href="/set/' +
                                msgdata.result.setid + '">its dataset page</a> ' +
                                'when done.';

                        }

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
                        // but failed
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
                        'Query failed! Search backend ' +
                            'cancelled it because of an exception.',
                        'danger'
                    );


                });

        }
        else {
            var error_message =
                "No query text found in the FTS query text box.";
            lcc_ui.alert_box(error_message, 'secondary');
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
        var [filters, filter_cols] = lcc_ui.parse_column_filters('conesearch');

        if (filters.length == 0) {
            filters = null;
        }
        // add any filter enabled columns to the columns to retrieve
        else {
            for (let thisfilt of filter_cols) {
                if (columns.indexOf(thisfilt) == -1) {
                    columns.push(thisfilt);
                }
            }
        }

        var geturl = '/api/conesearch';
        var getparams = {coords: coords,
                         result_ispublic: ispublic,
                         collections: collections,
                         columns: columns,
                         filters: filters};


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

                        // inform the user their query finished
                        var alertmsg = 'Query <code>' + msg_setid +
                            '</code> finished successfully. <strong>' +
                            + msgdata.result.nobjects +
                            '</strong> matched objects found. ' +
                            '<a target="_blank" ' +
                            'rel="nofollow noreferrer noopener" href="' +
                            msgdata.result.seturl +
                            '">Result dataset is ready to view.</a>';

                        // set the alert
                        lcc_ui.alert_box(alertmsg, 'primary');

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

                        // check if the query went to the background or if it
                        // actually decided to give up LC zipping because too
                        // many LCs were requested
                        if (msgdata.message.indexOf("won't generate") != -1) {
                            alertmsg = 'Query <code>' +
                                msgdata.result.setid +
                                '</code> is complete, but there are &gt; ' +
                                '20,000 LCs to collect so no ZIP file was ' +
                                'generated. Try refining your query, or see ' +
                                '<a target="_blank" ' +
                                'rel="nofollow noopener noreferer" href="' +
                                msgdata.result.seturl + '">its dataset page</a> for a ' +
                                'CSV that lists all objects and download links ' +
                                'for their individual light curves.';
                        }

                        else {
                            // notify the user that the query is in the background
                            alertmsg = 'Query <code>' +
                                msgdata.result.setid +
                                '</code> now in background queue. ' +
                                'Results will appear at ' +
                                '<a rel="nofollow noopener noreferrer"' +
                                'target="_blank" href="/set/' +
                                msgdata.result.setid + '">its dataset page</a> ' +
                                'when done.';

                        }

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
                        // but failed
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
                        'Query failed! Search backend ' +
                            'cancelled it because of an exception.',
                        'danger'
                    );

                });

        }
        else {
            var error_message =
                "Some of the arguments for cone search " +
                "are missing or incorrect.";
            lcc_ui.alert_box(error_message, 'secondary');
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

                // 4 and 5. searchtype and searchargs
                $('#dataset-searchargs').html('<details><summary>' +
                                              data.searchtype
                                              .replace('sqlite_','')
                                              .replace('postgres_','') +
                                              '</summary><pre>' +
                                              JSON.stringify(data.searchargs,
                                                             null,
                                                             2) +
                                              '</pre></detail>');

                // 6. collections
                $('#dataset-collections').html( data.collections.join(', '));

                // 7. setpickle
                $('#dataset-setpickle')
                    .html('<a download ref="nofollow" href="' +
                          data.dataset_pickle + '">download file</a>');
                // 8. picklesha
                $('#dataset-picklesha')
                    .html('SHA256: <code>' + data.dataset_shasum + '</code>');

                // 9. setcsv
                $('#dataset-setcsv')
                    .html('<a download ref="nofollow" href="' +
                          data.dataset_csv + '">download file</a>');
                // 10. csvsha
                $('#dataset-csvsha')
                    .html('SHA256: <code>' + data.csv_shasum + '</code>');


                // 11. nobjects
                if ('rowstatus' in data) {
                    $('#dataset-nobjects').html(data.nobjects +
                                                ' (' +
                                                data.rowstatus +
                                                ' &mdash; see the ' +
                                                '<a download ref="nofollow" href="' +
                                                data.dataset_csv + '">dataset CSV</a>' +
                                                ' for complete table)');
                }
                else {
                    $('#dataset-nobjects').html(data.nobjects);
                }


                if (data.lczip != null && data.lczip != undefined) {
                    // 12. lczip
                    $('#dataset-lczip')
                        .html('<a download ref="nofollow" href="' +
                              data.lczip + '">download file</a>');
                    // 13. lcsha
                    $('#dataset-lcsha')
                        .html('SHA256: <code>' + data.lczip_shasum + '</code>');
                }
                else {
                    $('#dataset-lczip').html('not available');
                    $('#dataset-lcsha').empty();
                }

                if (data.pfzip != null) {
                    // 14. pfzip
                    $('#dataset-pfzip')
                        .html('<a download ref="nofollow" href="' +
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
                        .html('<a download ref="nofollow" href="' +
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

                var column_widths = [];
                var thiscol_width = null;

                for (colind; colind < columns.length; colind++) {

                    var this_col = columns[colind];

                    var this_title = coldesc[this_col]['title'];
                    var this_desc = coldesc[this_col]['desc'];
                    var this_dtype = coldesc[this_col]['dtype'].replace('<','');

                    // add the columndef
                    var this_row = '<tr>' +
                        '<td width="100"><code>' + this_col + '</code></td>' +
                        '<td width="150">' + this_title + '</td>' +
                        '<td width="350">' + this_desc + '</td>' +
                        '<td width="100"><code>' +
                        this_dtype +
                        '</code>' +
                        '</td>' +
                        '</tr>';
                    coldef_rows.push(this_row);

                    // calculate the width of the header cells
                    if (this_dtype == 'f8') {
                        thiscol_width = 100;
                    }
                    else if (this_dtype == 'i8') {
                        thiscol_width = 80;
                    }
                    else if (this_dtype.indexOf('U') != -1) {
                        thiscol_width = parseInt(this_dtype.replace('U',''))*10;
                        if (thiscol_width > 500) {
                            thiscol_width = 500;
                        }
                    }
                    else {
                        thiscol_width = 100;
                    }
                    column_widths.push(thiscol_width);

                    // add the column names to the table header
                    $('#lcc-datatable-header').append(
                        '<th width="'+ thiscol_width + '">' + this_col + '</th>'
                    );

                }

                // make the table header width = to the sum of the widths we
                // need
                var table_width = column_widths
                    .reduce(function (acc, curr, currind, prev ) {
                        return parseInt(acc + curr);
                    });
                $('#lcc-datatable').width(table_width);

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
                var max_rows = data.rows.length;
                if (data.rows.length > 3000) {
                    max_rows = 3000;
                }

                for (rowind; rowind < max_rows; rowind++) {

                    datarows_elem.append('<tr><td>' +
                                         data.rows[rowind].join('</td><td>') +
                                         '</td></tr>');

                }

                // clear out the loading indicators at the end
                $('#setload-icon').empty();
                $('#setload-indicator').empty();

                // make the table div bottom stick to the bottom of the window
                // so we can have a scrollbar at the bottom

                // calculate the offset
                var datacontainer_offset = $('.datatable-container').offset().top;

                $('.datatable-container').height($(window).height() -
                                                 datacontainer_offset - 5);

                // make the table div bottom stick to the bottom of the container
                // so we can have a scrollbar at the bottom
                $('.dataset-table')
                    .height($('.datatable-container').height());


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

                // 4 and 5. searchtype and searchargs
                $('#dataset-searchargs').html('<details><summary>' +
                                              data.searchtype
                                              .replace('sqlite_','')
                                              .replace('postgres_','') +
                                              '</summary><pre>' +
                                              JSON.stringify(data.searchargs,
                                                             null,
                                                             2) +
                                              '</pre></detail>');

                // 6. nobjects
                if ('rowstatus' in data) {
                    $('#dataset-nobjects').html(data.nobjects +
                                                ' (' + data.rowstatus + ')');
                }
                else {
                    $('#dataset-nobjects').html(data.nobjects);
                }

                // 7. collections
                $('#dataset-collections').html(data.collections.join(', '));

                // now wait for the next loop
                window.setTimeout(function () {

                    // call us again after the timeout expires
                    lcc_datasets.get_dataset(setid, refresh);

                }, refresh*1000.0);

            }

            // anything else is weird and broken
            else {

                var message = 'Could not retrieve the dataset ' +
                    'from the LCC server backend';

                lcc_ui.alert_box(message, 'danger');

                // clear out the loading indicators at the end
                $('#setload-icon').empty();
                $('#setload-indicator').empty();

            }


        }).fail(function (xhr) {

            var message = 'Could not retrieve the dataset ' +
                'from the LCC server backend';

            lcc_ui.alert_box(message, 'danger');

            // clear out the loading indicators at the end
            $('#setload-icon').empty();
            $('#setload-indicator').empty();

        });

    }

};
