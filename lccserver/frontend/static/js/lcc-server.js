/*global $, moment, oboe, setTimeout, clearTimeout, Image */

/*
  lcc-server.js - Waqas Bhatti (wbhatti@astro.princeton.edu) - Jun 2018
  License: MIT. See the LICENSE file for details.

  This contains JS to drive the LCC server interface.

*/

var lcc_ui = {

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

        column_selectboxes.each(function () {

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

        filter_selectboxes.each(function () {

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

        sort_selectboxes.each(function () {

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
        $('.lcc-collection-select').on('change', function () {

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

                current_val.forEach(function (elem) {

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
                    .reduce(function (acc, curr) {
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
                    .reduce(function (acc, curr) {
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
                    .reduce(function (acc, curr) {
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

        // fancy zoom and pan effects for a phased LC tile
        // see https://codepen.io/ccrch/pen/yyaraz
        $('.modal-body')
            .on('mouseover', '.zoomable-tile', function () {

                $(this).css({'transform': 'scale(1.6)',
                             'z-index':1000});

            });
        $('.modal-body')
            .on('mouseout', '.zoomable-tile', function () {

                $(this).css({'transform': 'scale(1.0)',
                             'z-index':0});

            });

        $('#objectinfo-container')
            .on('mouseover', '.zoomable-tile', function () {

                $(this).css({'transform': 'scale(1.6)',
                             'z-index':1000});

            });
        $('#objectinfo-container')
            .on('mouseout', '.zoomable-tile', function () {

                $(this).css({'transform': 'scale(1.0)',
                             'z-index':0});

            });

        // bind the objectinfo-link to show a modal with objectinfo from
        // checkplots
        $('#objectinfo-modal').on('show.bs.modal', function (evt) {

            // this is us
            var modal = $(this);

            // what triggered us?
            var button = $(evt.relatedTarget);

            // objectid, collection, lcfname
            var objectid = button.attr('data-objectid');
            var collection = button.attr('data-collection');
            var lcfname = button.attr('data-lcfname');

            modal.find('#modal-objectid').html(objectid);
            modal.find('#modal-collectionid').html(collection);

            if (lcfname.indexOf('unavailable') != -1) {
                modal.find('#modal-downloadlc')
                    .addClass('disabled')
                    .html('No light curve available');
            }

            else {
                var lcfbasename = lcfname.split('/');
                lcfbasename = lcfbasename[lcfbasename.length-1];

                modal.find('#modal-downloadlc')
                    .attr('href',lcfname)
                    .attr('download',lcfbasename);
            }

            // fire the objectinfo function
            lcc_objectinfo.get_object_info(collection, objectid, '.modal-body');

        });


    },


    // this parses the filter control results for any searchtype it is pointed
    // to. returns an SQL string that can be validated by the backend.
    parse_column_filters: function(target) {

        var filterbucket_elem = $('#' + target + '-filterbucket');
        var filterbucket_items = filterbucket_elem.children();

        var filters = [];
        var filter_cols = [];

        // go through each of the filter items and parse them
        filterbucket_items.each( function (i) {

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
                    var dataset_csv = result[rowind]['dataset_csv'];
                    var lczip_fpath = result[rowind]['lczip_fpath'];

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
                    var csv_download = '';
                    var lczip_download = '';

                    if (dataset_fpath != null) {
                        dataset_download = '<a download rel="nofollow" ' +
                            'href="' + dataset_fpath +
                            '" title="download search results pickle">' +
                            'dataset pickle' +
                            '</a>';
                    }
                    if (dataset_csv != null) {
                        csv_download = '<a download rel="nofollow" ' +
                            'href="' + dataset_csv +
                            '" title="download search results CSV">' +
                            'dataset CSV' +
                            '</a>';
                    }

                    if (lczip_fpath != null) {
                        lczip_download = '<a download rel="nofollow" ' +
                            'href="' + lczip_fpath +
                            '" title="download light curves ZIP">' +
                            'light curve ZIP' +
                            '</a>';
                    }

                    // format the column
                    var table_downloadlinks = '<td>' +
                        dataset_download + '<br>' +
                        csv_download + '<br>' +
                        lczip_download + '</td>';


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

            if (xhr.status == 500) {
                message = 'Something went wrong with the server backend ' +
                    ' while trying to fetch a list of recent datasets';
            }

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
                    collection_selectboxes.each(function () {

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
                    var table_column_desc = '<td width="150">' +
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
                        '<td width="200"><code>' +
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

            if (xhr.status == 500) {
                message = 'Something went wrong with the LCC server backend ' +
                    'while trying to fetch a list of all LC collections';
            }

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
    decimal_regex: /^(\w+)\s(\d{1,3}\.?\d*)\s([+-]?\d{1,2}\.?\d*)$/,

    sexagesimal_regex: /^(\w+)\s(\d{1,2}[ :]\d{2}[ :]\d{2}\.?\d*)\s([+-]?\d{1,2}[: ]\d{2}[: ]\d{2}\.?\d*)$/,

    // this validates the lines in the xmatch input
    validate_xmatch_query: function (target) {

        var xmt = $(target).val().split('\n');

        if (xmt.length > 5000) {
            lcc_ui.alert_box('Cross-match input is limited to 5000 lines',
                             'secondary');
            return false;
        }

        var msgs = [], oklines = 0;
        var decimalok = false, sexagesok = false, commentok = false;

        for (let line of xmt) {

            decimalok = lcc_search.decimal_regex.test(line);
            sexagesok = lcc_search.sexagesimal_regex.test(line);
            commentok = line.startsWith('#');

            if (!commentok && !(decimalok || sexagesok)) {
                msgs.push('# ' + line + '  <---- could not parse this line');
            }
            else {
                msgs.push(line);
                oklines++;
            }

        }

        if (oklines == xmt.length) {
            return true;
        }
        else {
            lcc_ui.alert_box(
                'Some cross-match input lines ' +
                    'were invalid and have been commented out. ' +
                    'See the messages below or ' +
                    'click Search to continue with any remaining input.',
                'secondary'
            );
            $(target).val(msgs.join('\n'));
            return false;
        }

    },


    // this runs the query and deals with the results
    run_search_query: function (url,
                                data,
                                method,
                                target,
                                ispublic,
                                nrun) {

        var oboe_options;

        if (method == 'POST') {

            // encode the data into form-urlencoded and put it into the body
            var headers = {'Content-Type': 'application/x-www-form-urlencoded',
                           'X-Xsrftoken': data._xsrf};
            var body = $.param(data);

            oboe_options = {
                url: url,
                method: 'POST',
                headers: headers,
                body: body
            };

        }

        else {

            var params = $.param(data);
            oboe_options = {
                url: url + '?' + params,
                method: 'GET',
            };

        }

        // fire the request
        oboe(oboe_options).node('{message}', function (msgdata) {

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
                $('#' + target + '-submit').attr('disabled',false);

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
                            $('#datasets-tab-icon')
                                .removeClass('animated bounce');
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
                            msgdata.result.seturl +
                            '">its dataset page</a> for a ' +
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
            .fail(function () {

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

        return nrun;
    },


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
        proceed_step1 = lcc_search.validate_xmatch_query('#xmatch-query');
        var xmatchtext = $('#xmatch-query').val().trim();

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
            nrun = lcc_search.run_search_query(posturl,
                                               postparams,
                                               'POST',
                                               'xmatch',
                                               ispublic,
                                               nrun);

        }
        else {
            var error_message =
                "Invalid input in the cross-match object list input box.";
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

            // use the run_search_query to hit the backend
            nrun = lcc_search.run_search_query(geturl,
                                               getparams,
                                               'GET',
                                               'columnsearch',
                                               ispublic,
                                               nrun);


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

            // use the run_search_query function to hit the backend
            nrun = lcc_search.run_search_query(geturl,
                                               getparams,
                                               'GET',
                                               'ftsquery',
                                               ispublic,
                                               nrun);

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

            // use the run_search_query function to hit the backend
            nrun = lcc_search.run_search_query(geturl,
                                               getparams,
                                               'GET',
                                               'conesearch',
                                               ispublic,
                                               nrun);

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

    // these are set so that one doesn't need to redo rendering after the
    // dataset loads
    table_rendered: false,
    columdefs_rendered: false,

    // this renders the column definitions when they're received from the
    // backend
    render_column_definitions: function (data) {

        var colind = 0;
        var columns = data.columns;
        var coldesc = data.coldesc;

        var coldef_rows = [];

        // the first column of the table holds controls for getting
        // object info. add this column first
        $('#lcc-datatable-header').append(
            '<th width="40"></th>'
        );

        // these are used to calculate the full table width
        var column_widths = [40];
        var thiscol_width = null;

        // generate the column names and descriptions, put them into the
        // column definitions table, and also append them to the header
        // row of the data table

        let colind_objectid = 0;
        let colind_collection = columns.length - 1;
        let colind_lcfname = 0;

        for (colind; colind < columns.length; colind++) {

            var this_col = columns[colind];
            if (this_col == 'db_oid') {
                colind_objectid = colind;
            }
            if (this_col == 'collection') {
                colind_collection = colind;
            }
            if (this_col == 'db_lcfname') {
                colind_lcfname = colind;
            }

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
                thiscol_width = parseInt(this_dtype.replace('U',''))*12;
                if (thiscol_width > 400) {
                    thiscol_width = 400;
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
            .reduce(function (acc, curr) {
                return parseInt(acc + curr);
            });
        $('#lcc-datatable').width(table_width);

        // finish up the column defs and write them to the table
        coldef_rows = coldef_rows.join('');
        $('#table-datarows').html(coldef_rows);

        return [colind_objectid, colind_collection, colind_lcfname];
    },


    // this renders the datatable rows as soon as they're received from the
    // backend
    render_datatable_rows: function (data,
                                     colind_objectid,
                                     colind_collection,
                                     colind_lcfname) {
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

        var objectentry_firstcol = '';
        var thisrow = null;
        var thisrow_lclink = null;

        for (rowind; rowind < max_rows; rowind++) {

            // get this object's db_oid and collection. we'll use these
            // to set up the links to checkplot info on-demand in the
            // first column of the table

            thisrow = data.rows[rowind];

            // get this row's light curve if available
            thisrow_lclink = $(thisrow[colind_lcfname]);
            if (thisrow_lclink.text().indexOf('unavailable') != -1) {
                thisrow_lclink = thisrow_lclink.text();
            }
            else {
                thisrow_lclink = thisrow_lclink.attr('href');
            }

            objectentry_firstcol = '<a href="#" rel="nofollow" role="button" ' +
                'data-toggle="modal" data-target="#objectinfo-modal"' +
                'title="get available object information" ' +
                'data-objectid="' + thisrow[colind_objectid] + '" ' +
                'data-collection="' + thisrow[colind_collection] + '" ' +
                'data-lcfname="' + thisrow_lclink + '" ' +
                'class="btn btn-link btn-sm objectinfo-link">' +
                '<img class="table-icon-svg" ' +
                'src="/static/images/twotone-assistant-24px.svg"></a>';
            thisrow.splice(0,0,objectentry_firstcol);

            datarows_elem.append(
                '<tr><td>' +
                    data.rows[rowind].join('</td><td>')
                    .replace('href','rel="nofollow" href') +
                    '</td></tr>'
            );

        }

        // make the table div bottom stick to the bottom of the
        // container so we can have a scrollbar at the bottom

        // calculate the offset
        var datacontainer_offset =
            $('.datatable-container').offset().top;

        $('.datatable-container').height($(window).height() -
                                         datacontainer_offset - 5);

        // set the height appropriately
        $('.dataset-table')
            .height($('.datatable-container').height());

    },


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
                $('#dataset-searchargs').html(
                    '<details><summary>' +
                        data.searchtype
                        .replace('sqlite_','')
                        .replace('postgres_','') +
                        '</summary><pre>' +
                        JSON.stringify(data.searchargs,
                                       null,
                                       2) +
                        '</pre></detail>'
                );

                // 6. collections
                $('#dataset-collections').html( data.collections.join(', '));

                // 7. setpickle
                $('#dataset-setpickle')
                    .html('<a download rel="nofollow" href="' +
                          data.dataset_pickle + '">download file</a>');

                // 9. setcsv
                $('#dataset-setcsv')
                    .html('<a download rel="nofollow" href="' +
                          data.dataset_csv + '">download file</a>');


                // 11. nobjects
                if ('rowstatus' in data) {
                    $('#dataset-nobjects').html(
                        data.nobjects +
                            ' (' +
                            data.rowstatus +
                            ' &mdash; see the ' +
                            '<a download rel="nofollow" href="' +
                            data.dataset_csv + '">dataset CSV</a>' +
                            ' for complete table)'
                    );
                }
                else {
                    $('#dataset-nobjects').html(data.nobjects);
                }


                if (data.lczip != null && data.lczip != undefined) {
                    // 12. lczip
                    $('#dataset-lczip')
                        .html('<a download rel="nofollow" href="' +
                              data.lczip + '">download file</a>');
               }
                else {
                    $('#dataset-lczip').html('not available');
                }


                /////////////////////////////////////
                // fill in the column descriptions //
                /////////////////////////////////////

                var [colind_objectid,
                     colind_collection,
                     colind_lcfname] =
                    lcc_datasets.render_column_definitions(data);

                ////////////////////////////////////////
                // finally, fill in the dataset table //
                ////////////////////////////////////////

                lcc_datasets.render_datatable_rows(data,
                                                   colind_objectid,
                                                   colind_collection,
                                                   colind_lcfname);

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
                    'from the LCC server backend.';

                lcc_ui.alert_box(message, 'danger');

                // clear out the loading indicators at the end
                $('#setload-icon').empty();
                $('#setload-indicator').empty();

            }


        }).fail(function (xhr) {

            var message = 'Could not retrieve the dataset ' +
                'from the LCC server backend.';

            if (xhr.status == 500) {
                message = 'Something went wrong with the server backend ' +
                    'while trying to fetch the dataset.';
            }

            lcc_ui.alert_box(message, 'danger');

            // clear out the loading indicators at the end
            $('#setload-icon').empty();
            $('#setload-indicator').empty();

        });

    }

};



// this contains methods to render objectinfo from checkplot JSONs
var lcc_objectinfo = {

    // this is the ES6 template string for the modal UI
    modal_template: `
<div class="row objectinfo-header">
</div>

<div class="row mt-2 d-flex align-items-center">
  <div class="col-5">
    <canvas id="finderchart"></canvas>
  </div>

  <div class="col-7">
    <img class="magseriesplot">
  </div>
</div>

<div class="row">
  <div class="col-12">

    <nav>
      <div class="nav nav-tabs" id="modal-nav-tab" role="tablist">

        <a class="nav-item nav-link active" id="modal-objectinfo"
           data-toggle="tab" href="#mtab-objectinfo" role="tab"
           aria-controls="modal-objectinfo" aria-selected="true">
          Object details
        </a>

        <a class="nav-item nav-link" id="modal-phasedlcs"
           data-toggle="tab" href="#mtab-phasedlcs" role="tab"
           aria-controls="modal-phasedlcs" aria-selected="false">
          Period search results
        </a>

      </div>
    </nav>

    <div class="tab-content" id="modal-nav-content">

      <div class="tab-pane show active" id="mtab-objectinfo"
           role="tabpanel" aria-labelledby="modal-objectinfo">

        <div class="row mt-2">
          <div class="col-6">

            <table id="objectinfo-basic"
                   class="table table-borderless objectinfo-table">

              <tr>
                <th>Observations</th>
                <td id="obsinfo">blah</td>
              </tr>
              <tr>
                <th>Coords and PM</th>
                <td id="coordspm">blah</td>
              </tr>
              <tr>
                <th>Magnitudes<span id="magnotice"></span></th>
                <td id="mags">blah</td>
              </tr>
              <tr>
                <th>Colors<span id="derednotice"></span></th>
                <td id="colors">blah</td>
              </tr>

            </table>

          </div>

          <div class="col-6">

            <table id="objectinfo-extra"
                   class="table table-borderless objectinfo-table">
            </table>

          </div>

        </div>

        <div class="row mt-2">
          <div class="col-12 lc-download-link">
          </div>
        </div>

      </div>

      <div class="tab-pane" id="mtab-phasedlcs"
           role="tabpanel" aria-labelledby="modal-phasedlcs">

        <div class="row mt-2">
          <div class="col-12">

            <div id="modal-phasedlc-container" class="phasedlc-container">

            </div>

          </div>
        </div>

      </div>

    </div>

  </div>
</div>
`,


    // this decodes a string from base64
    b64_decode: function (str) {
        return decodeURIComponent(window.atob(str).split('').map(function(c) {
            return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
        }).join(''));

    },

    // https://stackoverflow.com/a/26601101
    b64_decode2: function (s) {

        var e={},i,b=0,c,x,l=0,a,r='',w=String.fromCharCode,L=s.length;
        var A="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for(i=0;i<64;i++){e[A.charAt(i)]=i;}
        for(x=0;x<L;x++){
            c=e[s.charAt(x)];b=(b<<6)+c;l+=6;
            while(l>=8){((a=(b>>>(l-=8))&0xff)||(x<(L-2)))&&(r+=w(a));}
        }
        return r;

    },


    // this turns a base64 string into an image by updating its source
    b64_to_image: function (str, targetelem) {

        var datauri = 'data:image/png;base64,' + str;
        $(targetelem).attr('src',datauri);

    },

    // this displays a base64 encoded image on the canvas
    b64_to_canvas: function (str, targetelem) {

        var datauri = 'data:image/png;base64,' + str;
        var newimg = new Image();
        var canvas = document.getElementById(targetelem.replace('#',''));

        var imgheight = 300;
        var imgwidth = 300;
        var cnvwidth = canvas.width;
        canvas.height = cnvwidth;
        var imgscale = cnvwidth/imgwidth;

        var ctx = canvas.getContext('2d');

        // this event listener will fire when the image is loaded
        newimg.addEventListener('load', function () {
            ctx.drawImage(newimg,
                          0,
                          0,
                          imgwidth*imgscale,
                          imgheight*imgscale);
        });

        // load the image and fire the listener
        newimg.src = datauri;

    },

    // this holds imagedata for the canvas so we can restore changed parts of
    // the image
    pixeltracker: null,

    // this writes out the template string containing the modal UI to the modal
    // if the object request succeeds
    render_modal_template: function(target) {
        $(target).html(lcc_objectinfo.modal_template);
    },

    render_infotable: function (currcp) {

        // get the number of detections
        var objndet = currcp.objectinfo.ndet;

        if (objndet == undefined) {
            objndet = currcp.magseries_ndet;
        }

        // get the observatory information
        if ('stations' in currcp.objectinfo) {

            // get the HAT stations
            var hatstations = currcp.objectinfo.stations;
            var splitstations = '';

            if (hatstations != undefined && hatstations) {
                splitstations = (String(hatstations).split(',')).join(', ');
            }

            // update the objectinfo
            var hatinfo = '<strong>' +
                splitstations +
                '</strong><br>' +
                '<strong>LC points:</strong> ' + objndet;
            $('#obsinfo').html(hatinfo);

        }
        else if ('observatory' in currcp.objectinfo) {

            var obsinfo = '<strong'> +
                currcp.objectinfo.observatory + '</strong><br>' +
                '<strong>LC points:</strong> ' + objndet;
            $('#obsinfo').html(obsinfo);

        }
        else if ('telescope' in currcp.objectinfo) {

            var telinfo = '<strong'> +
                currcp.objectinfo.telescope + '</strong><br>' +
                '<strong>LC points:</strong> ' + objndet;
            $('#obsinfo').html(telinfo);

        }
        else {
            $('#obsinfo').html('<strong>LC points:</strong> ' + objndet);
        }

        // get the GAIA status (useful for G mags, colors, etc.)
        var gaia_ok = false;
        var gaia_message = (
            'no GAIA cross-match information available'
        );
        if (currcp.objectinfo.gaia_status != undefined) {
            gaia_ok =
                currcp.objectinfo.gaia_status.indexOf('ok') != -1;
            gaia_message =
                currcp.objectinfo.gaia_status.split(':')[1];
        }

        // get the SIMBAD status (useful for G mags, colors, etc.)
        var simbad_ok = false;
        var simbad_message = (
            'no SIMBAD cross-match information available'
        );
        if (currcp.objectinfo.simbad_status != undefined) {
            simbad_ok =
                currcp.objectinfo.simbad_status.indexOf('ok') != -1;
            simbad_message =
                currcp.objectinfo.simbad_status.split(':')[1];
        }


        //
        // get the coordinates and PM
        //
        var objectra = '';
        var objectdecl = '';
        var objectgl = '';
        var objectgb = '';
        var objectpm = '';
        var objectrpmj = '';


        // ra
        if (currcp.objectinfo.ra != undefined) {
            objectra = currcp.objectinfo.ra.toFixed(3);
        }
        // decl
        if (currcp.objectinfo.decl != undefined) {
            objectdecl = currcp.objectinfo.decl.toFixed(3);
        }
        // gl
        if (currcp.objectinfo.gl != undefined) {
            objectgl = currcp.objectinfo.gl.toFixed(3);
        }
        // gb
        if (currcp.objectinfo.gb != undefined) {
            objectgb = currcp.objectinfo.gb.toFixed(3);
        }
        // total proper motion
        if (currcp.objectinfo.propermotion != undefined) {
            objectpm = currcp.objectinfo.propermotion.toFixed(2)
                + ' mas/yr';

            if ( (currcp.objectinfo.pmra_source != undefined) &&
                 (currcp.objectinfo.pmdecl_source != undefined) ) {

                var pmra_source = currcp.objectinfo.pmra_source;
                var pmdecl_source = currcp.objectinfo.pmdecl_source;

                // note if the propermotion came from GAIA
                if ( (pmra_source == pmdecl_source) &&
                     (pmra_source == 'gaia') ) {
                    objectpm = objectpm + ' (from GAIA)';
                }

            }

        }

        // reduced proper motion [Jmag]
        if (currcp.objectinfo.rpmj != undefined) {
            objectrpmj = currcp.objectinfo.rpmj.toFixed(2);
        }
        else if (currcp.objectinfo.reducedpropermotion != undefined) {
            objectrpmj = currcp.objectinfo.reducedpropermotion.toFixed(2);
        }

        // format the coordinates and PM
        var coordspm =
            '<strong>Equatorial (&alpha;, &delta;):</strong> (' +
            objectra + ', ' + objectdecl + ')<br>' +
            '<strong>Galactic (l, b):</strong> (' +
            objectgl + ', ' + objectgb + ')<br>' +
            '<strong>Total PM:</strong> ' + objectpm + '<br>' +
            '<strong>Reduced PM<sub>J</sub>:</strong> ' + objectrpmj;

        // see if we can get the GAIA parallax
        if (gaia_ok && currcp.objectinfo.gaia_parallaxes[0]) {

            var gaia_parallax = currcp.objectinfo.gaia_parallaxes[0].toFixed(2);
            coordspm = coordspm + '<br>' +
                '<strong>GAIA parallax:</strong> ' +
                gaia_parallax + ' mas';

        }

        $('#coordspm').html(coordspm);

        //
        // handle the mags
        //

        var magnotices = [];

        if (currcp.objectinfo.bmagfromjhk != undefined &&
            currcp.objectinfo.bmagfromjhk) {
            magnotices.push('B');
        }
        if (currcp.objectinfo.vmagfromjhk != undefined &&
            currcp.objectinfo.vmagfromjhk) {
            magnotices.push('V');
        }
        if (currcp.objectinfo.sdssufromjhk != undefined &&
            currcp.objectinfo.sdssufromjhk) {
            magnotices.push('u');
        }
        if (currcp.objectinfo.sdssgfromjhk != undefined &&
            currcp.objectinfo.sdssgfromjhk) {
            magnotices.push('g');
        }
        if (currcp.objectinfo.sdssrfromjhk != undefined &&
            currcp.objectinfo.sdssrfromjhk) {
            magnotices.push('r');
        }
        if (currcp.objectinfo.sdssifromjhk != undefined &&
            currcp.objectinfo.sdssifromjhk) {
            magnotices.push('i');
        }
        if (currcp.objectinfo.sdsszfromjhk != undefined &&
            currcp.objectinfo.sdsszfromjhk) {
            magnotices.push('z');
        }

        if (magnotices.length > 0) {
            $('#magnotice').html('<br>(' + magnotices.join('') +
                                 ' via JHK transform)');
        }

        // set up the cmdplots property for currcp
        currcp.cmdplots = [];


        // set up GAIA info
        var gaiamag = 'N/A';
        var gaiakcolor = '';
        var gaiaabsmag = 'N/A';
        if (gaia_ok) {
            gaiamag = currcp.objectinfo.gaia_mags[0].toFixed(3);
            if (currcp.objectinfo.gaiak_colors != null) {
                gaiakcolor = currcp.objectinfo.gaiak_colors[0].toFixed(3);
            }
            gaiaabsmag = currcp.objectinfo.gaia_absolute_mags[0].toFixed(3);
        }

        //
        // now we need to handle both generations of checkplots
        //

        // this is for the current generation of checkplots
        if (currcp.objectinfo.hasOwnProperty('available_bands')) {

            var mind = 0;
            var cind = 0;
            var mlen = currcp.objectinfo['available_bands'].length;
            var clen = currcp.objectinfo['available_colors'].length;

            var maglabel_pairs = [];
            var colorlabel_pairs = [];

            var thiskey = null;
            var thislabel = null;
            var thisval = null;

            // generate the mag-label pairs
            for (mind; mind < mlen; mind++) {

                thiskey = currcp.objectinfo['available_bands'][mind];
                thislabel =
                    currcp.objectinfo['available_band_labels'][mind];
                if (!isNaN(parseFloat(currcp.objectinfo[thiskey]))) {
                    thisval = currcp.objectinfo[thiskey].toFixed(3);
                }
                else {
                    thisval = '';
                }
                maglabel_pairs.push('<span class="no-wrap-break">' +
                                    '<strong><em>' +
                                    thislabel +
                                    '</em>:</strong> ' +
                                    thisval +
                                    '</span>');

            }

            // generate the color-label pairs
            for (cind; cind < clen; cind++) {

                thiskey = currcp.objectinfo['available_colors'][cind];
                thislabel =
                    currcp.objectinfo['available_color_labels'][cind];

                if (!isNaN(parseFloat(currcp.objectinfo[thiskey]))) {
                    thisval = currcp.objectinfo[thiskey].toFixed(2);
                }
                else {
                    thisval = '';
                }

                if (currcp.objectinfo.dereddened != undefined &&
                    currcp.objectinfo.dereddened) {
                    thislabel = '(' + thislabel
                        + ')<sub>0</sub>';
                    $('#derednotice').html('<br>(dereddened)');
                }
                else {
                    thislabel = '(' + thislabel + ')';
                }

                colorlabel_pairs.push('<span class="no-wrap-break">' +
                                      '<strong><em>' +
                                      thislabel +
                                      '</em>:</strong> ' +
                                      thisval +
                                      '</span>');

            }


            // now add the GAIA information if it exists
            maglabel_pairs.push(
                '<span class="no-wrap-break">' +
                    '<strong><em>GAIA G</em>:</strong> ' +
                    gaiamag +
                    '</span>'
            );
            maglabel_pairs.push(
                '<span class="no-wrap-break">' +
                    '<strong><em>GAIA M<sub>G</sub></em>:</strong> ' +
                    gaiaabsmag +
                    '</span>'
            );
            colorlabel_pairs.push(
                '<span class="no-wrap-break">' +
                    '<strong><em>G - K<sub>s</sub></em>:</strong> ' +
                    gaiakcolor +
                    '</span>'
            );


            maglabel_pairs = maglabel_pairs.join(', ');
            colorlabel_pairs = colorlabel_pairs.join(', ');

            $('#mags').html(maglabel_pairs);
            $('#colors').html(colorlabel_pairs);

        }

        // this is for the older generation of checkplots
        else {

            var [sdssu, sdssg, sdssr, sdssi, sdssz] = ['','','','',''];
            var [jmag, hmag, kmag] = ['','',''];
            var [bmag, vmag] = ['',''];

            if (!isNaN(parseFloat(currcp.objectinfo.sdssu))) {
                sdssu = currcp.objectinfo.sdssu.toFixed(3);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.sdssg))) {
                sdssg = currcp.objectinfo.sdssg.toFixed(3);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.sdssr))) {
                sdssr = currcp.objectinfo.sdssr.toFixed(3);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.sdssi))) {
                sdssi = currcp.objectinfo.sdssi.toFixed(3);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.sdssz))) {
                sdssz = currcp.objectinfo.sdssz.toFixed(3);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.jmag))) {
                jmag = currcp.objectinfo.jmag.toFixed(3);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.hmag))) {
                hmag = currcp.objectinfo.hmag.toFixed(3);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.kmag))) {
                kmag = currcp.objectinfo.kmag.toFixed(3);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.bmag))) {
                bmag = currcp.objectinfo.bmag.toFixed(3);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.vmag))) {
                vmag = currcp.objectinfo.vmag.toFixed(3);
            }

            var mags = '<strong><em>ugriz</em>:</strong> ' +
                sdssu + ', ' +
                sdssg + ', ' +
                sdssr + ', ' +
                sdssi + ', ' +
                sdssz + '<br>' +
                '<strong><em>JHK</em>:</strong> ' +
                jmag + ', ' +
                hmag + ', ' +
                kmag + '<br>' +
                '<strong><em>BV</em>:</strong> ' +
                bmag + ', ' +
                vmag + '<br>' +
                '<strong><em>GAIA G</em>:</strong> ' +
                gaiamag + ', ' +
                '<strong><em>GAIA M<sub>G</sub></em>:</strong> ' +
                gaiaabsmag;

            $('#mags').html(mags);

            //
            // handle the colors
            //
            var [bvcolor, vkcolor, jkcolor] = ['','',''];
            var [ijcolor, gkcolor, grcolor] = ['','',''];

            if (!isNaN(parseFloat(currcp.objectinfo.bvcolor))) {
                bvcolor = currcp.objectinfo.bvcolor.toFixed(2);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.vkcolor))) {
                vkcolor = currcp.objectinfo.vkcolor.toFixed(2);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.jkcolor))) {
                jkcolor = currcp.objectinfo.jkcolor.toFixed(2);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.ijcolor))) {
                ijcolor = currcp.objectinfo.ijcolor.toFixed(2);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.gkcolor))) {
                gkcolor = currcp.objectinfo.gkcolor.toFixed(2);
            }
            if (!isNaN(parseFloat(currcp.objectinfo.grcolor))) {
                grcolor = currcp.objectinfo.grcolor.toFixed(2);
            }

            var colors =
                '<strong><em>(B - V)</em>:</strong> ' +
                bvcolor + ',  ' +
                '<strong><em>(V - K)</em>:</strong> ' +
                vkcolor + '<br>' +
                '<strong><em>(J - K)</em>:</strong> ' +
                jkcolor + ',  ' +
                '<strong><em>(i - J)</em>:</strong> ' +
                ijcolor + '<br>' +
                '<strong><em>(g - K)</em>:</strong> ' +
                gkcolor + ',  ' +
                '<strong><em>(g - r)</em>:</strong> ' +
                grcolor;

            if (currcp.objectinfo.dereddened != undefined &&
                currcp.objectinfo.dereddened) {
                $('#derednotice').html('<br>(dereddened)');
            }

            // format the colors
            $('#colors').html(colors);

        }


        //
        // additional stuff
        //

        // first, empty out the extra info table
        $("#objectinfo-extra").empty();

        // add the color classification if available
        if (currcp.objectinfo.color_classes != undefined &&
            currcp.objectinfo.color_classes.length > 0) {

            var formatted_color_classes =
                currcp.objectinfo.color_classes.join(', ');
            $('#objectinfo-extra')
                .append(
                    "<tr>" +
                        "<th>SDSS/SEGUE color class</th>" +
                        "<td>" + formatted_color_classes + "</td>" +
                        "</tr>"
                );

        }

        // neighbors
        if (currcp.objectinfo.neighbors != undefined ||
            (currcp.objectinfo.gaia_ids != undefined &&
             currcp.objectinfo.gaia_ids.length > 0) ) {

            var formatted_neighbors =
                '<strong><em>from LCs in collection</em>:</strong> 0<br>';
            if (currcp.objectinfo.neighbors > 0) {

                formatted_neighbors =
                    '<strong><em>from LCs in collection</em>:</strong> ' +
                    currcp.objectinfo.neighbors + '<br>' +
                    '<em>closest:</em> ' +
                    currcp.objectinfo.closestdistarcsec.toFixed(1) +
                    '&Prime; &rarr; <span class="text-primary">N1: ' +
                    currcp.neighbors[0]['objectid'] + '</span><br>';
            }

            var formatted_gaia =
                '<strong><em>GAIA query failed</em>:</strong> ' +
                gaia_message;
            if (gaia_ok) {
                formatted_gaia =
                    '<strong><em>from GAIA</em>:</strong> ' +
                    (currcp.objectinfo.gaia_ids.length - 1) + '<br>' +
                    '<em>closest distance</em>: ' +
                    currcp.objectinfo.gaia_closest_distarcsec.toFixed(2) +
                    '&Prime;<br>' +
                    '<em>closest G mag (obj - nbr)</em>: ' +
                    currcp.objectinfo.gaia_closest_gmagdiff.toFixed(2) +
                    ' mag';

            }

            $('#objectinfo-extra').append(
                "<tr>" +
                    "<th>Neighbors within " +
                    currcp.objectinfo.searchradarcsec.toFixed(1) +
                    "&Prime;</th>" +
                    "<td>" + formatted_neighbors +
                    formatted_gaia +
                    "</td>" +
                    "</tr>"
            );


        }

        // get the CMDs for this object if there are any
        if (currcp.hasOwnProperty('colormagdiagram') &&
            currcp.colormagdiagram != null) {

            var cmdlist = Object.getOwnPropertyNames(
                currcp.colormagdiagram
            );

            var cmdkey = '<tr><th>' +
                'Color-magnitude diagrams' +
                '</th>';

            var cmdval = '<td>';
            var cmdimgs = [];

            // prepare the img divs
            var cmdi = 0;
            for (cmdi; cmdi < cmdlist.length; cmdi++) {

                var thiscmdlabel = cmdlist[cmdi];
                var thiscmdplot = currcp.colormagdiagram[cmdlist[cmdi]];

                var cmddd =
                    '<div class="dropdown">' +
                    '<a href="#" ' +
                    'title="Click to see the ' +
                    thiscmdlabel +
                    ' color-magnitude ' +
                    'diagram for this object" ' +
                    'id="cmd-' + cmdi +
                    '-dropdown" data-toggle="dropdown" ' +
                    'aria-haspopup="true" aria-expanded="false">' +
                    '<strong>' + thiscmdlabel + ' CMD</strong>' +
                    '</a>' +
                    '<div class="dropdown-menu text-sm-center cmd-dn" ' +
                    'aria-labelledby="cmd-' + cmdi + '-dropdown">' +
                    '<img id="cmd-' + cmdi +'-plot" class="img-fluid">' +
                    '</div></div>';
                cmdval = cmdval + cmddd;
                cmdimgs.push('#cmd-' + cmdi + '-plot');

            }

            cmdval = cmdkey + cmdval + '</td></tr>';
            $('#objectinfo-extra').append(cmdval);

            // now populate the img divs with the actual CMD images
            cmdi = 0;
            for (cmdi; cmdi < cmdlist.length; cmdi++) {

                thiscmdlabel = cmdlist[cmdi];
                thiscmdplot = currcp.colormagdiagram[thiscmdlabel];
                lcc_objectinfo.b64_to_image(thiscmdplot, cmdimgs[cmdi]);

            }

        }

        // get SIMBAD info if possible
        if (currcp.objectinfo.simbad_status != undefined) {

            var formatted_simbad =
                '<strong><em>SIMBAD query failed</em>:</strong> ' +
                simbad_message;
            if (simbad_ok) {

                var simbad_best_allids =
                    currcp.objectinfo.simbad_best_allids
                    .split('|').join(', ');

                formatted_simbad =
                    '<strong><em>matching objects</em>:</strong> ' +
                    (currcp.objectinfo.simbad_nmatches) + '<br>' +
                    '<em>closest distance</em>: ' +
                    currcp.objectinfo.simbad_best_distarcsec.toFixed(2) +
                    '&Prime;<br>' +
                    '<em>closest object ID</em>: ' +
                    currcp.objectinfo.simbad_best_mainid + '<br>' +
                    '<em>closest object type</em>: ' +
                    currcp.objectinfo.simbad_best_objtype + '<br>' +
                    '<em>closest object other IDs</em>: ' +
                    simbad_best_allids;

            }

            $('#objectinfo-extra')
                .append(
                    "<tr>" +
                        "<th>SIMBAD information</th>" +
                        "<td>" + formatted_simbad +
                        "</td>" +
                        "</tr>"
                );

        }

        // get the period and variability info -> .objectinfo-extra
        if ('varinfo' in currcp) {

            var objectisvar = currcp.varinfo.objectisvar;

            if (parseInt(objectisvar) == 1) {
                objectisvar =
                    '<span class="text-success">probably variable</span>';
            }
            else if (parseInt(objectisvar) == 2) {
                objectisvar = 'probably not variable';
            }
            else if (parseInt(objectisvar) == 3) {
                objectisvar = 'may be variable, but difficult to tell.';
            }
            else {
                objectisvar = 'unknown or not checked yet';
            }

            var objectperiod = currcp.varinfo.varperiod;
            if (objectperiod != null && objectperiod != undefined) {
                objectperiod = objectperiod.toFixed(6);
            }
            else {
                objectperiod = 'undetermined';
            }

            var objectepoch = currcp.varinfo.varepoch;
            if (objectepoch != null && objectepoch != undefined) {
                objectepoch = objectepoch.toFixed(5);
            }
            else {
                objectepoch = 'undetermined';
            }

            var vartags = 'none';
            if ('vartags' in currcp.varinfo &&
                (currcp.varinfo.vartags != null ||
                 currcp.varinfo.vartags != undefined) &&
                currcp.varinfo.vartags.length > 0) {

                vartags = currcp.varinfo.vartags.split(', ').map(
                    function (elem) {
                        return '<span class="badge badge-success">' +
                            elem + '</span>';
                    }).join(' ');

            }

            $('#objectinfo-extra')
                .append('<tr>' +
                        '<th>Variable star?</th>' +
                        '<td>' + objectisvar + '</td></tr>');
            $('#objectinfo-extra')
                .append('<tr>' +
                        '<th>Best period and epoch</th>' +
                        '<td><em>Period [days]</em>: ' + objectperiod + '<br>' +
                        '<em>Epoch [RJD]</em>: ' + objectepoch + '</td>' +
                        '</tr>');
            $('#objectinfo-extra')
                .append('<tr>' +
                        '<th>Variability tags</th>' +
                        '<td>' + vartags + '</td></tr>');

        }

        // get the object tags -> .objectinfo-table
        if ('objecttags' in currcp.objectinfo &&
            (currcp.objectinfo.objecttags != null ||
             currcp.objectinfo.objecttags != undefined) &&
            currcp.objectinfo.objecttags.length > 0) {

            var objecttags = currcp.objectinfo.objecttags.split(', ').map(
                function (elem) {
                    return '<span class="badge badge-secondary">' +
                        elem + '</span>';
                }).join(' ');

            $('#objectinfo-basic').append(
                '<tr>' +
                    '<th>Object tags</th>' +
                    '<td>' + objecttags + '</td></tr>'
            );

        }

        // get the object comments -> .objectinfo-table
        if ('objectcomments' in currcp &&
            (currcp.objectcomments != null ||
             currcp.objectcomments != undefined) &&
            currcp.objectcomments.length > 0) {

            $('#objectinfo-basic').append(
                '<tr>' +
                    '<th>Comments</th>' +
                    '<td>' + currcp.objectcomments + '</td></tr>'
            );

        }


    },

    render_pfresult: function (currcp) {

        // first, check if we have any pfmethods at all
        var pfmethods = null;

        if ('pfmethods' in currcp &&
            currcp.pfmethods != null &&
            currcp.pfmethods.length > 0) {

            pfmethods = currcp.pfmethods;

            // we'll make tiles of best 3 phased LCs for each pfmethod
            // -> 3 x nrows using one row per pfmethod. the first tile will
            // be the periodogram, the other 3 will be the best 3 period
            // phased LCs. we'll use the zoom-in technique from the
            //checkplotserver since these plots will be tiny
            // to fit into the modal

            var [row, header, container] = ['', '', ''];
            var [col1, col2, col3, col4] = ['', '', '', ''];

            for (let pfm of pfmethods) {

                var pfm_label = pfm.split('-');
                pfm_label = pfm_label[pfm_label.length - 1];

                row = '<div class="row mt-2">';

                header = '<div class="col-12">' +
                    '<h6>' +
                    'Period-finder: ' + pfm_label.toUpperCase() +
                    ', best period [days]: ' +
                    currcp[pfm].phasedlc0.period.toFixed(6) +
                    ', best epoch [RJD]: ' +
                    currcp[pfm].phasedlc0.epoch.toFixed(5) +
                    '</h6></div>';

                col1 = '<div class="col-3 px-0">' +
                    '<img src="data:image/png;base64,' +
                    currcp[pfm].periodogram.replace('null','NaN') +
                    '" ' +
                    'class="img-fluid zoomable-tile" id="periodogram-' +
                    pfm + '"></div>';

                col2 = '<div class="col-3 px-0">' +
                    '<img src="data:image/png;base64,' +
                    currcp[pfm]['phasedlc0']['plot'].replace('null','NaN') +
                    '" ' +
                    'class="img-fluid zoomable-tile" id="phasedlc-0-' +
                    pfm + '"></div>';

                col3 = '<div class="col-3 px-0">' +
                    '<img src="data:image/png;base64,' +
                    currcp[pfm]['phasedlc1']['plot'].replace('null','NaN') +
                    '" ' +
                    'class="img-fluid zoomable-tile" id="phasedlc-1-' +
                    pfm + '"></div>';

                col4 = '<div class="col-3 px-0">' +
                    '<img src="data:image/png;base64,' +
                    currcp[pfm]['phasedlc2']['plot'].replace('null','NaN') +
                    '" ' +
                    'class="img-fluid zoomable-tile" id="phasedlc-2-' +
                    pfm + '"></div>';

                container = row + header + '</div>' +
                    row + col1 + col2 + col3 + col4 + '</div>';
                $('#modal-phasedlc-container').append(container);

            }

        }

        else {
            $('#modal-phasedlc-container').html(
                "<p>Period-finding for general stellar variability " +
                    "has not been run on this object, either " +
                    "because it didn't look like a variable star, " +
                    "or we haven't gotten around to it just yet. " +
                    "The light curve is available for download " +
                    "if you'd like to give it a go.</p>"
            );
        }

    },

    // this function rolls up the others above
    get_object_info: function (collection, objectid, target, separatepage) {

        // we'll hit the objectinfo API for info on this object
        var geturl = '/api/object';
        var params = {objectid:objectid,
                      collection:collection};

        // put in a message saying we're getting info
        $(target)
            .html('<div class="row"><div class="col-12">' +
                  '<h6>Looking up this object...</h6></div></div>');

        $.getJSON(geturl, params, function (data) {

            var result = data.result;

            // render the modal UI
            lcc_objectinfo.render_modal_template(target);

            // add in the finder chart
            if ('finderchart' in result && result.finderchart != null) {
                var finderchart = result.finderchart;
                lcc_objectinfo.b64_to_canvas(finderchart, '#finderchart');
            }

            // add in the object light curve
            if ('magseries' in result && result.magseries != null) {
                var magseries = result.magseries;
                lcc_objectinfo.b64_to_image(magseries, '.magseriesplot');
            }

            // add in the object's info table
            lcc_objectinfo.render_infotable(result);

            // render the object's lightcurve download link if we're in separate
            // page mode. also render the object's collection and title
            if (separatepage != undefined && separatepage == true) {

                $('.lc-download-link').html(
                    '<a rel="nofollow" class="btn btn-primary" ' +
                        'href="/l/' + collection +
                        '/' + objectid + '-csvlc.gz" download="' +
                        objectid + '-csvlc.gz' +
                        '">Download light curve</a>'
                );

                $('.objectinfo-header')
                    .addClass('mt-2')
                    .html('<div class="col-12"><h2>' + objectid +
                          ' in collection <code>' +
                          collection.replace('-','_') + '</code></h2>');

            }

            // add in the object's phased LCs from all available PFMETHODS
            lcc_objectinfo.render_pfresult(result);


        }).fail(function (xhr) {

            // this means the object wasn't found
            if (xhr.status == 404) {
                $(target).html(
                    '<div class="row mt-2"><div class="col-12">' +
                        '<h6>Sorry, no detailed information ' +
                        'is available for this object.</h6></div</div>'
                );
            }

            // any other status code means the backend threw an error
            else {
                $(target).html(
                    '<div class="row mt-2"><div class="col-12">' +
                        '<h6>Sorry, something broke while trying ' +
                        'to look up this object.</h6></div></div>'
                );

            }

        });

    }


};
