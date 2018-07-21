var lcc_ui = {

    // alert types: 'primary', 'secondary', 'success', 'danger', 'warning',
    //              'info', 'light', 'dark'
    alert_box: function(message, alert_type) {

        var alert = '<div class="mt-4 alert alert-' + alert_type +
            ' alert-dismissible fade show"' +
            ' role="alert">' + message +
            '<button type="button" class="close" data-dismiss="alert" ' +
            'aria-label="Close"><span aria-hidden="true">&times;</span>' +
            '</button></div>';

        // can stack multiple alerts
        $('#alert-box').append(alert);

    },

    // this function generates a spinner
    make_spinner: function (message, target) {

        var spinner = '<div class="spinner">' +
            '<div class="rect1"></div>' +
            '<div class="rect2"></div>' +
            '<div class="rect3"></div>' +
            '<div class="rect4"></div>' +
            '<div class="rect5"></div>' +
            '</div>' + message;

        $(target).html(spinner);

    },

    action_setup: function () {

        // bind the quicksearch type select
        $('#qt').on('change', function(event) {

            if ($('#qt').val() == 'conesearch') {

                $('#q').attr(
                    'placeholder',
                    ('[RA] [Dec] '+
                     '[optional radius (arcmin)]')
                );
            }

            else if ($('#qt').val() == 'objectidsearch') {
                $('#q').attr(
                    'placeholder',
                    'e.g. EPIC216858738 / HAT-579-0111255 etc.'
                );

            }

            else if ($('#qt').val() == 'xmatchsearch') {
                $('#q').attr(
                    'placeholder',
                    'e.g. obj1 ra1 dec1, obj2 ra2 dec2, ...'
                );

            }

        });

    },

    get_recent_datasets: function(nrecent) {

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
                        '<a href="/set/' + setid + '">' +
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
                    if (dataset_fpath != null) {
                        var dataset_download = '<a href="' + dataset_fpath +
                            '" title="download search results pickle">' +
                            'dataset pickle' +
                            '</a> <span data-toggle="tooltip" title="' +
                            dataset_shasum + '">' +
                            '[SHA256]</span>';
                    }
                    else {
                        var dataset_download = '';
                    }

                    if (lczip_fpath != null) {
                        var lczip_download = '<a href="' + lczip_fpath +
                            '" title="download light curves ZIP">' +
                            'light curve ZIP' +
                            '</a> <span data-toggle="tooltip" title="' +
                            lczip_shasum + '">' +
                            '[SHA256]</span>';
                    }
                    else {
                        var lczip_download = '';
                    }

                    if (pfzip_fpath != null) {
                        var pfzip_download = '<a href="' + pfzip_fpath +
                            '" title="download period-finding results ZIP">' +
                            'period-finding result pickles ZIP' +
                            '</a> <span data-toggle="tooltip" title="' +
                            pfzip_shasum + '">' +
                            '[SHA256]</span>';
                    }
                    else {
                        var pfzip_download = '';
                    }

                    if (cpzip_fpath != null) {
                        var cpzip_download = '<a href="' + cpzip_fpath +
                            '" title="download checkplot pickles ZIP">' +
                            'checkplot pickles ZIP' +
                            '</a> <span data-toggle="tooltip" title="' +
                            cpzip_shasum + '">' +
                            '[SHA256]</span>';
                    }
                    else {
                        var cpzip_download = '';
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
                var collection_ids = collections.collection_id;

                //we use all available columns to figure out the common cols and
                //also the per collection special cols
                var available_columns = result.available_columns;
                var indexed_columns = result.available_index_columns;
                var fts_columns = result.available_fts_columns;

                var rowind = 0;

                for (rowind; rowind < collection_ids.length; rowind++) {

                    //
                    // name column
                    //
                    var db_collid = collections.db_collection_id[rowind];
                    var collname = collections.name[rowind];
                    var table_column_name = '<td width="80">' +
                        collname + ' (<code>' + db_collid + '</code>)' +
                        '</td>';

                    //
                    // description column
                    //
                    var desc = collections.description[rowind];
                    var nobjects = collections.nobjects[rowind];
                    var table_column_desc = '<td width="100">' +
                        desc + '<br><br>Number of objects: <code>' +
                        nobjects +
                        '</code></td>';

                    //
                    // extent column
                    //
                    var minra = collections.minra[rowind];
                    var maxra = collections.maxra[rowind];
                    var mindecl = collections.mindecl[rowind];
                    var maxdecl = collections.maxdecl[rowind];

                    var center_ra = math.format((minra + maxra)/2.0,5)
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
                    // FIXME: prepend the db_collection_id for each colname?
                    var columns =
                        collections.columnlist[rowind].split(',').sort();

                    // get the indexed columns for the collection
                    // FIXME: prepend the db_collection_id for each colname?
                    var indexedcols =
                        collections.indexedcols[rowind].split(',').sort();

                    // get the FTS columns for this collection
                    // FIXME: prepend the db_collection_id for each colname?
                    var ftscols =
                        collections.ftsindexedcols[rowind].split(',').sort();

                    var colind = 0;

                    // we'll make an list with three sections
                    // 1. indexed columns
                    // 2. full-text search enabled columns
                    // 3. other columns
                    var formatted_colspec = [];

                    for (colind; colind < columns.length; colind++) {

                        var thiscol = columns[colind];

                        var thiscol_title =
                            collections.columnjson[rowind][thiscol]['title'];
                        var thiscol_desc =
                            collections.columnjson[rowind][thiscol]['description'];

                        if (thiscol_title != null && thiscol_desc != null) {

                            var col_popover = '<span class="pop-span" ' +
                                'data-toggle="popover" ' +
                                'data-placement="top" ' +
                                'data-title="' + thiscol_title + '" ' +
                                'data-content="' + thiscol_desc + '" ' +
                                'data-html="true">' + thiscol + '</span>';

                            if (ftscols.indexOf(thiscol) != -1) {
                                formatted_colspec.push('<span class="fts-col">' +
                                                       col_popover + '</span>');
                            }
                            else if (indexedcols.indexOf(thiscol) != -1) {
                                formatted_colspec.push('<span class="kdtree-col">' +
                                                       col_popover + '</span>');
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


                // we'll update the lcc_search variables here too so we can
                // build control panes on the fly

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

}


var lcc_search = {

};
