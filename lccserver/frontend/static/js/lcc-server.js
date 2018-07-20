var lccui = {

    // alert types: 'primary', 'secondary', 'success', 'danger', 'warning',
    //              'info', 'light', 'dark'
    alert_box: function(message, alert_type) {

        var alert = '<div class="alert alert-' + alert_type +
            ' alert-dismissible fade show"' +
            ' role="alert">' + message +
            '<button type="button" class="close" data-dismiss="alert" ' +
            'aria-label="Close"><span aria-hidden="true">&times;</span>' +
            '</button></div>';

        $('#alert-box').html(alert);

    },

    // this function generates a spinner
    make_spinner: function (message, target) {

        var spinner = message +
            '<div class="spinner">' +
            '<div class="rect1"></div>' +
            '<div class="rect2"></div>' +
            '<div class="rect3"></div>' +
            '<div class="rect4"></div>' +
            '<div class="rect5"></div>' +
            '</div>';

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
                lccui.alert_box(message, 'danger');
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
                            'search results' +
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
                    console.log(table_lastupdated);

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

            lccui.alert_box(message, 'danger');

        });

    },

    get_lc_collections: function() {




    }

}
