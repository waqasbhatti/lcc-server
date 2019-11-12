/*global $, moment, oboe, setTimeout, clearTimeout, Image, Cookies, localStorage */

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


  // this finds ADS bibcodes in text and linkifies them to an ADS lookup
  // https://en.wikipedia.org/wiki/Bibcode
  // regex adapted from the super awesome https://regex101.com/
  bibcode_linkify: function (text) {

    let match_text;

    // turn &amp; back into &
    if (text !== null && text !== undefined) {
      match_text = text.replace(/&amp;/g,'&');
    }
    else {
      match_text = '';
    }

    const regex = /(\d{4}\S{5}\S{4}[a-zA-Z.]\S{4}[A-Z])+/g;
    let m;
    let bibcodes = [];
    let biblinks = [];
    let new_match_text = match_text;

    while ((m = regex.exec(match_text)) !== null) {
      // This is necessary to avoid infinite loops with zero-width matches
      if (m.index === regex.lastIndex) {
        regex.lastIndex++;
      }

      // The result can be accessed through the `m`-variable.
      m.forEach((match, groupIndex) => {
        bibcodes.push(match);
        biblinks.push(
          `<a target="_blank" rel="noopener noreferrer" href="https://ui.adsabs.harvard.edu/#abs/${match}/abstract">${match}</a>`);
      });
    }

    // remove all the bib codes
    let ind = 0;
    for (ind = 0; ind < bibcodes.length; ind++) {
      new_match_text = new_match_text.replace(
        bibcodes[ind],
        '_bib' + ind + '_'
      );
    }

    // add back the linkified bibcodes
    for (ind = 0; ind < bibcodes.length; ind++) {
      new_match_text = new_match_text.replace(
        '_bib' + ind + '_',
        biblinks[ind]
      );
    }

    return new_match_text;

  },


  // also finds DOIs in text and linkifies them to an dx.doi.org lookup
  // https://en.wikipedia.org/wiki/Digital_object_identifier
  doi_linkify: function (text) {

    const regex = /(doi:\d{2}.[0-9]+\/[.:a-zA-Z0-9_-]+)+/g;
    let m;
    let doicodes = [];
    let doilinks = [];
    let new_text = text;

    while ((m = regex.exec(text)) !== null) {
      // This is necessary to avoid infinite loops with zero-width matches
      if (m.index === regex.lastIndex) {
        regex.lastIndex++;
      }

      // The result can be accessed through the `m`-variable.
      m.forEach((match, groupIndex) => {
        doicodes.push(match);
        doilinks.push(
          `<a target="_blank" rel="noopener noreferrer" href="https://dx.doi.org/${match.replace(/doi:/g,'')}">${match}</a>`);
      });
    }

    // remove all the doi codes
    let ind = 0;
    for (ind = 0; ind < doicodes.length; ind++) {
      new_text = new_text.replace(
        doicodes[ind],
        '_doi' + ind + '_'
      );
    }

    // add back the linkified doicodes
    for (ind = 0; ind < doicodes.length; ind++) {
      new_text = new_text.replace(
        '_doi' + ind + '_',
        doilinks[ind]
      );
    }

    return new_text;

  },


  // this finds ADS bibcodes and DOIs in the given text and linkifies them
  bib_linkify: function (text) {

    let one = lcc_ui.bibcode_linkify(text);
    let two = lcc_ui.doi_linkify(one);

    return two;

  },


  // this interprets the URL from the click on a collection in the SVG
  svgurl_to_collection_id: function (url) {

    let u = new window.URL(url);
    let hash = u.hash;
    let collection_id = hash.replace(/#fp-collection\//g,'');

    if (lcc_ui.collections['db_collection_id'].indexOf(collection_id) != -1) {
      return collection_id;
    }
    else {
      return null;
    }

  },


  // this saves UI prefs on the user home page to the lccserver_prefs cookie
  save_prefs_cookie: function () {

    let always_email = $('#prefs-email-when-done').prop('checked');

    let default_visibility = $('[name="prefs-dataset-visibility"]')
        .filter(':checked').attr('id');

    if (default_visibility === undefined) {
      default_visibility = null;
    }

    let cookie_settings = {
      expires: lcc_ui.prefs_cookie_expires_days
    };
    if (lcc_ui.prefs_cookie_secure) {
      cookie_settings.secure = true;
    }

    Cookies.set('lccserver_prefs',
                {always_email: always_email,
                 default_visibility: default_visibility},
                cookie_settings);

    lcc_ui.alert_box('Your preferences have been saved.','primary');

  },


  // this loads UI preferences from the lccserver_prefs cookie
  // target is one of 'main-page', 'prefs-page' to switch between the controls
  // to set
  load_cookie_prefs: function (target) {

    let prefs = Cookies.getJSON('lccserver_prefs');

    if (prefs !== undefined && target == 'prefs-page') {

      if (prefs.always_email) {
        $('#prefs-email-when-done').prop('checked',true);
      }
      else {
        $('#prefs-email-when-done').prop('checked',false);
      }

      if (prefs.default_visibility !== null || prefs.default_visibility !== undefined) {
        $('#' + prefs.default_visibility).click();
      }

    }

    else if (prefs !== undefined && target == 'main-page') {

      if (prefs.always_email) {
        $('.pref-emailwhendone').prop('checked',true);
      }
      else {
        $('.pref-emailwhendone').prop('checked',false);
      }

      if (prefs.default_visibility !== null || prefs.default_visibility !== undefined) {
        let pref_visibility = prefs.default_visibility.split('-');
        pref_visibility = pref_visibility[pref_visibility.length-1];
        $('.lcc-visibility-select').val(pref_visibility);
      }

    }

  },


  // this asks for an API key and puts it into the target elements
  generate_new_apikey: function (target_key, target_expiry) {

    let geturl = '/api/key';
    $.getJSON(geturl, function(data) {

      let result = data.result;
      let status = data.status;

      if (status == 'ok') {

        $(target_key).val(JSON.stringify(result, null, 2));
        $(target_expiry).html(
          (' expires at ' +
           moment.utc(result.expires).format('Y-M-D HH:mm Z'))
        );

        // save the API key to browser local storage. this should be OK
        // because the key is encrypted and signed and tied to an IP
        // address. (famous last words)
        localStorage.setItem('lccserver_apikey_token', result.apikey);
        localStorage.setItem('lccserver_apikey_expiry', result.expires);

      }

      else {
        lcc_ui.alert_box('Could not fetch a new API key.','warning');
      }

    }).fail(function (xhr) {
      lcc_ui.alert_box('Could not fetch a new API key.','warning');
    });

  },


  // this attempts to load a saved API key from local storage
  load_previous_apikey: function (target_key, target_expiry) {

    let apikey = localStorage.getItem('lccserver_apikey_token');
    let expires = localStorage.getItem('lccserver_apikey_expiry');

    if (apikey !== null && expires !== null) {

      $(target_key).val(
        JSON.stringify({apikey:apikey, expires:expires},null,2)
      );

      // check expiry date
      let expiry_utc = moment.utc(expires);
      if (expiry_utc.isBefore(moment.utc())) {
        $(target_expiry).html('. <span class="text-danger">' +
                              'This API key has expired!' +
                              '</span>');
      }
      else {
        $(target_expiry).html(
          (' expires at ' + expiry_utc.format('Y-M-D HH:mm Z'))
        );
      }

    }

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
      var thistarget = thisbox.attr('data-target');

      if (thistarget == 'conesearch' || thistarget == 'xmatch') {

        thisbox.empty();
        thisbox.append('<option value="' +
                       'dist_arcsec' +
                       '" selected>' +
                       'match distance' +
                       '</option>');

      }
      else if (thistarget == 'ftsquery') {

        thisbox.empty();
        thisbox.append('<option value="' +
                       'relevance' +
                       '" selected>' +
                       'relevance rank' +
                       '</option>');

      }
      else {
        // clear it out
        thisbox.empty();
      }


      var column_ind = 0;
      for (column_ind; column_ind < columns.length; column_ind++) {

        thisbox.append('<option value="' +
                       columns[column_ind] +
                       '">' +
                       columns[column_ind] +
                       '</option>');
      }


      if (thistarget == 'columnsearch') {

        // find either the sdssr column or the objectid column in the
        // columns to select them as a default sort col
        var sdssr_ok = columns.find(function (elem) {
          return elem == 'sdssr';
        });
        var objectid_ok = columns.find(function (elem) {
          return elem == 'objectid';
        });

        if (sdssr_ok) {
          thisbox.children('option[value="sdssr"]')
            .attr('selected',true);
        }
        else if (objectid_ok) {
          thisbox.children('option[value="objectid"]')
            .attr('selected',true);
        }
      }

    });

    // update the FTS query FTS column list
    $('#ftsquery-column-list')
      .html(fts_columns.sort().join(', '));

  },


  // this wires up all the controls
  action_setup: function () {

    ////////////////////////////////
    // FOOTPRINT CONTROL BINDINGS //
    ////////////////////////////////

    // translate the links in the footprint SVG
    $('#footprint > svg').find('a').on('hover focus', function (evt) {

      evt.preventDefault();

      let full_url = window.location.protocol +
          '//' + window.location.host + $(this).attr('xlink:href');
      let collection_id = lcc_ui.svgurl_to_collection_id(full_url);

      if (collection_id !== null) {

        let coll_ind = lcc_ui.collections.db_collection_id.indexOf(
          collection_id
        );

        // find the appropriate row in the collections table
        let collrow = '#lccid-' + collection_id.replace(/_/g,'-');
        $(this).attr('href',collrow);
        $(this).attr('title', lcc_ui.collections.name[coll_ind]);

      }

      else {
        $(this).attr('xlink:href',undefined);
      }

    });


    // also bind the paths inside the anchors so they active the link
    $('#footprint > svg').find('a').find('path').on('click', function (evt) {
      $(this).parent('a').click();
    });


    // bind the collection-search-init controls
    $('#collection-container').on(
      'click', 'a.collection-search-init', function(evt) {

        evt.preventDefault();

        let target = $(this).attr('data-target');
        let collection = $(this).attr('data-collection');

        // select the collection in the appropriate select box
        $('#' + target + '-collection-select').val(collection);

        // click on the earch tab
        $('#' + target + '-tab').click();

        // focus the main search controls

        if (target == 'conesearch') {
          $('#conesearch-query').focus();
        }
        else if (target == 'ftsquery') {
          $('#ftsquery-query').focus();

        }
        else if (target == 'columnsearch') {
          $('#columnsearch-filtercolumn-select').focus();

        }
        else if (target == 'xmatch') {
          $('#xmatch-query').focus();

        }

      });

    // bind the 100random-init control
    $('#collection-container').on(
      'click', '.collection-100random-init', function(evt) {

        evt.preventDefault();
        let collection = $(this).attr('data-collection');

        let searchparams = {
          collections: [collection],
          columns: ['sdssr', 'ndet', 'dered_jmag_kmag'],
          filters: '(sdssr lt 16.0) and (ndet gt 5000)',
          visibility: 'unlisted',
          sortspec: JSON.stringify([['sdssr','asc']]),
          samplespec: 100,
          limitspec: null,
        };

        lcc_search.do_columnsearch(searchparams);

      });


    // bind the centercone-init control
    $('#collection-container').on(
      'click', '.collection-centercone-init', function(evt) {

        evt.preventDefault();
        let collection = $(this).attr('data-collection');
        let collind = lcc_ui.collections.db_collection_id.indexOf(collection);

        let center_ra = ((lcc_ui.collections.minra[collind] +
                          lcc_ui.collections.maxra[collind])/2.0);
        let center_decl = ((lcc_ui.collections.mindecl[collind] +
                            lcc_ui.collections.maxdecl[collind])/2.0);

        let searchparams = {
          coords: `${center_ra} ${center_decl} 60.0`,
          collections: [collection],
          columns: ['sdssr', 'ndet', 'dered_jmag_kmag'],
          filters: '(sdssr lt 16.0) and (ndet gt 5000)',
          visibility: 'unlisted',
          sortspec: JSON.stringify([['dist_arcsec','asc']]),
          samplespec: 100,
          limitspec: null,
        };

        lcc_search.do_conesearch(searchparams);

      });


    // bind the stetsonvar-init control
    $('#collection-container').on(
      'click', '.collection-stetsonvar-init', function(evt) {

        evt.preventDefault();
        let collection = $(this).attr('data-collection');

        // get this collection's magcols
        let collind = lcc_ui.collections.db_collection_id.indexOf(collection);
        let magcol = lcc_ui.collections.lcmagcols[collind].split(',')[0];

        let searchparams = {
          collections: [collection],
          columns: [ magcol+'_stetsonj', 'sdssr', 'ndet', 'dered_jmag_kmag'],
          filters: '(sdssr lt 16.0) and (ndet gt 5000)',
          visibility: 'unlisted',
          sortspec: JSON.stringify([[magcol + '_stetsonj','desc']]),
          samplespec: null,
          limitspec: 100,
        };

        lcc_search.do_columnsearch(searchparams);

      });


    // bind the simbadok-init control
    $('#collection-container').on(
      'click', '.collection-simbadok-init', function(evt) {

        evt.preventDefault();
        let collection = $(this).attr('data-collection');

        let searchparams = {
          collections: [collection],
          columns: ['simbad_best_mainid',
                    'simbad_best_allids',
                    'simbad_best_objtype',
                    'sdssr'],
          filters: '(sdssr lt 16.0) and (ndet gt 5000) and ' +
            '(simbad_best_mainid notnull)',
          visibility: 'unlisted',
          sortspec: JSON.stringify([['sdssr','asc']]),
          samplespec: null,
          limitspec: 100,
        };

        lcc_search.do_columnsearch(searchparams);

      });

    // bind the gaiadwarfs-init control
    $('#collection-container').on(
      'click', '.collection-gaiadwarfs-init', function (evt) {

        evt.preventDefault();
        let collection = $(this).attr('data-collection');

        let searchparams = {
          collections: [collection],
          columns: ['propermotion',
                    'gaia_parallax',
                    'gaia_parallax_err',
                    'gaia_mag',
                    'gaia_absmag',
                    'sdssr'],
          filters: '(sdssr lt 16.0) and (ndet gt 5000) and ' +
            '(gaia_absmag notnull) and (gaia_absmag gt 3.0)',
          visibility: 'unlisted',
          sortspec: JSON.stringify([['gaia_absmag','desc']]),
          samplespec: null,
          limitspec: 100,
        };

        lcc_search.do_columnsearch(searchparams);

      });

    // bind the gaiadwarfs-init control
    $('#collection-container').on(
      'click', '.collection-fastmovers-init', function (evt) {

        evt.preventDefault();
        let collection = $(this).attr('data-collection');

        let searchparams = {
          collections: [collection],
          columns: ['gaia_parallax',
                    'gaia_parallax_err',
                    'propermotion',
                    'gaia_mag',
                    'gaia_absmag',
                    'sdssr'],
          filters: '(sdssr lt 16.0) and (ndet gt 5000) ' +
            'and (gaia_parallax notnull)',
          visibility: 'unlisted',
          sortspec: JSON.stringify([['gaia_parallax','desc']]),
          samplespec: null,
          limitspec: 100,
        };

        lcc_search.do_columnsearch(searchparams);

      });


    /////////////////////////
    // USER PREFS BINDINGS //
    /////////////////////////

    // bind the cookie setters
    $('#prefs-save').on('click', function(evt) {
      lcc_ui.save_prefs_cookie();
    });

    // delete the API key on session end
    $('#user-logout-form').on('submit', function(evt) {
      localStorage.clear();
    });

    // bind the apikey generate button
    $('#prefs-generate-apikey').on('click', function(evt) {
      lcc_ui.generate_new_apikey('#api-key','#apikey-expiry');
    });

    // handle the site settings update form
    $('#prefs-update-details-form').on('submit', function (evt) {

      evt.preventDefault();

      // find the updated values
      let updated_fullname =
          $('#userhome-fullname').val();

      if (updated_fullname.trim().length == 0) {
        updated_fullname = null;
      }

      var posturl = '/users/home';
      var _xsrf = $('#prefs-update-details-form > input[type="hidden"]').val();
      var postparams = {
        _xsrf:_xsrf,
        updated_fullname: updated_fullname,
      };

      $.post(posturl, postparams, function (data) {

        var status = data.status;
        var result = data.result;
        var message = data.message;

        // if something broke, alert the user
        if (status != 'ok' || result === null || result.length == 0) {
          lcc_ui.alert_box(message, 'danger');
        }

        // if the update succeeded, inform the user and update the
        // controls to reflect the new state
        else if (status == 'ok') {

          // update the controls
          $('#userhome-fullname').val(
            result.full_name
          );
          lcc_ui.alert_box(message, 'info');

        }

      }, 'json').fail(function (xhr) {

        var message = 'Could not update user information, ' +
            'something went wrong with the LCC-Server backend.';

        if (xhr.status == 500) {
          message = 'Something went wrong with the LCC-Server backend ' +
            ' while trying to update user information.';
        }
        else if (xhr.status == 400) {
          message = 'Invalid input provided in the user ' +
            ' update form. Please check and try again.';
        }

        lcc_ui.alert_box(message, 'danger');

      });

    });


    /////////////////////////////////
    // SEARCH FORM SUBMIT BINDINGS //
    /////////////////////////////////

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

      // check the size of the filter bucket
      var filterbucket_size = $('#columnsearch-filterbucket').length;
      if (filterbucket_size > 0) {
        lcc_ui.debounce(lcc_search.do_columnsearch(), 250);
      }

    });

    // bind the form submit for the cone search
    $('#xmatch-form').on('submit', function (event) {

      event.preventDefault();
      lcc_ui.debounce(lcc_search.do_xmatch(), 250);

    });


    /////////////////////////////////
    // COLLECTION CONTROL BINDINGS //
    /////////////////////////////////

    // bind link to collections tab
    $('#alert-box').on('click','.collection-link', function (e) {

      e.preventDefault();
      $('#collections-tab').click();

    });

    // bind link to collections tab
    $('.tab-pane').on('click','.collection-link', function (e) {

      e.preventDefault();
      $('#collections-tab').click();

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
      $('.lcc-filterbucket').empty();

    });


    /////////////////////////////////
    // RESULT ROW CONTROL BINDINGS //
    /////////////////////////////////

    // bind the lcc-result-samplecheck checkbox so a click will toggle the
    // disabled state of the associated lcc-result-samplerows text box
    $('.lcc-result-samplecheck').on('click', function (evt) {

      var checked = $(this).prop('checked');
      var searchtype = $(this).attr('data-searchtype');
      var associated_inputbox = $('#' + searchtype + '-samplerows');

      if (checked) {
        associated_inputbox.attr('disabled',false);
      }
      else {
        associated_inputbox.attr('disabled',true);
      }

    });


    // bind the lcc-result-limitcheck checkbox so a click will toggle the
    // disabled state of the associated lcc-result-limitrows text box
    $('.lcc-result-limitcheck').on('click', function (evt) {

      var checked = $(this).prop('checked');
      var searchtype = $(this).attr('data-searchtype');
      var associated_inputbox = $('#' + searchtype + '-limitrows');

      if (checked) {
        associated_inputbox.attr('disabled',false);
      }
      else {
        associated_inputbox.attr('disabled',true);
      }

    });


    //////////////////////////////////
    // SEARCH FILTERBUCKET BINDINGS //
    //////////////////////////////////

    // bind the lcc-filtertarget so an Enter clicks the add-filter button
    $('.lcc-filtertarget').on('keyup', function (evt) {

      evt.preventDefault();
      evt.stopImmediatePropagation();

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
      else if (filter_opstr == 'isnull') {
        filter_op = 'is null';
      }
      else if (filter_opstr == 'notnull') {
        filter_op = 'not null';
      }

      // look up the dtype of the column
      // this is done in two steps
      // 1. look up the currently active

      var filter_dtype = lcc_search.coldefs[filter_col]['dtype']
          .replace('<','');

      var filter_val = filter_val_elem.val();
      var filter_check = false;

      // check if the filter val and operator matches the expected dtype

      // the isnull, notnull operators take no operand
      if ((filter_opstr == 'isnull') || (filter_opstr == 'notnull')) {

        if (filter_val.trim().length == 0) {
          filter_check = true;
        }
        else {
          filter_check = false;
        }

      }

      // check the other operators
      else {

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
            target.replace(/"/g,'').replace(/'/g,'').trim() +
            '" data-column="' +
            filter_col.replace(/"/g,'').replace(/'/g,'').trim() +
            '" data-operator="' +
            filter_opstr.replace(/"/g,'').replace(/'/g,'').trim() +
            '" data-filterval="' +
            filter_val.replace(/"/g,'').replace(/'/g,'').trim() +
            '" data-dtype="' +
            filter_dtype.replace(/"/g,'').replace(/'/g,'').trim() +
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


    //////////////////////////////////
    // SEARCH EXAMPLE LINK BINDINGS //
    //////////////////////////////////

    // handle the columnsearch example 1 link
    $('#columnsearch-example-1').on('click', function (evt) {

      evt.preventDefault();

      // clean the filterbucket out
      $('#columnsearch-filterbucket').empty();

      $('#columnsearch-filtercolumn-select').val('propermotion');
      $('#columnsearch-filtercondition-select').val('gt');
      $('#columnsearch-filtertarget').val('200.0');
      $('#columnsearch-filter-add').click();

      $('#columnsearch-filtercolumn-select').val('sdssr');
      $('#columnsearch-filtercondition-select').val('lt');
      $('#columnsearch-filtertarget').val('11.0');
      $('#columnsearch-filter-add').click();

    });

    // handle the columnsearch example 2 link
    $('#columnsearch-example-2').on('click', function (evt) {

      evt.preventDefault();

      // clean the filterbucket out
      $('#columnsearch-filterbucket').empty();

      $('#columnsearch-filtercolumn-select').val('color_jmag_kmag');
      $('#columnsearch-filtercondition-select').val('gt');
      $('#columnsearch-filtertarget').val('2.0');
      $('#columnsearch-filter-add').click();

      $('#columnsearch-filtercolumn-select').val('aep_000_stetsonj');
      $('#columnsearch-filtercondition-select').val('gt');
      $('#columnsearch-filtertarget').val('1.0');
      $('#columnsearch-filter-add').click();

    });

    // handle the columnsearch example 3 link
    $('#columnsearch-example-3').on('click', function (evt) {

      evt.preventDefault();

      // clean the filterbucket out
      $('#columnsearch-filterbucket').empty();

      $('#columnsearch-filtercolumn-select').val('color_classes');
      $('#columnsearch-filtercondition-select').val('ct');
      $('#columnsearch-filtertarget').val('RR');
      $('#columnsearch-filter-add').click();

      $('#columnsearch-filtercolumn-select').val('dered_sdssg_sdssr');
      $('#columnsearch-filtercondition-select').val('lt');
      $('#columnsearch-filtertarget').val('0.3');
      $('#columnsearch-filter-add').click();

    });


    // handle the example coordlist link
    $('#xmatch-example').on('click', function (evt) {

      evt.preventDefault();

      $('#xmatch-query').val(lcc_search.coordlist_placeholder)
        .focus()
        .blur();

    });


    //////////////////////////////
    // DATASET CONTROL BINDINGS //
    //////////////////////////////

    // bind the lcc-datasets-open link
    $('.lcc-datasets-tabopen').on('click', function (evt) {
      evt.preventDefault();
      $('#datasets-tab').click();
    });

    // dataset page handling
    $('.dataset-pagination-prev').on('click', function (evt) {

      lcc_datasets.get_dataset_page(
        lcc_datasets.setid,
        lcc_datasets.currpage - 1
      );

    });
    // dataset page handling
    $('.dataset-pagination-next').on('click', function (evt) {

      lcc_datasets.get_dataset_page(
        lcc_datasets.setid,
        lcc_datasets.currpage + 1
      );

    });
    // dataset page handling
    $('.dataset-pagination-first').on('click', function (evt) {

      lcc_datasets.get_dataset_page(
        lcc_datasets.setid,
        1
      );

    });
    // dataset page handling
    $('.dataset-pagination-last').on('click', function (evt) {

      lcc_datasets.get_dataset_page(
        lcc_datasets.setid,
        lcc_datasets.npages
      );

    });

    // handle the dataset show all button
    $('#dataset-show-all').on('click', function(evt) {

      lcc_ui.get_recent_datasets(1000);
      $('#dataset-result-header')
        .html('All available datasets');

    });

    // handle the dataset search page
    $('#dataset-search-form').on('submit', function (evt) {

      evt.preventDefault();

      var posturl = '/api/datasets';
      var _xsrf = $('#dataset-search-form > input[type="hidden"]').val();
      var postparams = {_xsrf:_xsrf,
                        datasetsearch:$('#dataset-searchbox').val()};
      $.post(posturl, postparams, function (data) {

        var status = data.status;
        var result = data.result;
        var message = data.message;

        // if something broke, alert the user
        if (status != 'ok' || result === null || result.length == 0) {
          lcc_ui.alert_box(message, 'danger');
        }

        if (result === null) {
          $('#dataset-result-header')
            .html('No matching datasets found');
          $('#lcc-datasets-tablerows').empty();
        }

        else {

          // set up the dataset result header
          var ndatasets = result.length;
          if (ndatasets == 0) {
            $('#dataset-result-header')
              .html('No matching datasets found');
          }
          else if (ndatasets == 1) {
            $('#dataset-result-header')
              .html('1 matching dataset found');
          }
          else {
            $('#dataset-result-header')
              .html(ndatasets + ' matching datasets found');
          }

          var rowind = 0;
          $('#lcc-datasets-tablerows').empty();

          for (rowind; rowind < result.length; rowind++) {

            // setid and queried collections
            var setid = result[rowind]['setid'];

            // number of objects
            var nobjects = result[rowind]['nobjects'];

            // query type and params
            var set_name = result[rowind]['name'];
            var set_desc = lcc_ui.bib_linkify(result[rowind]['description']);
            var set_citation = lcc_ui.bib_linkify(result[rowind]['citation']);
            var set_owned = result[rowind]['owned'];
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
                setid + '</a>' +
                '</td>';
            if (set_owned) {
              table_setid = '<td>' +
                '<a rel="nofollow" href="/set/' +
                setid + '">' +
                setid + '</a><br>' +
                '<span class="text-success">You own this dataset</span><br>' +
                'Dataset is ' + result[rowind]['dataset_visibility'] +
                '</td>';
            }


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
                '<strong>name:</strong> ' + set_name + '<br>' +
                '<strong>description:</strong> ' + set_desc + '<br>' +
                '<strong>citation:</strong> ' + set_citation + '<br><br>' +
                '<details><summary><strong>query:</strong> <code>' + query_type + '</code>' +
                '</summary><pre>' +
                JSON.stringify(JSON.parse(query_params),null,2) +
                '</pre></details>' +
                '</td>';
            table_query = table_query
              .replace(/sqlite_/g,'')
              .replace(/postgres_/g,'');

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

            $('#lcc-datasets-tablerows').append(setrow);

          }

        }

        // at the end, activate the tooltips
        $('[data-toggle="tooltip"]').tooltip();

      }, 'json').fail(function (xhr) {

        var message = 'could not get list of recent ' +
            'datasets from the LCC server backend';

        if (xhr.status == 500) {
          message = 'Something went wrong with the LCC-Server backend ' +
            ' while trying to fetch a list of recent datasets.';
        }

        lcc_ui.alert_box(message, 'danger');

      });

    });

    // this handles editing dataset names
    $('.accordion').on('click', '#dataset-name-submit', function (evt) {

      lcc_datasets.edit_dataset_name(
        lcc_datasets.setid,
        $('#dataset-name-inputbox').val()
      );

    });
    // this handles editing dataset names
    $('.accordion').on('keyup', '#dataset-name-inputbox', function (evt) {

      if (evt.key == 'Enter') {
        $('#dataset-name-submit').click();
      }

    });

    // this handles editing dataset descriptions
    $('.accordion').on('click', '#dataset-desc-submit', function (evt) {

      lcc_datasets.edit_dataset_description(
        lcc_datasets.setid,
        $('#dataset-desc-inputbox').val()
      );

    });
    // this handles editing dataset descriptions
    $('.accordion').on('keyup', '#dataset-desc-inputbox', function (evt) {

      if (evt.key == 'Enter') {
        $('#dataset-desc-submit').click();
      }

    });

    // this handles editing dataset citations
    $('.accordion').on('click', '#dataset-citation-submit', function (evt) {

      lcc_datasets.edit_dataset_citation(
        lcc_datasets.setid,
        $('#dataset-citation-inputbox').val()
      );

    });
    // this handles editing dataset descriptions
    $('.accordion').on('keyup', '#dataset-citation-inputbox', function (evt) {

      if (evt.key == 'Enter') {
        $('#dataset-citation-submit').click();
      }

    });

    // this handles changing dataset visibility
    $('.accordion').on('click', '#dataset-visibility-submit', function (evt) {

      lcc_datasets.change_dataset_visibility(
        lcc_datasets.setid,
        $('#dataset-visibility-select').val()
      );

    });

    // this handles changing dataset owners
    $('.accordion').on('click', '#owner-label-submit', function (evt) {

      lcc_datasets.change_dataset_owner(
        lcc_datasets.setid,
        $('#owner-label-inputbox').val()
      );

    });


    ////////////////////////////////////////
    // OBJECTINFO POPUP AND PAGE BINDINGS //
    ////////////////////////////////////////

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
      var lcmagcols = button.attr('data-lcmagcols');
      var lcfname = button.attr('data-lcfname');

      modal.find('#modal-objectid').html(objectid);
      modal.find('#modal-collectionid').html(collection);
      modal.find('#modal-permalink').html(
        '<a rel="nofollow noopener noreferrer" target="_blank" ' +
          'href="/obj/' + collection +
          '/' + objectid + '">[object page]</a>'
      );

      if (lcfname.indexOf('unavailable') != -1 ||
          lcfname.indexOf('null') != -1) {
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
      lcc_objectinfo.get_object_info(collection,
                                     lcmagcols,
                                     objectid,
                                     '.modal-body');

    });

    // bind the neighbor links in the modals

    // FIXME: should disable the prev/next object links until we get back to
    // the original object.

    // FIXME: show a link in the bottom bar to go back to the original
    // object. Once there, we'll re-enable the prev/next links.
    $('#objectinfo-modal').on('click','.objectinfo-nbrlink', function (e) {

      e.preventDefault();

      // get the info on the neighbor
      var objectid = $(this).attr('data-objectid');
      var collection = $(this).attr('data-collection');
      var lcmagcols = $(this).attr('data-lcmagcols');

      // fire the objectinfo function to get this neighbor's information
      lcc_objectinfo.get_object_info(collection,
                                     lcmagcols,
                                     objectid,
                                     '.modal-body');

      // get the pointer to neighbor's light curve
      var modal = $('#objectinfo-modal');
      var lcfbasename = objectid + '-csvlc.gz';
      var lcfurl = '/l/' +
          collection.replace(/_/g,'-') +
          '/' + lcfbasename;
      modal.find('#modal-downloadlc')
        .attr('href',lcfurl)
        .attr('download',lcfbasename);

    });


    // this handles the hover per objectid row to highlight the object in
    // the finder chart
    $('#objectinfo-modal').on('mouseover','.gaia-objectlist-row', function (e) {

      e.preventDefault();

      var canvas = document.getElementById('finderchart');
      var canvaswidth = canvas.width;
      var canvasheight = canvas.height;
      var ctx = canvas.getContext('2d');

      // FIXME: check if astropy.wcs returns y, x and we've been doing
      // this wrong all this time
      var thisx = $(this).attr('data-xpos');
      var thisy = $(this).attr('data-ypos');

      var cnvx = thisx * canvaswidth/300.0;

      // y is from the top of the image for canvas
      // FITS coords are from the bottom of the image
      var cnvy = (300.0 - thisy) * canvasheight/300.0;

      // save the damaged part of the image
      lcc_objectinfo.pixeltracker = ctx.getImageData(cnvx-20,cnvy-20,40,40);

      ctx.strokeStyle = 'green';
      ctx.lineWidth = 3.0;
      ctx.strokeRect(cnvx-7.5,cnvy-7.5,12.5,12.5);

    });


    // this handles the repair to the canvas after the user mouses out of
    // the row
    $('#objectinfo-modal').on('mouseout','.gaia-objectlist-row', function (e) {

      e.preventDefault();

      var canvas = document.getElementById('finderchart');
      var canvaswidth = canvas.width;
      var canvasheight = canvas.height;
      var ctx = canvas.getContext('2d');

      var thisx = $(this).attr('data-xpos');
      var thisy = $(this).attr('data-ypos');

      var cnvx = thisx * canvaswidth/300.0;

      // y is from the top of the image for canvas
      // FITS coords are from the bottom of the image
      var cnvy = (300.0 - thisy) * canvasheight/300.0;

      // restore the imagedata if we have any
      if (lcc_objectinfo.pixeltracker != null) {
        ctx.putImageData(lcc_objectinfo.pixeltracker,
                         cnvx-20, cnvy-20);
      }

    });

    // bind the SIMBAD update button
    $('#objectinfo-modal').on('submit', '#simbad-lookup-form', function (evt) {

      evt.preventDefault();
      lcc_objectinfo.simbad_check($(this).attr('data-objectid'),
                                  $(this).attr('data-collection'));

    });

    // bind the SIMBAD update button
    $('#objectinfo-container').on('submit', '#simbad-lookup-form', function (evt) {

      evt.preventDefault();
      lcc_objectinfo.simbad_check($(this).attr('data-objectid'),
                                  $(this).attr('data-collection'));

    });


    // this handles the hover per objectid row to highlight the object in
    // the finder chart
    $('#objectinfo-container').on('mouseover','.gaia-objectlist-row', function (e) {

      e.preventDefault();

      var canvas = document.getElementById('finderchart');
      var canvaswidth = canvas.width;
      var canvasheight = canvas.height;
      var ctx = canvas.getContext('2d');

      // FIXME: check if astropy.wcs returns y, x and we've been doing
      // this wrong all this time
      var thisx = $(this).attr('data-xpos');
      var thisy = $(this).attr('data-ypos');

      var cnvx = thisx * canvaswidth/300.0;

      // y is from the top of the image for canvas
      // FITS coords are from the bottom of the image
      var cnvy = (300.0 - thisy) * canvasheight/300.0;

      // save the damaged part of the image
      lcc_objectinfo.pixeltracker = ctx.getImageData(cnvx-20,cnvy-20,40,40);

      ctx.strokeStyle = 'green';
      ctx.lineWidth = 3.0;
      ctx.strokeRect(cnvx-7.5,cnvy-7.5,12.5,12.5);

    });

    // this handles the repair to the canvas after the user mouses out of
    // the row
    $('#objectinfo-container').on('mouseout','.gaia-objectlist-row', function (e) {

      e.preventDefault();

      var canvas = document.getElementById('finderchart');
      var canvaswidth = canvas.width;
      var canvasheight = canvas.height;
      var ctx = canvas.getContext('2d');

      var thisx = $(this).attr('data-xpos');
      var thisy = $(this).attr('data-ypos');

      var cnvx = thisx * canvaswidth/300.0;

      // y is from the top of the image for canvas
      // FITS coords are from the bottom of the image
      var cnvy = (300.0 - thisy) * canvasheight/300.0;

      // restore the imagedata if we have any
      if (lcc_objectinfo.pixeltracker != null) {
        ctx.putImageData(lcc_objectinfo.pixeltracker,
                         cnvx-20, cnvy-20);
      }

    });

    // bind the neighbor links in the permalink pages
    $('#objectinfo-container').on('click','.objectinfo-nbrlink', function (e) {

      e.preventDefault();

      // get the info on the neighbor
      var objectid = $(this).attr('data-objectid');
      var collection = $(this).attr('data-collection');
      var lcmagcols = $(this).attr('data-lcmagcols');

      // fire the objectinfo function to get this neighbor's information
      lcc_objectinfo.get_object_info(collection,
                                     lcmagcols,
                                     objectid,
                                     '#objectinfo-container',
                                     true);

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

      var thisfilter = '';
      if (oper == 'isnull' || oper == 'notnull') {
        thisfilter = '(' + col + ' ' + oper + ')';
      }
      else {
        thisfilter = '(' + col + ' ' + oper + ' ' + fval + ')';
      }

      // check if this card has a chainer operator
      var chain_op = $(this)
          .children('div')
          .children('div.mr-auto')
          .children('select').val();

      if (chain_op != undefined && i > 0) {
        thisfilter = chain_op + ' ' + thisfilter;
      }

      filters.push(thisfilter);
      filter_cols.push(col);

    });

    return [filters.join(' '), new Set(filter_cols)];

  },


  get_recent_datasets: function(nrecent, highlight, useronly) {

    var geturl = '/api/datasets';
    var getparams = {
      nsets: nrecent,
    };

    if (useronly !== undefined) {
      getparams.useronly = useronly;
    }

    // clear out the recent queries box to keep things fresh
    $('#lcc-datasets-tablerows').empty();

    $.getJSON(geturl, getparams, function (data) {

      var status = data.status;
      var result = data.result;
      var message = data.message;

      // if something broke, alert the user
      if (status != 'ok') {
        lcc_ui.alert_box(message, 'warning');
      }

      // otherwise, fill in the datasets table
      else {

        var rowind = 0;

        for (rowind; rowind < result.length; rowind++) {

          // setid and queried collections
          var setid = result[rowind]['setid'];

          // number of objects
          var nobjects = result[rowind]['nobjects'];

          // query type and params
          var set_name = result[rowind]['name'];
          var set_desc = lcc_ui.bib_linkify(result[rowind]['description']);
          var set_citation = lcc_ui.bib_linkify(result[rowind]['citation']);
          var set_owned = result[rowind]['owned'];
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
              setid + '</a>' +
              '</td>';
          if (set_owned) {
            table_setid = '<td>' +
              '<a rel="nofollow" href="/set/' +
              setid + '">' +
              setid + '</a><br>' +
              '<span class="text-success">You own this dataset</span><br>' +
              'Dataset is ' + result[rowind]['dataset_visibility'] +
              '</td>';
          }

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
              '<strong>name:</strong> ' + set_name + '<br>' +
              '<strong>description:</strong> ' + set_desc + '<br>' +
              '<strong>citation:</strong> ' + set_citation + '<br><br>' +
              '<details><summary><strong>query:</strong> <code>' + query_type + '</code>' +
              '</summary><pre>' +
              JSON.stringify(JSON.parse(query_params),null,2) +
              '</pre></details>' +
              '</td>';
          table_query = table_query
            .replace(/sqlite_/g,'')
            .replace(/postgres_/g,'');

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

          if (highlight !== undefined &&
              highlight !== null &&
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
        message = 'Something went wrong with the LCC-Server backend ' +
          ' while trying to fetch a list of recent datasets.';
      }

      lcc_ui.alert_box(message, 'danger');

    });

  },


  // this renders a single collection
  render_collection: function (collections, ind) {

    // get the indexed columns for the collection
    let indexedcols =
        collections.indexedcols[ind].split(',').sort();

    // get the FTS columns for this collection
    let ftscols =
        collections.ftsindexedcols[ind].split(',').sort();

    // get the collection DB ID and name
    let db_collection_id = collections.db_collection_id[ind];
    let collection_name = collections.name[ind];

    // get the project
    let project = collections.project[ind];

    // get the description
    let description = collections.description[ind];

    // get the description
    let citation = collections.citation[ind];

    // get the description
    let last_updated = moment.utc(
      collections.last_updated[ind] + 'Z'
    ).format('Y-M-D HH:mm Z');


    // get the number of objects
    let nobjects = collections.nobjects[ind];

    // get the center RA and DEC
    let center_ra = ((collections.minra[ind] + collections.maxra[ind])/2.0).toFixed(3);
    let center_decl = ((collections.mindecl[ind] + collections.maxdecl[ind])/2.0).toFixed(3);

    // format the columns
    let formatted_ftscol_colspec = [];
    let formatted_indexed_colspec = [];

    for (let thiscol of indexedcols) {

      let thiscol_title =
          collections.columnjson[ind][thiscol]['title'];
      let thiscol_desc =
          collections.columnjson[ind][thiscol][
            'description'
          ];

      if (thiscol_title != null && thiscol_desc != null) {

        let col_popover = '<span class="pop-span" ' +
            'data-toggle="popover" ' +
            'data-placement="top" ' +
            'data-title="' + thiscol_title + '" ' +
            'data-content="' + thiscol_desc + '" ' +
            'data-html="true">' + thiscol + '</span>';
        formatted_indexed_colspec.push(
          '<span class="indexed-col">' +
            col_popover + '</span>'
        );

      }

    }

    for (let thiscol of ftscols) {

      let thiscol_title =
          collections.columnjson[ind][thiscol]['title'];
      let thiscol_desc =
          collections.columnjson[ind][thiscol][
            'description'
          ];

      if (thiscol_title != null && thiscol_desc != null) {

        let col_popover = '<span class="pop-span" ' +
            'data-toggle="popover" ' +
            'data-placement="top" ' +
            'data-title="' + thiscol_title + '" ' +
            'data-content="' + thiscol_desc + '" ' +
            'data-html="true">' + thiscol + '</span>';

        formatted_ftscol_colspec.push(
          '<span class="fts-col">' +
            col_popover + '</span>'
        );

      }

    }

    let formatted_ftscol_list = formatted_ftscol_colspec.join(', ');
    let formatted_indexedcol_list = formatted_indexed_colspec.join(', ');

    //
    // now we have everything. fill in the column row template
    //
    let collection_row = `
<h4><a id="lccid-${db_collection_id.replace(/_/g,'-')}">${collection_name}
  <br>(<code>${db_collection_id}</code>)</a></h4>

<details class="mt-2" open><summary class="h5-summary">About this collection</summary>

  ${lcc_ui.bib_linkify(description)}

  <table class="table table-sm collection-infotable mt-2 mx-auto">
    <tr>
      <th scope="row">Objects</th>
      <td>${nobjects}</td>
    </tr>
    <tr>
      <th scope="row">Center [J2000 deg]</th>
      <td>(${center_ra}, ${center_decl})</td>
    </tr>
    <tr>
      <th scope="row">Updated [UTC]</th>
      <td>${last_updated}</td>
    </tr>
    <tr>
      <th scope="row">Project</th>
      <td>${project}</td>
    </tr>
    <tr>
      <th scope="row">Citation</th>
      <td>${lcc_ui.bib_linkify(citation)}</td>
    </tr>
  </table>

</details>


<details class="mt-2"><summary class="h5-summary">Available database columns</summary>

  <div class="ml-3 collection-column-list">
    <details>
      <summary>List of full-text-search indexed columns</summary>
      ${formatted_ftscol_list}
    </details>
  </div>

  <div class="mt-2 ml-3 collection-column-list">
    <details>
      <summary>List of other indexed columns</summary>
      ${formatted_indexedcol_list}
    </details>
  </div>

</details>


<details class="mt-2"><summary class="h5-summary">Search this collection</summary>

  <ul class="list-unstyled">

    <li>
      <a href="#" rel="nofollow" class="collection-search-init"
         data-collection="${db_collection_id}" data-target="conesearch">
        Find objects by their coordinates
      </a>
    </li>

    <li>
      <a href="#" rel="nofollow" class="collection-search-init"
         data-collection="${db_collection_id}" data-target="ftsquery">
        Find objects by name or description
      </a>
    </li>

    <li>
      <a href="#" rel="nofollow" class="collection-search-init"
         data-collection="${db_collection_id}" data-target="columnsearch">
        Find objects using database column filters
      </a>
    </li>

    <li>
      <a href="#" rel="nofollow" class="collection-search-init"
         data-collection="${db_collection_id}" data-target="xmatch">
        Cross-match to objects in an uploaded list
      </a>
    </li>

  </ul>

</details>

<details class="mt-2 mb-4">
  <summary class="h5-summary">Explore this collection</summary>

  <ul class="list-unstyled">

    <li>
      <a href="#" rel="nofollow" class="collection-100random-init"
         data-collection="${db_collection_id}">
        100 random objects from this collection
      </a>
    </li>

    <li>
      <a href="#" rel="nofollow" class="collection-simbadok-init"
         data-collection="${db_collection_id}">
        100 random objects with SIMBAD counterparts
      </a>
    </li>

    <li>
      <a href="#" rel="nofollow" class="collection-stetsonvar-init"
         data-collection="${db_collection_id}">
        Top 100 objects sorted by decreasing Stetson <em>J<sub>var</sub></em>
      </a>
    </li>

    <li>
      <a href="#" rel="nofollow" class="collection-fastmovers-init"
         data-collection="${db_collection_id}">
        Top 100 objects sorted by decreasing GAIA parallax
      </a>
    </li>

    <li>
      <a href="#" rel="nofollow" class="collection-gaiadwarfs-init"
         data-collection="${db_collection_id}">
        Top 100 objects sorted by decreasing GAIA M<sub><em>G</em></sub>
      </a>
    </li>

  </ul>
</details>
`;

    $('#collection-container').append(collection_row);


  },


  // this renders the LC collections page
  render_collections_tab: function () {

    let collind;
    let collections = lcc_ui.collections;

    for (collind = 0;
         collind < collections.collection_id.length;
         collind++) {
      lcc_ui.render_collection(collections, collind);
    }

    // add the count of total objects below the footprint SVG
    let total_objects = collections.nobjects.reduce(function (acc, curr) {
      return acc + curr;
    });
    let total_collections = collections.nobjects.length;
    total_objects = total_objects.toLocaleString();

    let coll_plural = '';
    if (total_collections > 1) {
      coll_plural = 's';
    }

    $('#footprint').append(
      `<h3>${total_objects} light curves in ${total_collections} collection${coll_plural}</h3>`
    );

  },


  // this gets the latest LC collections and updates the controls
  // calls render_lc_collections to render the collections tab
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

          // update the collection select boxes
          collection_selectboxes.each(function () {
            var thisbox = $(this);
            thisbox.append('<option value="' +
                           db_collid +
                           '">' +
                           collname +
                           '</option>');
          });


          // get the column list for this collection
          var columns =
              collections.columnlist[coll_idx].split(',').sort();

          var colind = 0;

          // add each column for this collection to the output
          for (colind; colind < columns.length; colind++) {

            var thiscol = columns[colind];

            if (! (thiscol in lcc_search.coldefs) ) {
              lcc_search.coldefs[thiscol] =
                collections.columnjson[coll_idx][thiscol];
            }

          }

        }

        // update the column select boxes
        lcc_ui.update_column_associated_controls(
          available_columns,
          indexed_columns,
          fts_columns
        );

      }

    }).fail(function (xhr) {
      var message = 'could not get list of recent ' +
          'LC collections from the LCC server backend';

      if (xhr.status == 500) {
        message = 'Something went wrong with the LCC-Server backend ' +
          'while trying to fetch a list of all LC collections.';
      }

      lcc_ui.alert_box(message, 'danger');


    }).done(function (data) {

      // this calls the render_collections_tab function
      lcc_ui.render_collections_tab();

      // at the end, activate the tooltips and popovers
      $('[data-toggle="tooltip"]').tooltip();
      $('[data-toggle="popover"]').popover();

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
  decimal_regex: /^([a-zA-Z0-9_+\-\[\].]+)\s(\d{1,3}\.?\d*)\s([+-]?\d{1,2}\.?\d*)$/,

  sexagesimal_regex: /^([a-zA-Z0-9_+\-\[\].]+)\s(\d{1,2}[ :]\d{2}[ :]\d{2}\.?\d*)\s([+-]?\d{1,2}[: ]\d{2}[: ]\d{2}\.?\d*)$/,

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

        if (target == 'conesearch') {
          $('#conesearch-help-box').removeClass('bg-warning');
        }

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
        if (data.visibility == 'public') {

          // hit the /api/datasets URL to update the datasets
          // also highlight the row with our query result in it
          lcc_ui.get_recent_datasets(100, msgdata.result.setid);

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
            + msgdata.result.actual_nrows +
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

        if (target == 'conesearch') {
          $('#conesearch-help-box').removeClass('bg-warning');
        }

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
            '</code> is complete, but there are too many ' +
            'LCs to collect, so no LC ZIP file was generated. ' +
            'Try refining your query, or see ' +
            '<a target="_blank" ' +
            'rel="nofollow noreferrer noopener" href="' +
            msgdata.result.seturl +
            '">the query result dataset page</a> for a ' +
            'CSV that lists all objects and download links ' +
            'for their individual light curves.';
        }

        else {
          // notify the user that the query is in the background
          alertmsg = 'Query <code>' +
            msgdata.result.setid +
            '</code> is now in the background queue. ' +
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

        // if this is a conesearch query and the message includes
        // 'could not parse' or 'invalid', then highlight the helpbox
        if (target == 'conesearch' &&
            (msgdata.message.indexOf('could not parse') != -1 ||
             msgdata.message.indexOf('invalid') != -1)) {
          $('#conesearch-help-box').addClass('bg-warning');
        }

        else {
          $('#conesearch-help-box').removeClass('bg-warning');
        }

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

        // re-enable the submit button until we return
        // FIXME: may need to turn on handler as well
        $('#' + target + '-submit').attr('disabled',false);

      });

    return nrun;
  },



  do_xmatch: function (override_params) {

    var _xsrf;
    var emailwhendone;
    var posturl = '/api/xmatch';
    var postparams;
    var proceed_step1 = false;
    var proceed_step2 = false;

    if (override_params !== undefined) {

      proceed_step1 = true;
      proceed_step2 = true;

      // get the value of the email_when_done checkbox
      emailwhendone = $('#xmatch-emailwhendone-check').prop('checked');

      // get the value of the _xsrf token
      _xsrf = $('#xmatch-form > input[type="hidden"]').val();

      // put together the request params
      postparams = {
        _xsrf:_xsrf,
        xmq: override_params.xmq,
        xmd: override_params.xmd,
        collections: override_params.collections,
        columns: override_params.columns,
        filters: override_params.filters,
        visibility: override_params.visibility,
        sortspec: override_params.sortspec,
        samplespec: override_params.samplespec,
        limitspec: override_params.limitspec,
        emailwhendone: emailwhendone
      };

    }

    else {

      proceed_step1 = false;
      proceed_step2 = false;

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

      // get the visibility parameter
      var visibility = $('#xmatch-visibility-select').val();

      // get the sort spec
      var sortcol = $('#xmatch-sortcolumn-select').val();
      var sortorder = $('#xmatch-sortorder-select').val();

      // this is a list of list items
      var sortspec = JSON.stringify([[sortcol, sortorder]]);

      // also, add the sortby column to the retrieval column list
      var sortcol_in_columns = columns.find(function (elem) {
        return elem == sortcol;
      });
      if (!sortcol_in_columns) {
        columns.push(sortcol);
      }

      // get the sample spec
      var samplespec = parseInt($('#xmatch-samplerows').val());
      if (isNaN(samplespec) || !$('#xmatch-samplecheck').prop('checked')) {
        samplespec = null;
      }

      // get the limit spec
      var limitspec = parseInt($('#xmatch-limitrows').val());
      if (isNaN(limitspec) || !$('#xmatch-limitcheck').prop('checked')) {
        limitspec = null;
      }

      // get the value of the _xsrf token
      _xsrf = $('#xmatch-form > input[type="hidden"]').val();

      // get the value of the email_when_done checkbox
      emailwhendone = $('#xmatch-emailwhendone-check').prop('checked');

      // put together the request params
      posturl = '/api/xmatch';
      postparams = {
        _xsrf:_xsrf,
        xmq: xmatchtext,
        xmd: xmatchdistance,
        collections: collections,
        columns: columns,
        filters: filters,
        visibility: visibility,
        sortspec: sortspec,
        samplespec: samplespec,
        limitspec: limitspec,
        emailwhendone: emailwhendone
      };

    }

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
      nrun = lcc_search.run_search_query(
        posturl,
        postparams,
        'POST',
        'xmatch',
        nrun
      );

    }

    else {
      var error_message =
          "Invalid input in the cross-match object list input box.";
      lcc_ui.alert_box(error_message, 'secondary');
    }


  },



  // this runs a full column search
  do_columnsearch: function (override_params) {

    var _xsrf;
    var emailwhendone;
    var posturl = '/api/columnsearch';
    var postparams;
    var proceed = false;

    if (override_params !== undefined) {

      // get the value of the email_when_done checkbox
      emailwhendone = $('#columnsearch-emailwhendone-check').prop('checked');

      // get the value of the _xsrf token
      _xsrf = $('#columnsearch-form > input[type="hidden"]').val();

      // get the params
      postparams = {
        _xsrf:_xsrf,
        collections: override_params.collections,
        columns: override_params.columns,
        filters: override_params.filters,
        visibility: override_params.visibility,
        sortspec: override_params.sortspec,
        samplespec: override_params.samplespec,
        limitspec: override_params.limitspec,
        emailwhendone: emailwhendone
      };

      proceed = true;

    }

    else {

      // get the collections to use
      var collections = $('#columnsearch-collection-select').val();

      if (collections.length == 0) {
        collections = null;
      }

      // get the columns to retrieve
      var columns = $('#columnsearch-column-select').val();

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

      // get the visibility parameter
      var visibility = $('#columnsearch-visibility-select').val();

      // get the sort spec
      var sortcol = $('#columnsearch-sortcolumn-select').val();
      var sortorder = $('#columnsearch-sortorder-select').val();

      // this is a list of list items
      var sortspec = JSON.stringify([[sortcol, sortorder]]);

      // also, add the sortby column to the retrieval column list
      var sortcol_in_columns = columns.find(function (elem) {
        return elem == sortcol;
      });
      if (!sortcol_in_columns) {
        columns.push(sortcol);
      }

      // get the sample spec
      var samplespec = parseInt($('#columnsearch-samplerows').val());
      if (isNaN(samplespec) || !$('#columnsearch-limitcheck').prop('checked')) {
        samplespec = null;
      }

      // get the limit spec
      var limitspec = parseInt($('#columnsearch-limitrows').val());
      if (isNaN(limitspec) || !$('#columnsearch-limitcheck').prop('checked')) {
        limitspec = null;
      }

      // get the value of the email_when_done checkbox
      emailwhendone = $('#columnsearch-emailwhendone-check').prop('checked');

      // get the value of the _xsrf token
      _xsrf = $('#columnsearch-form > input[type="hidden"]').val();

      // get the params
      postparams = {
        _xsrf:_xsrf,
        collections: collections,
        columns: columns,
        filters: filters,
        visibility: visibility,
        sortspec: sortspec,
        samplespec: samplespec,
        limitspec: limitspec,
        emailwhendone: emailwhendone
      };

    }

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
      nrun = lcc_search.run_search_query(
        posturl,
        postparams,
        'POST',
        'columnsearch',
        nrun
      );

    }

    else {
      var error_message =
          "No valid column filters were found for the column search query.";
      lcc_ui.alert_box(error_message, 'secondary');
    }

  },



  // this runs an FTS query
  do_ftsquery: function (override_params) {

    var _xsrf;
    var emailwhendone;
    var posturl = '/api/ftsquery';
    var postparams;
    var proceed = false;

    if (override_params !== undefined) {

      // get the value of the email_when_done checkbox
      emailwhendone = $('#ftsquery-emailwhendone-check').prop('checked');

      // get the value of the _xsrf token
      _xsrf = $('#ftsquery-form > input[type="hidden"]').val();

      // get the params
      postparams = {
        _xsrf:_xsrf,
        ftstext: override_params.ftstext,
        sesame: override_params.sesame_check,
        collections: override_params.collections,
        columns: override_params.columns,
        filters: override_params.filters,
        visibility: override_params.visibility,
        sortspec: override_params.sortspec,
        samplespec: override_params.samplespec,
        limitspec: override_params.limitspec,
        emailwhendone: emailwhendone
      };

      proceed = true;

    }

    else {

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

      // see if the user wants to resolve object names with SESAME
      var sesame_check = $('#ftsquery-sesame-check').prop('checked');

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

      // get the visibility parameter
      var visibility = $('#ftsquery-visibility-select').val();

      // get the sort spec
      var sortcol = $('#ftsquery-sortcolumn-select').val();
      var sortorder = $('#ftsquery-sortorder-select').val();

      // this is a list of list items
      var sortspec = JSON.stringify([[sortcol, sortorder]]);

      // also, add the sortby column to the retrieval column list
      var sortcol_in_columns = columns.find(function (elem) {
        return elem == sortcol;
      });
      if (!sortcol_in_columns) {
        columns.push(sortcol);
      }

      // get the sample spec
      var samplespec = parseInt($('#ftsquery-samplerows').val());
      if (isNaN(samplespec) || !$('#ftsquery-samplecheck').prop('checked')) {
        samplespec = null;
      }

      // get the limit spec
      var limitspec = parseInt($('#ftsquery-limitrows').val());
      if (isNaN(limitspec) || !$('#ftsquery-limitcheck').prop('checked')) {
        limitspec = null;
      }

      // get the value of the _xsrf token
      _xsrf = $('#ftsquery-form > input[type="hidden"]').val();

      // get the value of the email_when_done checkbox
      emailwhendone = $('#ftsquery-emailwhendone-check').prop('checked');

      posturl = '/api/ftsquery';
      postparams = {
        ftstext: ftstext,
        sesame: sesame_check,
        _xsrf:_xsrf,
        collections: collections,
        columns: columns,
        filters: filters,
        visibility: visibility,
        sortspec: sortspec,
        samplespec: samplespec,
        limitspec: limitspec,
        emailwhendone: emailwhendone
      };

    }

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
      nrun = lcc_search.run_search_query(posturl,
                                         postparams,
                                         'POST',
                                         'ftsquery',
                                         nrun);

    }
    else {
      var error_message =
          "No query text found in the FTS query text box.";
      lcc_ui.alert_box(error_message, 'secondary');
    }

  },



  // this runs a cone search query
  do_conesearch: function(override_params) {

    var _xsrf;
    var emailwhendone;
    var posturl = '/api/conesearch';
    var postparams;
    var proceed = false;

    if (override_params !== undefined) {

      // get the value of the email_when_done checkbox
      emailwhendone = $('#conesearch-emailwhendone-check').prop('checked');

      // get the value of the _xsrf token
      _xsrf = $('#conesearch-form > input[type="hidden"]').val();

      // get the params
      postparams = {
        _xsrf:_xsrf,
        coords: override_params.coords,
        collections: override_params.collections,
        columns: override_params.columns,
        filters: override_params.filters,
        visibility: override_params.visibility,
        sortspec: override_params.sortspec,
        samplespec: override_params.samplespec,
        limitspec: override_params.limitspec,
        emailwhendone: emailwhendone
      };

      proceed = true;

    }

    else {

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

      // get the visibility parameter
      var visibility = $('#conesearch-visibility-select').val();

      // get the sort spec
      var sortcol = $('#conesearch-sortcolumn-select').val();
      var sortorder = $('#conesearch-sortorder-select').val();

      // this is a list of list items
      var sortspec = JSON.stringify([[sortcol, sortorder]]);

      // also, add the sortby column to the retrieval column list
      var sortcol_in_columns = columns.find(function (elem) {
        return elem == sortcol;
      });
      if (!sortcol_in_columns) {
        columns.push(sortcol);
      }

      // get the sample spec
      var samplespec = parseInt($('#conesearch-samplerows').val());
      if (isNaN(samplespec) || !$('#conesearch-samplecheck').prop('checked')) {
        samplespec = null;
      }

      // get the limit spec
      var limitspec = parseInt($('#conesearch-limitrows').val());
      if (isNaN(limitspec) || !$('#conesearch-limitcheck').prop('checked')) {
        limitspec = null;
      }

      // get the value of the _xsrf token
      _xsrf = $('#conesearch-form > input[type="hidden"]').val();

      // get the value of the email_when_done checkbox
      emailwhendone = $('#conesearch-emailwhendone-check').prop('checked');

      posturl = '/api/conesearch';
      postparams = {
        _xsrf: _xsrf,
        coords: coords,
        collections: collections,
        columns: columns,
        filters: filters,
        visibility: visibility,
        sortspec: sortspec,
        samplespec: samplespec,
        limitspec: limitspec,
        emailwhendone: emailwhendone
      };

    }

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
      nrun = lcc_search.run_search_query(posturl,
                                         postparams,
                                         'POST',
                                         'conesearch',
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
  staticbits_rendered: false,

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
      '<th width="60" class="text-center">details</th>'
    );

    // these are used to calculate the full table width
    var column_widths = [60];
    var thiscol_width = null;

    // generate the column names and descriptions, put them into the
    // column definitions table, and also append them to the header
    // row of the data table

    let colind_objectid = 0;
    let colind_collection = columns.length - 1;
    let colind_lcfname = 0;
    let colind_extrainfo = -1;
    let colind_simbad_best_allids = -1;
    let colind_simbad_best_objtype = -1;

    for (colind; colind < columns.length; colind++) {

      var this_col = columns[colind];

      if (this_col == 'db_oid') {
        colind_objectid = colind;
      }

      if (this_col == 'collection') {
        colind_collection = colind;
      }

      if (this_col == 'extra_info') {
        colind_extrainfo = colind;
      }

      if (this_col == 'simbad_best_allids') {
        colind_simbad_best_allids = colind;
      }

      if (this_col == 'simbad_best_objtype') {
        colind_simbad_best_objtype = colind;
      }

      if (this_col == 'db_lcfname') {
        colind_lcfname = colind;
      }
      else if (this_col == 'lcfname') {
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
        '<th class="text-center" width="'+ thiscol_width + '">' + this_col + '</th>'
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

    return [colind_objectid,
            colind_collection,
            colind_lcfname,
            colind_extrainfo,
            colind_simbad_best_allids,
            colind_simbad_best_objtype];
  },


  // this renders the datatable rows as soon as they're received from the
  // backend
  render_datatable_rows: function (data,
                                   colind_objectid,
                                   colind_collection,
                                   colind_lcfname,
                                   colind_extrainfo,
                                   colind_simbad_best_allids,
                                   colind_simbad_best_objtype) {
    var rowind = 0;
    var datarows_elem = $('#lcc-datatable-datarows');

    // clear the table first
    datarows_elem.empty();
    lcc_datasets.objectid_map = {};

    // check if there are too many rows
    // if so, only draw the first 3000
    var max_rows = data.rows.length;
    if (data.rows.length > 3000) {
      max_rows = 3000;
    }

    var objectentry_firstcol = '';
    var thisrow = null;
    var thisrow_lclink = null;
    var thisrow_collection = null;
    var thisrow_lcmagcols = null;

    for (rowind; rowind < max_rows; rowind++) {

      // get this object's db_oid and collection. we'll use these
      // to set up the links to checkplot info on-demand in the
      // first column of the table

      thisrow = data.rows[rowind];
      let prev_objectid;
      let prev_lcfname;
      let next_objectid;
      let next_lcfname;

      if (rowind == 0) {
        prev_objectid = null;
        prev_lcfname = null;

        if (max_rows > 1) {
          next_objectid = data.rows[rowind+1][colind_objectid];
          next_lcfname = data.rows[rowind+1][colind_lcfname];
        }
        else {
          next_objectid = null;
          next_lcfname = null;
        }
      }
      else if (rowind == max_rows - 1) {
        next_objectid = null;
        next_lcfname = null;
        prev_objectid = data.rows[rowind-1][colind_objectid];
        prev_lcfname = data.rows[rowind-1][colind_lcfname];
      }
      else {
        prev_objectid = data.rows[rowind-1][colind_objectid];
        prev_lcfname = data.rows[rowind-1][colind_lcfname];
        next_objectid = data.rows[rowind+1][colind_objectid];
        next_lcfname = data.rows[rowind+1][colind_lcfname];
      }

      // FIXME: use this to implement the next/prev object links
      // FIXME: add a back to original object button to modal bottom
      // FIXME: disable next/prev links after clicking on neighbors
      // FIXME: implement a full neighbors tab

      // store these values in the lcc_datasets object
      lcc_datasets.objectid_map[thisrow[colind_objectid]] = {
        prev_objectid:prev_objectid,
        prev_lcfname:prev_lcfname,
        next_objectid:next_objectid,
        next_lcfname:next_lcfname,
        this_rowind: rowind
      };

      // get this row's light curve if available
      thisrow_lclink = thisrow[colind_lcfname];

      // get this row's collection
      thisrow_collection = thisrow[colind_collection];

      // get this row's LC magcols if available
      if ( ('lcmagcols' in data) &&
           (data['lcmagcols'] !== null || data['lcmagcols'] !== undefined) &&
           (thisrow_collection in data['lcmagcols']) &&
           ( (data['lcmagcols'][thisrow_collection] !== undefined) ||
             (data['lcmagcols'][thisrow_collection] !== null) ) ) {

        thisrow_lcmagcols = data['lcmagcols'][thisrow_collection];

      }

      // FIXME: note that if the LC isn't ready yet, this will throw a
      // 404. use a handler to check the download link and display an
      // alert if it returns a 404. if it doesn't, pass through the file
      // to window.location (??). that should trigger a download.
      thisrow[colind_lcfname] =
        '<a class="download-lc-link" download rel="nofollow" href="' +
        thisrow_lclink + '">download light curve</a>';

      // bibcode linkify the extra_info, simbad_best_allids, and
      // simbad_best_obtype columns
      if (colind_extrainfo > -1) {

        thisrow[colind_extrainfo] =
          '<details class="table-details-elem"><summary>view JSON</summary>' +
          lcc_ui.bib_linkify(
            '<pre>' +
              JSON.stringify(JSON.parse(thisrow[colind_extrainfo]),null, 2) +
              '</pre>'
          ) +
          '</details>';

      }

      if (colind_simbad_best_allids > -1) {
        thisrow[colind_simbad_best_allids] = lcc_ui.bib_linkify(
          thisrow[colind_simbad_best_allids]
        );
      }
      if (colind_simbad_best_objtype > -1) {
        thisrow[colind_simbad_best_objtype] = lcc_ui.bib_linkify(
          thisrow[colind_simbad_best_objtype]
        );
      }

      // add the details column to the row
      objectentry_firstcol = '<a href="#" rel="nofollow" role="button" ' +
        'data-toggle="modal" data-target="#objectinfo-modal"' +
        'title="get available object information" ' +
        'data-objectid="' + thisrow[colind_objectid] + '" ' +
        'data-collection="' + thisrow[colind_collection] + '" ' +
        'data-lcfname="' + thisrow_lclink + '" ' +
        'data-lcmagcols="' + thisrow_lcmagcols + '" ' +
        'class="btn btn-link btn-sm objectinfo-link">' +
        '<img class="table-icon-svg" ' +
        'src="/static/images/twotone-assistant-24px.svg"></a>';
      thisrow.splice(0,0,objectentry_firstcol);

      datarows_elem.append(
        '<tr><td width="60" class="text-center table-warning">' +
          data.rows[rowind].join('</td><td class="text-center">')
          .replace(/href/g,'rel="nofollow" href') +
          '</td></tr>'
      );

    }

    // make the table div bottom stick to the bottom of the
    // container so we can have a scrollbar at the bottom

    // calculate the offset
    var datacontainer_offset =
        $('.datatable-container').offset().top;

    $('.datatable-container').height(window.innerHeight -
                                     datacontainer_offset);

    // set the height appropriately
    $('.dataset-table')
      .height($('.datatable-container').height());

  },


  // this just gets the header once and loads the first page
  get_dataset_preview: function (setid, refresh) {

    var geturl = '/set/' + setid;
    var getparams = {json: 1,
                     strformat: 1};

    // here we do the retrieval
    $.getJSON(geturl, getparams, function (data) {

      var status = data.status;

      // fill in stuff that only needs to be done once
      if (!lcc_datasets.staticbits_rendered) {

        // save these to the object so we can refer to them later
        [lcc_datasets.colind_objectid,
         lcc_datasets.colind_collection,
         lcc_datasets.colind_lcfname,
         lcc_datasets.colind_extrainfo,
         lcc_datasets.colind_simbad_best_allids,
         lcc_datasets.colind_simbad_best_objtype] =
          lcc_datasets.render_column_definitions(data);

        // searchtype and searchargs
        $('#dataset-searchargs').html(
          '<details><summary>' +
            data.searchtype
            .replace(/sqlite_/g,'')
            .replace(/postgres_/g,'') +
            '</summary><pre>' +
            JSON.stringify(data.searchargs,
                           null,
                           2) +
            '</pre></detail>'
        );

        // collections queries
        $('#dataset-collections').html( data.collections.join(', '));

        // setpickle URL
        $('#dataset-setpickle')
          .html('<a download rel="nofollow" href="' +
                data.dataset_pickle + '">download file</a>');

        // setcsv URL
        $('#dataset-setcsv')
          .html('<a download rel="nofollow" href="' +
                data.dataset_csv + '">download file</a>');


        // nobjects in this dataset
        if ('actual_nrows' in data) {
          $('#dataset-nobjects').html(
            data.actual_nrows +
              ' (showing ' +
              data.rows_per_page +
              ' per page &mdash; see the ' +
              '<a download rel="nofollow" href="' +
              data.dataset_csv + '">dataset CSV</a>' +
              ' for complete table)'
          );
        }
        else {
          $('#dataset-nobjects').html(data.actual_nrows);
        }

        // update the current page number
        $('.dataset-pagination-currpage').html(
          '<span id="page-indicator" data-currpage="' +
            data.currpage +
            '">Page 1 of ' + data.npages
        );

        // parse and display the dataset owner
        if (data.owned ||
            (data.editable !== undefined && data.editable === true)) {

          let visibility_controls = `
<details>
<summary>Dataset is currently ${data.visibility}.</summary>
<div class="form-inline">
  <select class="custom-select" id="dataset-visibility-select">
    <option value="public">Dataset is publicly listed and visible to all users</option>
    <option value="unlisted">Dataset is private but accessible at this URL</option>
    <option value="private">Dataset is private and inaccessible to others</option>
  </select>
  <button class="ml-2 btn btn-outline-success"
          type="button" id="dataset-visibility-submit">Update visibility</button>
</div>
</details>
`;
          let name_controls = `
<details>
<summary>${data.name}</summary>
<div class="form-inline">
  <input type="text" class="form-control flex-grow-1" id="dataset-name-inputbox"
         value="${data.name}" placeholder="Type in a dataset name."
         maxlength="280">
  <button class="ml-2 btn btn-outline-success"
          type="button" id="dataset-name-submit">Update name</button>
</div>
</details>
`;

          let desc_controls = `
<details>
<summary>${lcc_ui.bib_linkify(data.desc)}</summary>
<div class="form-inline">
  <input type="text" class="form-control flex-grow-1" id="dataset-desc-inputbox"
         value="${data.desc}" placeholder="Type in a description. ADS bibcodes and DOIs will be auto-linked."
         maxlength="1024">
  <button class="ml-2 btn btn-outline-success"
          type="button" id="dataset-desc-submit">Update description</button>
</div>
</details>
`;

          let citation_controls = `
<details>
<summary>${lcc_ui.bib_linkify(data.citation)}</summary>
<div class="form-inline">
  <input type="text" class="form-control flex-grow-1" id="dataset-citation-inputbox"
         value="${data.citation}" placeholder="Type in a citation. ADS bibcodes and DOIs will be auto-linked."
         maxlength="1024">
  <button class="ml-2 btn btn-outline-success"
          type="button" id="dataset-citation-submit">Update citation</button>
</div>
</details>
`;

          // if we can edit this DS, we can set its owner.
          if (data.editable !== undefined && data.editable === true) {

            let owner_controls = `
<details>
<summary>Dataset is currently owned by user ID: ${data.owner}</summary>
<div class="form-inline">
  <input type="text" class="form-control" id="owner-label-inputbox"
         value="${data.owner}"
         placeholder="Type in user ID of new owner."
         maxlength="10" minlength="1" required>
  <button class="ml-2 btn btn-outline-success"
          type="button" id="owner-label-submit">Update owner</button>
</div>
</details>
`;

            $('#owner-label').html(
              owner_controls
            );

          }

          // otherwise, if we own this dataset, show that we do.
          else if (data.owned) {

            $('#owner-label').html(
              '<span class="text-success">' +
                'You own this dataset. You can ' +
                'edit its metadata and set its visibility.</span>'
            );

          }


          $('#visibility-label').html(visibility_controls);
          $('#dataset-visibility-select').val(data.visibility);
          $('#dataset-name').html(name_controls);
          $('#dataset-desc').html(desc_controls);
          $('#dataset-citation').html(citation_controls);

        }

        else {
          let dataset_desc = $('#other-dataset-desc').html();
          $('#other-dataset-desc').html(lcc_ui.bib_linkify(dataset_desc));
          let dataset_citation = $('#other-dataset-citation').html();
          $('#other-dataset-citation').html(lcc_ui.bib_linkify(dataset_citation));
        }

      }

      // the rest of the stuff can be called over again until we stop

      // 2a. created
      var created_on = data.created;
      created_on = created_on + ' UTC <strong>(' +
        moment(created_on + 'Z').fromNow() + ')<strong>';
      $('#dataset-createdon').html(created_on);

      // 2b. lastupdated
      var last_updated = data.updated;
      last_updated = last_updated + ' UTC <strong>(' +
        moment(last_updated + 'Z').fromNow() + ')<strong>';
      $('#dataset-lastupdated').html(last_updated);

      // show the LC ZIP URL
      if (data.lczipfpath != null && data.lczipfpath != undefined) {
        // 12. lczip
        $('#dataset-lczip')
          .html('<a download rel="nofollow" href="' +
                data.lczipfpath + '">download file</a>');
      }
      else {
        $('#dataset-lczip').html('not available yet');
      }

      // set the status indicator and kill the loading icon as appropriate
      if (status == 'complete') {
        $('#dataset-status').html('<span class="text-success">' +
                                  status +
                                  '</span>');
        $('#setload-icon').empty();
        $('#setload-indicator').empty();
        lcc_datasets.dataset_complete = true;
      }
      else {
        $('#dataset-status').html('<span class="text-secondary">' +
                                  status +
                                  '</span>');
        $('#setload-indicator').html(
          '<span class="text-warning">' +
            'verifying and converting LCs...' +
            '</span>'
        );
        lcc_datasets.dataset_complete = false;
      }

    }).done(function (data) {

      if (!lcc_datasets.staticbits_rendered) {

        // load the first page only once
        lcc_datasets.render_datatable_rows(
          data,
          lcc_datasets.colind_objectid,
          lcc_datasets.colind_collection,
          lcc_datasets.colind_lcfname,
          lcc_datasets.colind_extrainfo,
          lcc_datasets.colind_simbad_best_allids,
          lcc_datasets.colind_simbad_best_objtype
        );

        // set the bits for later rendering
        lcc_datasets.staticbits_rendered = true;
        lcc_datasets.npages = data.npages;
        lcc_datasets.currpage = 1;
        lcc_datasets.setid = setid;

      }

      if (!lcc_datasets.dataset_complete) {

        window.setTimeout(function () {
          lcc_datasets.get_dataset_preview(setid, refresh);
        }, refresh*1000.0);

      }

    }).fail(function (xhr) {

      var message = 'Could not retrieve the dataset ' +
          'from the LCC server backend.';

      if (xhr.status == 500) {
        message = 'Something went wrong with the LCC-Server backend ' +
          'while trying to fetch this dataset.';
      }

      lcc_ui.alert_box(message, 'danger');

      // clear out the loading indicators at the end
      $('#setload-icon').empty();
      $('#setload-indicator').empty();

    });

  },


  // this gets an arbitrary dataset page
  get_dataset_page: function (setid, pagenumber) {

    // do nothing if the page number is out of bounds
    if ((pagenumber > lcc_datasets.npages) ||
        (pagenumber < 1) ||
        (pagenumber == lcc_datasets.currpage)) {
      return;
    }

    var geturl = '/set/' + setid;
    var getparams = {json: 1,
                     strformat: 1,
                     page: pagenumber};


    // set the loading indicators
    var load_indicator = $('#setload-indicator');
    if (load_indicator.text() == '') {

      $('#setload-indicator').html(
        '<span class="text-warning">getting dataset page...' +
          '</span>'
      );

      $('#setload-icon').html(
        '<img src="/static/images/twotone-sync-24px.svg' +
          '" class="animated flash infinite">'
      );

    }

    // here we do the retrieval
    $.getJSON(geturl, getparams, function (data) {

      var status = data.status;

      //////////////////////////////
      // fill in the header first //
      //////////////////////////////

      // 2a. created
      var created_on = data.created;
      created_on = created_on + ' UTC <strong>(' +
        moment(created_on + 'Z').fromNow() + ')<strong>';
      $('#dataset-createdon').html(created_on);

      // 2b. lastupdated
      var last_updated = data.updated;
      last_updated = last_updated + ' UTC <strong>(' +
        moment(last_updated + 'Z').fromNow() + ')<strong>';
      $('#dataset-lastupdated').html(last_updated);

      // 3. status
      $('#dataset-status').html('<span class="text-success">' +
                                status +
                                '</span>');

      // show the LC ZIP URL
      if (data.lczipfpath != null && data.lczipfpath != undefined) {
        // 12. lczip
        $('#dataset-lczip')
          .html('<a download rel="nofollow" href="' +
                data.lczipfpath + '">download file</a>');
      }
      else {
        $('#dataset-lczip').html('not available yet');
      }

      // update the page number
      $('.dataset-pagination-currpage').html(
        'Page ' + pagenumber + ' of ' + data.npages
      );
      lcc_datasets.currpage = pagenumber;

      ////////////////////////////////////////
      // finally, fill in the dataset table //
      ////////////////////////////////////////

      lcc_datasets.render_datatable_rows(
        data,
        lcc_datasets.colind_objectid,
        lcc_datasets.colind_collection,
        lcc_datasets.colind_lcfname,
        lcc_datasets.colind_extrainfo,
        lcc_datasets.colind_simbad_best_allids,
        lcc_datasets.colind_simbad_best_objtype
      );

    }).fail(function (xhr) {

      var message = 'Could not retrieve the dataset ' +
          'from the LCC server backend.';

      if (xhr.status == 500) {
        message = 'Something went wrong with the LCC-Server backend ' +
          'while trying to fetch this dataset.';
      }

      lcc_ui.alert_box(message, 'danger');

      // clear out the loading indicators at the end
      $('#setload-icon').empty();
      $('#setload-indicator').empty();

    }).done(function (data) {

      // clear out the loading indicators at the end
      $('#setload-icon').empty();
      $('#setload-indicator').empty();

    });

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
        '<span class="text-warning">getting dataset...' +
          '</span>'
      );

      $('#setload-icon').html(
        '<img src="/static/images/twotone-sync-24px.svg' +
          '" class="animated flash infinite">'
      );

    }

    // call this to render the initial bits and start refresh loop if
    // dataset is not yet complete
    lcc_datasets.get_dataset_preview(setid, refresh);

  },


  // this changes a dataset's owner
  change_dataset_owner: function (setid, new_owner) {

    var posturl = '/set/' + setid;
    var _xsrf = $('#dataset-edit-form > input[type="hidden"]').val();
    var postparams = {
      _xsrf: _xsrf,
      action: 'change_owner',
      update: JSON.stringify({new_owner_userid: parseInt(new_owner)})
    };

    $.post(posturl, postparams, function (data) {

      var result = data.result;
      var message = data.message;
      var status = data.status;

      if (status == 'ok') {

        // update the dataset's name
        $('#owner-label > details > summary').html(
          'Dataset is currently owned by user ID: ' + result
        );
        $('#owner-label-inputbox').val(result);

        var last_updated = data.date;
        last_updated = last_updated + ' UTC <strong>(' +
          moment(last_updated + 'Z').fromNow() + ')<strong>';
        $('#dataset-lastupdated').html(last_updated);

      }

      else {

        lcc_ui.alert_box(message, 'danger');

        // clear out the loading indicators at the end
        $('#setload-icon').empty();
        $('#setload-indicator').empty();

      }

    },'json').fail(function (xhr) {

      var message = 'Could not edit this dataset.';

      if (xhr.status == 500) {
        message = 'Something went wrong with the LCC-Server backend ' +
          'while trying to fetch this dataset.';
      }

      lcc_ui.alert_box(message, 'danger');

      // clear out the loading indicators at the end
      $('#setload-icon').empty();
      $('#setload-indicator').empty();

    });

  },


  // this edits a dataset's name
  edit_dataset_name: function (setid, new_name) {

    var posturl = '/set/' + setid;
    var _xsrf = $('#dataset-edit-form > input[type="hidden"]').val();
    var postparams = {
      _xsrf: _xsrf,
      action: 'edit',
      update: JSON.stringify({name:new_name})
    };

    $.post(posturl, postparams, function (data) {

      var result = data.result;
      var message = data.message;
      var status = data.status;

      if (status == 'ok') {

        // update the dataset's name
        $('#dataset-name > details > summary').html(result.name);
        $('#dataset-name-inputbox').val(result.name);

        var last_updated = data.date;
        last_updated = last_updated + ' UTC <strong>(' +
          moment(last_updated + 'Z').fromNow() + ')<strong>';
        $('#dataset-lastupdated').html(last_updated);

        if ('slug' in result &&
            document.URL.indexOf(result.slug) == -1) {
          $('#dataset-url > a').text(document.URL +
                                     '/' +
                                     result.slug);
          $('#dataset-url > a').attr('href',
                                     document.URL +
                                     '/' +
                                     result.slug);
        }

      }

      else {

        lcc_ui.alert_box(message, 'danger');

        // clear out the loading indicators at the end
        $('#setload-icon').empty();
        $('#setload-indicator').empty();

      }

    },'json').fail(function (xhr) {

      var message = 'Could not edit this dataset.';

      if (xhr.status == 500) {
        message = 'Something went wrong with the LCC-Server backend ' +
          'while trying to fetch this dataset.';
      }

      lcc_ui.alert_box(message, 'danger');

      // clear out the loading indicators at the end
      $('#setload-icon').empty();
      $('#setload-indicator').empty();

    });

  },

  // this edits a dataset's description
  edit_dataset_description: function (setid, new_description) {

    var posturl = '/set/' + setid;
    var _xsrf = $('#dataset-edit-form > input[type="hidden"]').val();
    var postparams = {
      _xsrf: _xsrf,
      action: 'edit',
      update: JSON.stringify({description:new_description})
    };

    $.post(posturl, postparams, function (data) {

      var result = data.result;
      var message = data.message;
      var status = data.status;

      if (status == 'ok') {

        var last_updated = data.date;
        last_updated = last_updated + ' UTC <strong>(' +
          moment(last_updated + 'Z').fromNow() + ')<strong>';
        $('#dataset-lastupdated').html(last_updated);

        // update the dataset's description
        $('#dataset-desc > details > summary').html(
          lcc_ui.bib_linkify(result.desc)
        );
        $('#dataset-desc-inputbox').val(result.desc);

      }

      else {

        lcc_ui.alert_box(message, 'danger');

        // clear out the loading indicators at the end
        $('#setload-icon').empty();
        $('#setload-indicator').empty();

      }

    },'json').fail(function (xhr) {

      var message = 'Could not edit this dataset.';

      if (xhr.status == 500) {
        message = 'Something went wrong with the LCC-Server backend ' +
          'while trying to fetch this dataset.';
      }

      lcc_ui.alert_box(message, 'danger');

      // clear out the loading indicators at the end
      $('#setload-icon').empty();
      $('#setload-indicator').empty();

    });

  },

  // this edits a dataset's citation
  edit_dataset_citation: function (setid, new_citation) {

    var posturl = '/set/' + setid;
    var _xsrf = $('#dataset-edit-form > input[type="hidden"]').val();
    var postparams = {
      _xsrf: _xsrf,
      action: 'edit',
      update: JSON.stringify({citation:new_citation})
    };

    $.post(posturl, postparams, function (data) {

      var result = data.result;
      var message = data.message;
      var status = data.status;

      if (status == 'ok') {

        var last_updated = data.date;
        last_updated = last_updated + ' UTC <strong>(' +
          moment(last_updated + 'Z').fromNow() + ')<strong>';
        $('#dataset-lastupdated').html(last_updated);

        // update the dataset's citation
        $('#dataset-citation > details > summary').html(
          lcc_ui.bib_linkify(result.citation)
        );
        $('#dataset-citation-inputbox').val(result.citation);

      }

      else {

        lcc_ui.alert_box(message, 'danger');

        // clear out the loading indicators at the end
        $('#setload-icon').empty();
        $('#setload-indicator').empty();

      }

    },'json').fail(function (xhr) {

      var message = 'Could not edit this dataset.';

      if (xhr.status == 500) {
        message = 'Something went wrong with the LCC-Server backend ' +
          'while trying to fetch this dataset.';
      }

      lcc_ui.alert_box(message, 'danger');

      // clear out the loading indicators at the end
      $('#setload-icon').empty();
      $('#setload-indicator').empty();

    });

  },

  // this changes a dataset's visibility
  change_dataset_visibility: function (setid, new_visibility) {

    var posturl = '/set/' + setid;
    var _xsrf = $('#dataset-edit-form > input[type="hidden"]').val();
    var postparams = {
      _xsrf: _xsrf,
      action: 'change_visibility',
      update: JSON.stringify({new_visibility: new_visibility})
    };

    $.post(posturl, postparams, function (data) {

      var result = data.result;
      var message = data.message;
      var status = data.status;

      if (status == 'ok') {

        var last_updated = data.date;
        last_updated = last_updated + ' UTC <strong>(' +
          moment(last_updated + 'Z').fromNow() + ')<strong>';
        $('#dataset-lastupdated').html(last_updated);

        // update the dataset's visibility
        $('#visibility-label > details > summary').html(
          'Dataset is currently ' + result
        );
        $('#dataset-visibility-select').val(result);

      }

      else {

        lcc_ui.alert_box(message, 'danger');

        // clear out the loading indicators at the end
        $('#setload-icon').empty();
        $('#setload-indicator').empty();

      }

    },'json').fail(function (xhr) {

      var message = 'Could not edit this dataset.';

      if (xhr.status == 500) {
        message = 'Something went wrong with the LCC-Server backend ' +
          'while trying to fetch this dataset.';
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

        <a class="nav-item nav-link" id="modal-gaianeighbors"
           data-toggle="tab" href="#mtab-gaianeighbors" role="tab"
           aria-controls="modal-gaianeighbors" aria-selected="false">
          GAIA neighbors
        </a>

        <a class="nav-item nav-link" id="modal-varfeatures"
           data-toggle="tab" href="#mtab-varfeatures" role="tab"
           aria-controls="modal-varfeatures" aria-selected="false">
          Variability features
        </a>

      </div>
    </nav>

    <div class="tab-content" id="modal-nav-content">

      <div class="tab-pane show active" id="mtab-objectinfo"
           role="tabpanel" aria-labelledby="modal-objectinfo">

        <div class="row mt-4">
          <div class="col-sm-12 col-md-6">

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

          <div class="col-sm-12 col-md-6">

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

        <div class="row mt-4">
          <div class="col-12">

            <div id="modal-phasedlc-container" class="phasedlc-container">

            </div>

          </div>
        </div>

      </div>


      <div class="tab-pane" id="mtab-gaianeighbors"
           role="tabpanel" aria-labelledby="modal-gaianeighbors">

        <div class="row mt-4 gaia-neighbor-table-container">
          <div class="col-12">

            <table id="modal-gaianeighbor-table"
                   class="table table-sm table-hover">

             <thead>
               <tr>
                 <th>GAIA source id<br>&nbsp;</th>
                 <th>distance<br>[arcsec]</th>
                 <th>parallax<br>[mas]</th>
                 <th><em>G</em><br>[mag]</th>
                 <th><em>M<sub>G</sub></em><br>[mag]</th>
               </tr>
             </thead>

             <tbody id="gaia-neighbor-tbody">

             </tbody>

            </table>

          </div>
        </div>

      </div>


      <div class="tab-pane" id="mtab-varfeatures"
           role="tabpanel" aria-labelledby="modal-varfeatures">

        <div class="row mt-4 varfeatures-table-container">
          <div class="col-12">

            <table id="modal-varfeatures-table"
                   class="table table-sm table-striped">

             <thead>
               <tr>
                 <th>feature</th>
                 <th>value</th>
                 <th>description</th>
               </tr>
             </thead>

             <tbody id="varfeatures-tbody">

             </tbody>

            </table>

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


  // this runs a SIMBAD check
  simbad_check: function (objectid, collection) {

    let _xsrf = $('#objectinfo-edit-form > input[type="hidden"]').val();
    let postparams = {
      _xsrf: _xsrf,
      objectid: objectid,
      collection: collection,
      action:'simbad-check'
    };
    let posturl = '/api/object';

    $.post(posturl, postparams, function (data) {

      let result = data.result;
      let status = data.status;
      let message = data.message;
      let simbad_best_allids;

      if (status.indexOf('ok') != -1){

        if (result.simbad_best_allids !== null) {

          simbad_best_allids =
            result.simbad_best_allids
            .split('|').join(', ');
        }

        else {

          simbad_best_allids = '';

        }

        let formatted_simbad =
            '<em>closest distance</em>: ' +
            result.simbad_best_distarcsec.toFixed(2) +
            '&Prime;<br>' +
            '<em>closest object ID</em>: ' +
            result.simbad_best_mainid + '<br>' +
            '<em>closest object type</em>: ' +
            lcc_ui.bib_linkify(
              result.simbad_best_objtype
            ) + '<br>' +
            '<em>closest object other IDs</em>: ' +
            lcc_ui.bib_linkify(simbad_best_allids);

        $('#simbad-formatted-info').html(formatted_simbad);

      }
      else {
        let formatted_simbad = message;
        $('#simbad-formatted-info').html('<span class="text-warning">' +
                                         formatted_simbad +
                                         '</span>');
      }

    }, 'json').fail(function(xhr) {
      $('#simbad-formatted-info').html(
        '<span class="text-danger">Sorry, SIMBAD lookup ' +
          'failed because the LCC-Server denied access.</span>'
      );
    });

  },


  // this renders the main info table in the objectinfo modal and standalone
  // page
  render_infotable: function (currcp, collection, lcmagcols, cpstatus) {

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

      if ('telescope' in currcp.objectinfo) {

        $('#obsinfo').html(
          '<strong>' +
            currcp.objectinfo.observatory + ':</strong> ' +
            currcp.objectinfo.telescope + '<br>' +
            '<strong>LC points:</strong> ' + objndet
        );

      }

      else {

        $('#obsinfo').html(
          '<strong>' + currcp.objectinfo.observatory + '</strong><br>' +
            '<strong>LC points:</strong> ' + objndet
        );
      }

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
    else {
      simbad_ok = false;
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

      if (currcp.objectinfo.gaia_absolute_mags != null &&
          currcp.objectinfo.gaia_absolute_mags[0] != null) {
        gaiaabsmag = currcp.objectinfo.gaia_absolute_mags[0].toFixed(3);
      }
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
          '&Prime; &rarr; ' +
          '<a href="#" title="look at objectinfo for this neighbor" ' +
          'class="objectinfo-nbrlink" ' +
          'data-objectid="' + currcp.neighbors[0]['objectid'] +
          '" ' +
          'data-lcmagcols="' + lcmagcols + '" ' +
          'data-collection="' +  collection + '">' +
          'N1: ' + currcp.neighbors[0]['objectid'] +
          '</a><br>';
      }

      var formatted_gaia =
          '<strong><em>GAIA query failed</em>:</strong> ' +
          gaia_message;
      $('#gaia-neighbor-tbody').empty();

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

    var gi = 0;
    var gaia_x = 0.0;
    var gaia_y = 0.0;
    var rowhtml = '';
    var curr_gaia_dist = 0.0;
    var curr_gaia_parallax = 0.0;
    var curr_gaia_parallax_err = 0.0;
    var curr_gaia_mag = 0.0;
    var curr_gaia_absmag = 0.0;

    // add in the GAIA neighbors to the table
    if (gaia_ok) {

      // for each gaia neighbor, put in a table row
      for (gi; gi < currcp.objectinfo.gaia_ids.length; gi++) {

        // format the current object's GAIA info
        curr_gaia_dist = currcp.objectinfo.gaia_dists[gi];
        curr_gaia_parallax = currcp.objectinfo.gaia_parallaxes[gi];
        curr_gaia_parallax_err = currcp.objectinfo.gaia_parallax_errs[gi];
        curr_gaia_mag = currcp.objectinfo.gaia_mags[gi];
        curr_gaia_absmag = currcp.objectinfo.gaia_absolute_mags[gi];
        if (curr_gaia_dist !== null) {
          curr_gaia_dist = curr_gaia_dist.toFixed(3);
        }
        else {
          curr_gaia_dist = 'N/A';
        }
        if (curr_gaia_parallax !== null) {
          curr_gaia_parallax = curr_gaia_parallax.toFixed(3);
        }
        else {
          curr_gaia_parallax = 'N/A';
        }
        if (curr_gaia_parallax_err !== null) {
          curr_gaia_parallax_err = curr_gaia_parallax_err.toFixed(3);
        }
        else {
          curr_gaia_parallax_err = 'N/A';
        }
        if (curr_gaia_mag !== null) {
          curr_gaia_mag = curr_gaia_mag.toFixed(3);
        }
        else {
          curr_gaia_mag = 'N/A';
        }
        if (curr_gaia_absmag !== null) {
          curr_gaia_absmag = curr_gaia_absmag.toFixed(3);
        }
        else {
          curr_gaia_absmag = 'N/A';
        }

        // get the current object's xy position on the finder based on
        // GAIA coords
        if (currcp.objectinfo.gaia_xypos != null) {
          gaia_x = currcp.objectinfo.gaia_xypos[gi][0];
        }
        else {
          gaia_x = 0.0;
        }
        if (currcp.objectinfo.gaia_xypos != null) {
          gaia_y = currcp.objectinfo.gaia_xypos[gi][1];
        }
        else {
          gaia_y = 0.0;
        }


        // special formatting for the object itself
        if (gi == 0) {
          rowhtml = '<tr class="gaia-objectlist-row ' +
            'text-primary' +
            '" ' +
            'data-gaiaid="' +
            currcp.objectinfo.gaia_ids[gi] +
            '" data-xpos="' +
            gaia_x +
            '" data-ypos="' +
            gaia_y +
            '" >' +
            '<td>this object: ' +
            currcp.objectinfo.gaia_ids[gi] +
            '</td>' +
            '<td>' + curr_gaia_dist +
            '</td>' +
            '<td>' +
            curr_gaia_parallax +
            ' &plusmn; ' +
            curr_gaia_parallax_err +
            '</td>' +
            '<td>' +
            curr_gaia_mag +
            '</td>' +
            '<td>' +
            curr_gaia_absmag +
            '</td>' +
            '</tr>';
        }

        else {

          rowhtml = '<tr class="gaia-objectlist-row" ' +
            'data-gaiaid="' +
            currcp.objectinfo.gaia_ids[gi] +
            '" data-xpos="' +
            gaia_x +
            '" data-ypos="' +
            gaia_y +
            '" >' +
            '<td>' + currcp.objectinfo.gaia_ids[gi] +
            '</td>' +
            '<td>' +
            curr_gaia_dist +
            '</td>' +
            '<td>' +
            curr_gaia_parallax +
            ' &plusmn; ' +
            curr_gaia_parallax_err +
            '</td>' +
            '<td>' +
            curr_gaia_mag +
            '</td>' +
            '<td>' +
            curr_gaia_absmag +
            '</td>' +
            '</tr>';

        }

        $('#gaia-neighbor-tbody').append(rowhtml);

      }

    }

    // if GAIA xmatch failed, fill in the table without special
    // formatting if possible
    else if (currcp.objectinfo.gaia_ids != undefined) {

      // for each gaia neighbor, put in a table row
      gi = 0;

      // put in any rows of neighbors if there are any
      for (gi; gi < currcp.objectinfo.gaia_ids.length; gi++) {

        // format the current object's GAIA info
        curr_gaia_dist = currcp.objectinfo.gaia_dists[gi];
        curr_gaia_parallax = currcp.objectinfo.gaia_parallaxes[gi];
        curr_gaia_parallax_err = currcp.objectinfo.gaia_parallax_errs[gi];
        curr_gaia_mag = currcp.objectinfo.gaia_mags[gi];
        curr_gaia_absmag = currcp.objectinfo.gaia_absolute_mags[gi];
        if (curr_gaia_dist !== null) {
          curr_gaia_dist = curr_gaia_dist.toFixed(3);
        }
        else {
          curr_gaia_dist = 'N/A';
        }
        if (curr_gaia_parallax !== null) {
          curr_gaia_parallax = curr_gaia_parallax.toFixed(3);
        }
        else {
          curr_gaia_parallax = 'N/A';
        }
        if (curr_gaia_parallax_err !== null) {
          curr_gaia_parallax_err = curr_gaia_parallax_err.toFixed(3);
        }
        else {
          curr_gaia_parallax_err = 'N/A';
        }
        if (curr_gaia_mag !== null) {
          curr_gaia_mag = curr_gaia_mag.toFixed(3);
        }
        else {
          curr_gaia_mag = 'N/A';
        }
        if (curr_gaia_absmag !== null) {
          curr_gaia_absmag = curr_gaia_absmag.toFixed(3);
        }
        else {
          curr_gaia_absmag = 'N/A';
        }

        // get the current object's xy position on the finder based on
        // GAIA coords
        if (currcp.objectinfo.gaia_xypos != null) {
          gaia_x = currcp.objectinfo.gaia_xypos[gi][0];
        }
        else {
          gaia_x = 0.0;
        }

        if (currcp.objectinfo.gaia_xypos != null) {
          gaia_y = currcp.objectinfo.gaia_xypos[gi][1];
        }
        else {
          gaia_y = 0.0;
        }

        rowhtml = '<tr class="gaia-objectlist-row" ' +
          'data-gaiaid="' +
          currcp.objectinfo.gaia_ids[gi] +
          '" data-xpos="' +
          gaia_x +
          '" data-ypos="' +
          gaia_y +
          '" >' +
          '<td>' + currcp.objectinfo.gaia_ids[gi] +
          '</td>' +
          '<td>' +
          curr_gaia_dist +
          '</td>' +
          '<td>' +
          curr_gaia_parallax +
          ' &plusmn; ' +
          curr_gaia_parallax_err +
          '</td>' +
          '<td>' +
          curr_gaia_mag +
          '</td>' +
          '<td>' +
          curr_gaia_absmag +
          '</td>' +
          '</tr>';
        $('#gaia-neighbor-tbody').append(rowhtml);

      }

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

    var formatted_simbad = simbad_message;
    if (simbad_ok) {

      var simbad_best_allids =
          currcp.objectinfo.simbad_best_allids
          .split('|').join(', ');

      formatted_simbad =
        '<strong><em>matching objects</em>:</strong> ' +
        '<em>closest distance</em>: ' +
        currcp.objectinfo.simbad_best_distarcsec.toFixed(2) +
        '&Prime;<br>' +
        '<em>closest object ID</em>: ' +
        currcp.objectinfo.simbad_best_mainid + '<br>' +
        '<em>closest object type</em>: ' +
        lcc_ui.bib_linkify(
          currcp.objectinfo.simbad_best_objtype
        ) + '<br>' +
        '<em>closest object other IDs</em>: ' +
        lcc_ui.bib_linkify(simbad_best_allids);

    }

    // if the current checkplot's status allows SIMBAD updates and the
    // SIMBAD status is failed, render the SIMBAD update controls
    else if (!simbad_ok && cpstatus.indexOf('sc-ok') != -1) {

      formatted_simbad = `
<div class="row">
<div class="col-12">
No SIMBAD information found for this object.
</div>
</div>
<div class="form-row mt-1 justify-content-center">
<form class="form"
 id="simbad-lookup-form"
 action="/api/object"
 data-objectid=${currcp.objectid}
 data-collection=${collection}
 method="POST">
<button type="submit" class="btn btn-secondary btn-sm">Retry SIMBAD query</button>
</form>
</div>
`;

    }

    $('#objectinfo-extra')
      .append(
        '<tr>' +
          '<th>SIMBAD information</th>' +
          '<td id="simbad-formatted-info">' +
          formatted_simbad +
          '</td>' +
          '</tr>'
      );

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


      // add in the variability features if they exist to the variability
      // features table
      $('.varfeatures-tbody').empty();

      if ('features' in currcp.varinfo) {

        var varfeatures = currcp.varinfo.features;
        var features_table = `
<tr>
  <td><code>amplitude</code></td>
  <td>${varfeatures.amplitude.toFixed(3)}</td>
  <td>Amplitude of variability of the magnitude time-series</td>
</tr>
<tr>
  <td><code>beyond1std</code></td>
  <td>${varfeatures.beyond1std.toFixed(3)}</td>
  <td>Fraction of observations beyond 1-stdev of the magnitude time-series</td>
</tr>
<tr>
  <td><code>eta</code></td>
  <td>${(1.0/(varfeatures.eta_normal)).toFixed(3)}</td>
  <td>The eta-inverse variability index of the magnitude time-series</td>
</tr>
<tr>
  <td><code>kurtosis</code></td>
  <td>${varfeatures.kurtosis.toFixed(3)}</td>
  <td>Distribution kurtosis of the magnitude time-series</td>
</tr>
<tr>
  <td><code>linear_fit_slope</code></td>
  <td>${varfeatures.linear_fit_slope.toFixed(3)}</td>
  <td>The slope of a linear fit to the magnitude time-series</td>
</tr>
<tr>
  <td><code>mad</code></td>
  <td>${varfeatures.mad.toFixed(3)}</td>
  <td>Median absolute deviation of the magnitude time-series</td>
</tr>
<tr>
  <td><code>mags_iqr</code></td>
  <td>${varfeatures.mag_iqr.toFixed(3)}</td>
  <td>Interquartile range (75-25) of the magnitude time-series</td>
</tr>
<tr>
  <td><code>median</code></td>
  <td>${varfeatures.median.toFixed(3)}</td>
  <td>Median of the magnitude time-series</td>
</tr>
<tr>
  <td><code>ndetobslength_ratio</code></td>
  <td>${varfeatures.ndetobslength_ratio.toFixed(3)}</td>
  <td>Ratio of the number of observations to the length of observations</td>
</tr>
<tr>
  <td><code>skew</code></td>
  <td>${varfeatures.skew.toFixed(3)}</td>
  <td>Distribution skew of the magnitude time-series</td>
</tr>
<tr>
  <td><code>stdev</code></td>
  <td>${varfeatures.stdev.toFixed(3)}</td>
  <td>Standard deviation of the magnitude time-series</td>
</tr>
<tr>
  <td><code>stetsonj</code></td>
  <td>${varfeatures.stetsonj.toFixed(3)}</td>
  <td>The Stetson J variability index of the magnitude time-series</td>
</tr>
<tr>
  <td><code>stetsonk</code></td>
  <td>${varfeatures.stetsonk.toFixed(3)}</td>
  <td>The Stetson K variability index of the magnitude time-series</td>
</tr>
<tr>
  <td><code>timelength</code></td>
  <td>${varfeatures.timelength.toFixed(3)}</td>
  <td>Time difference in days between the last and first observation</td>
</tr>
<tr>
  <td><code>wmean</code></td>
  <td>${varfeatures.wmean.toFixed(3)}</td>
  <td>Weighted mean of the magnitude time-series</td>
</tr>
`;
        $('#varfeatures-tbody').html(features_table);

      }

      else {
        $('.varfeatures-tbody')
          .html('<tr>' +
                '<td></td>' +
                '<td></td>' +
                '<td>No variability features found for this object.' +
                '</td>' +
                '</tr>');
      }


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
          '<td>' + lcc_ui.bib_linkify(currcp.objectcomments) + '</td></tr>'
      );

    }


  },

  render_pfresult: function (currcp,
                             cpstatus) {

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
          currcp[pfm].periodogram.replace(/null/g,'NaN') +
          '" ' +
          'class="img-fluid zoomable-tile" id="periodogram-' +
          pfm + '"></div>';

        col2 = '<div class="col-3 px-0">' +
          '<img src="data:image/png;base64,' +
          currcp[pfm]['phasedlc0']['plot'].replace(/null/g,'NaN') +
          '" ' +
          'class="img-fluid zoomable-tile" id="phasedlc-0-' +
          pfm + '"></div>';

        col3 = '<div class="col-3 px-0">' +
          '<img src="data:image/png;base64,' +
          currcp[pfm]['phasedlc1']['plot'].replace(/null/g,'NaN') +
          '" ' +
          'class="img-fluid zoomable-tile" id="phasedlc-1-' +
          pfm + '"></div>';

        col4 = '<div class="col-3 px-0">' +
          '<img src="data:image/png;base64,' +
          currcp[pfm]['phasedlc2']['plot'].replace(/null/g,'NaN') +
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
  get_object_info: function (collection,
                             lcmagcols,
                             objectid,
                             target,
                             separatepage) {

    // we'll hit the objectinfo API for info on this object
    var geturl = '/api/object';
    var params = {objectid: objectid,
                  collection: collection,
                  lcmagcols: lcmagcols};

    // put in a message saying we're getting info
    $(target)
      .html('<div class="row"><div class="col-12">' +
            '<h6>Looking up this object...</h6></div></div>');

    $.getJSON(geturl, params, function (data) {

      var result = data.result;
      var status = data.status;

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
      lcc_objectinfo.render_infotable(result,
                                      collection,
                                      lcmagcols,
                                      status);

      // render the object's lightcurve download link if we're in separate
      // page mode. also render the object's collection and title
      if (separatepage != undefined && separatepage == true) {

        $('.lc-download-link').html(
          '<a rel="nofollow" class="btn btn-primary" ' +
            'href="/l/' + collection.replace(/_/g,'-') +
            '/' + objectid + '-csvlc.gz" download="' +
            objectid + '-csvlc.gz' +
            '">Download light curve</a>'
        );

        $('.objectinfo-header')
          .addClass('mt-2')
          .html('<div class="col-12"><h2>' + objectid +
                ' in collection <code>' +
                collection.replace(/-/g,'_') + '</code></h2>');

      }

      // add in the object's phased LCs from all available PFMETHODS
      lcc_objectinfo.render_pfresult(result,
                                     status);


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
