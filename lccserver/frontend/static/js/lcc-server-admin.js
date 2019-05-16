/*global $, lcc_ui */

/*
  lcc-server-admin.js - Waqas Bhatti (wbhatti@astro.princeton.edu) - Mar 2019
  License: MIT. See the LICENSE file for details.

  This contains JS to drive the LCC server's admin interface.

*/

var lcc_admin = {

  // this is like the usual lcc_ui.get_recent_datasets function, but adds in
  // controls for deleting/changing visibility/changing owners of the
  // datasets.
  get_recent_datasets: function () {


  },

  // this sets up the admin form actions
  action_setup: function () {

    // handle the site settings update form
    $('#admin-site-update-form').on('submit', function (evt) {

      evt.preventDefault();

      var posturl = '/admin/site';
      var _xsrf = $('#admin-site-update-form > input[type="hidden"]').val();
      var postparams = {
        _xsrf:_xsrf,
        projectname: $('#projectname').val(),
        projectlink: $('#projectlink').val(),
        institution: $('#institution').val(),
        institutionlink: $('#institutionlink').val(),
        institutionlogo: $('#institutionlogo').val(),
        department: $('#department').val(),
        departmentlink: $('#departmentlink').val(),

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
          $('#projectname').val(result.project);
          $('#projectlink').val(result.project_link);
          $('#institution').val(result.institution);
          $('#institutionlink').val(result.institution_link);
          $('#institutionlogo').val(result.institution_logo);
          $('#department').val(result.department);
          $('#departmentlink').val(result.department_link);

          lcc_ui.alert_box(message, 'info');

        }

      }, 'json').fail(function (xhr) {

        var message = 'Could not update site settings, ' +
            'something went wrong with the LCC server backend.';

        if (xhr.status == 500) {
          message = 'Something went wrong with the LCC-Server backend ' +
            ' while trying to update site settings.';
        }
        else if (xhr.status == 400) {
          message = 'Invalid input provided in the site-settings ' +
            ' update form. Please check and try again.';
        }

        lcc_ui.alert_box(message, 'danger');

      });

    });


    // handle the email and signups form update
    $('#admin-email-update-form').on('submit', function (evt) {

      evt.preventDefault();

      var posturl = '/admin/email';
      var _xsrf = $('#admin-email-update-form > input[type="hidden"]').val();
      var postparams = {
        _xsrf:_xsrf,
        loginradio: $('input[name="loginradio"]:checked').val(),
        signupradio: $('input[name="signupradio"]:checked').val(),
        emailsender: $('#emailsender').val(),
        emailserver: $('#emailserver').val(),
        emailport: $('#emailport').val(),
        emailuser: $('#emailuser').val(),
        emailpass: $('#emailpass').val(),
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

          if (result.logins_allowed === true) {

            $('#loginradio-yes').prop('checked',true);

          }
          else {

            $('#loginradio-no').prop('checked',true);

          }

          if (result.signups_allowed === true) {

            $('#signupradio-yes').prop('checked',true);

          }
          else {

            $('#signupradio-no').prop('checked',true);

          }

          // update the rest of the controls
          $('#emailsender').val(result.email_sender);
          $('#emailserver').val(result.email_server);
          $('#emailport').val(result.email_port);
          $('#emailuser').val(result.email_user);
          $('#emailpass').val(result.email_pass);

          lcc_ui.alert_box(message, 'info');

        }


      }, 'json').fail(function (xhr) {

        var message =
            'Could not update email or sign-up/in settings, ' +
            'something went wrong with the LCC server backend.';

        if (xhr.status == 500) {
          message = 'Something went wrong with the LCC-Server backend ' +
            ' while trying to update email/sign-up/in settings.';
        }

        lcc_ui.alert_box(message, 'danger');

      });

    });


    // handle the site settings update form
    $('.admin-user-update-btn').on('click', function (evt) {

      evt.preventDefault();

      // find the updated values
      let this_userid = $(this).attr('data-userid');

      let updated_emailaddr =
          $('#userlist-email-id' + this_userid).val();
      let updated_fullname =
          $('#userlist-fullname-id' + this_userid).val();

      if (updated_fullname.trim().length == 0) {
        updated_fullname = null;
      }

      let updated_role =
          $('#userlist-role-id' + this_userid).val();

      var posturl = '/admin/users';
      var _xsrf = $('#admin-users-update-form > input[type="hidden"]').val();
      var postparams = {
        _xsrf:_xsrf,
        updated_email: updated_emailaddr,
        updated_fullname: updated_fullname,
        updated_role: updated_role,
        target_userid: parseInt(this_userid)
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
          $('#userlist-email-id' + this_userid).val(
            result.email
          );
          $('#userlist-fullname-id' + this_userid).val(
            result.full_name
          );
          $('#userlist-role-id' + this_userid).val(
            result.user_role
          );

          lcc_ui.alert_box(message, 'info');

        }

      }, 'json').fail(function (xhr) {

        var message = 'Could not update user information, ' +
            'something went wrong with the LCC server backend.';

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


  }

};
