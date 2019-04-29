/*global $, ui, Math */

/*
  vizinspect-admin.js - Waqas Bhatti (wbhatti@astro.princeton.edu) - Mar 2019
  License: MIT. See the LICENSE file for details.

  This contains JS to drive the server's admin interface.

*/

var assignments = {

  unassigned_objects: [],
  unassigned_start_keyid: 0,
  unassigned_end_keyid: null,
  assigned_objects: [],
  assigned_start_keyid: 0,
  assigned_end_keyid: null,

  // this fetches the lists of assigned and unassigned objects
  review_assignment_lists: function (unassigned_start_keyid,
                                     unassigned_end_keyid,
                                     assigned_start_keyid,
                                     assigned_end_keyid) {

    let url = `/api/review-assign?unassigned_start_keyid=${unassigned_start_keyid}&unassigned_end_keyid=${unassigned_end_keyid}&assigned_start_keyid=${assigned_start_keyid}&assigned_end_keyid=${assigned_end_keyid}`;

    $.getJSON(url, function (data) {

      let status = data.status;
      let result = data.result;
      let message = data.message;

      if (status == 'ok') {

        // update the lists
        assignments.unassigned_objects = result.unassigned_objects;
        assignments.unassigned_start_keyid = result.unassigned_start_keyid;
        assignments.unassigned_end_keyid = result.unassigned_end_keyid;

        assignments.assigned_objects = result.assigned_objects;
        assignments.assigned_start_keyid = result.assigned_start_keyid;
        assignments.assigned_end_keyid = result.assigned_end_keyid;

        // update the controls for unassigned objects
        let $unassigned_select = $('#objectid-reviewlist');

        $unassigned_select.empty();

        for (let objectid of assignments.unassigned_objects) {

          $unassigned_select.append(
            `<option value="${objectid}">Object ID: ${objectid}</option>`
          );

        }

        // for each user ID that has assigned objects, update their select
        // controls as well
        for (let userid in assignments.assigned_objects) {

          let this_user_assigned = assignments.assigned_objects[userid];

          // look up the select associated with this userid
          let $userid_select = $(`#assigned-reviewlist-userid-${userid}`);
          $userid_select.empty();

          for (let objectid of this_user_assigned) {
            $userid_select.append(
              `<option value="${objectid}">Object ID: ${objectid}</option>`
            );
          }

        }

      }

      else {
        ui.alert_box(message, 'danger');
      }

    }).fail(function (xhr) {

      let message =
          'Could not fetch lists of assigned/un-assigned objects, ' +
          'something went wrong with the server backend.';

      if (xhr.status == 500) {
        message = 'Something went wrong with the server backend ' +
          ' while trying to get assigned/un-assigned object lists.';
      }

      ui.alert_box(message, 'danger');

    });

  },


  // this adds to the assignment list for a user ID
  assign_objects_to_userid: function (userid) {

    // look up the select associated with this userid
    let $userid_select = $(`#assigned-reviewlist-userid-${userid}`);

    // look up the select for the unassigned objects
    let $unassigned_select = $('#objectid-reviewlist');
    let selected_objects = $unassigned_select.val();

    let posturl = '/api/review-assign';
    let _xsrf = $('#admin-review-assign-update-form > input[type="hidden"]').val();
    let postparams = {
      _xsrf:_xsrf,
      userid: userid,
      assigned_objects: selected_objects
    };

    $.post(posturl, postparams, function (data) {

      let status = data.status;
      let result = data.result;
      let message = data.message;

      if (status == 'ok') {

        assignments.review_assignment_lists(
          assignments.unassigned_start_keyid,
          assignments.unassigned_end_keyid,
          assignments.assigned_start_keyid,
          assignments.assigned_end_keyid
        );

      }

      else {

        ui.alert_box(message, 'danger');

      }

    }, 'json').fail( function (xhr) {

      let message =
          'Could not fetch lists of assigned/un-assigned objects, ' +
          'something went wrong with the server backend.';

      if (xhr.status == 500) {
        message = 'Something went wrong with the server backend ' +
          ' while trying to get assigned/un-assigned object lists.';
      }

      ui.alert_box(message, 'danger');

    });

  },


  // this removes objects from the assignment list for a user ID
  unassign_objects_from_userid: function (userid, unassigned_objects) {

    // look up the select associated with this userid
    let $userid_select = $(`#assigned-reviewlist-userid-${userid}`);
    let selected_objects = $userid_select.val();


  }

};


var admin = {

  // this sets up the admin form actions
  action_setup: function () {

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
        allowedemailaddr: $('#admin-allowed-email-addrs').val(),
      };

      $.post(posturl, postparams, function (data) {

        var status = data.status;
        var result = data.result;
        var message = data.message;

        // if something broke, alert the user
        if (status != 'ok' || result === null || result.length == 0) {
          ui.alert_box(message, 'danger');
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

          $('#admin-allowed-email-addrs').val(
            result.allowed_email_addrs.join(', ')
          );

          ui.alert_box(message, 'info');

        }


      }, 'json').fail(function (xhr) {

        var message =
            'Could not update email or sign-up/in settings, ' +
            'something went wrong with the server backend.';

        if (xhr.status == 500) {
          message = 'Something went wrong with the server backend ' +
            ' while trying to update email/sign-up/in settings.';
        }

        ui.alert_box(message, 'danger');

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
          ui.alert_box(message, 'danger');
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

          ui.alert_box(message, 'info');

        }

      }, 'json').fail(function (xhr) {

        var message = 'Could not update user information, ' +
            'something went wrong with the server backend.';

        if (xhr.status == 500) {
          message = 'Something went wrong with the server backend ' +
            ' while trying to update user information.';
        }
        else if (xhr.status == 400) {
          message = 'Invalid input provided in the user ' +
            ' update form. Please check and try again.';
        }

        ui.alert_box(message, 'danger');

      });

    });


  }

};
