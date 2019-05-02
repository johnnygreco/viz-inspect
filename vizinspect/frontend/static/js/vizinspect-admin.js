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
  unassigned_page: null,
  unassigned_objectcount: null,
  unassigned_npages: null,
  unassigned_rows_per_page: null,

  assigned_objects: {},
  assigned_start_keyid: {},
  assigned_end_keyid: {},
  assigned_page: {},
  assigned_objectcount: {},
  assigned_npages: {},
  assigned_rows_per_page: {},


  // this fetches the lists of assigned or unassigned objects
  review_assignment_list: function (list_type, list_page, user_id) {

    let get_user_id = 'all';
    if (user_id !== undefined) {
      get_user_id = user_id;
    }

    let url = `/api/review-assign?list_type=${list_type}&list_page=${list_page}&user_id=${get_user_id}`;

    $.getJSON(url, function (data) {

      let status = data.status;
      let result = data.result;
      let message = data.message;

      if (status == 'ok') {

        // updating the unassigned objects
        if (list_type == 'unassigned') {

          assignments.unassigned_objects = result.object_list;
          assignments.unassigned_start_keyid = result.start_keyid;
          assignments.unassigned_end_keyid = result.end_keyid;

          assignments.unassigned_page = result.curr_page;
          assignments.unassigned_npages = result.n_pages;
          assignments.unassigned_objectcount = result.object_count;
          assignments.unassigned_rows_per_page = result.rows_per_page;

          // update the controls for unassigned objects
          let $unassigned_select = $('#objectid-reviewlist');
          $unassigned_select.empty();

          // update the current page number
          $('#current-list-page-unassigned').val(assignments.unassigned_page);

          for (let objectid of assignments.unassigned_objects) {

            $unassigned_select.append(
              `<option value="${objectid}">Object ID: ${objectid}</option>`
            );

          }

        }

        // updating all user IDs
        else if (list_type == 'assigned' && user_id === undefined) {

          for (let userid in result) {

            assignments.assigned_objects[userid] = result[userid].object_list;
            assignments.assigned_start_keyid[userid] = result[userid].start_keyid;
            assignments.assigned_end_keyid[userid] = result[userid].end_keyid;
            assignments.assigned_page[userid] = result[userid].curr_page;
            assignments.assigned_npages[userid] = result[userid].n_pages;
            assignments.assigned_objectcount[userid] = result[userid].object_count;
            assignments.assigned_rows_per_page[userid] = result[userid].rows_per_page;

            let $page_control = $('.current-list-page-assigned').filter(
              `[data-userid='${userid}']`
            );
            $page_control.val(assignments.assigned_page[userid]);

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

        // updating a single user_id
        else if (list_type == 'assigned' && user_id !== undefined) {

          assignments.assigned_objects[user_id] = result[user_id].object_list;
          assignments.assigned_start_keyid[user_id] = result[user_id].start_keyid;
          assignments.assigned_end_keyid[user_id] = result[user_id].end_keyid;
          assignments.assigned_page[user_id] = result[user_id].curr_page;
          assignments.assigned_npages[user_id] = result[user_id].n_pages;
          assignments.assigned_objectcount[user_id] = result[user_id].object_count;
          assignments.assigned_rows_per_page[user_id] = result[user_id].rows_per_page;

          let $page_control = $('.current-list-page-assigned').filter(
            `[data-user_id='${user_id}']`
          );
          $page_control.val(assignments.assigned_page[user_id]);

          let this_user_assigned = assignments.assigned_objects[user_id];

          // look up the select associated with this user_id
          let $user_id_select = $(`#assigned-reviewlist-user_id-${user_id}`);
          $user_id_select.empty();

          for (let objectid of this_user_assigned) {
            $user_id_select.append(
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
      assigned_objects: JSON.stringify(selected_objects)
    };

    $.post(posturl, postparams, function (data) {

      let status = data.status;
      let result = data.result;
      let message = data.message;

      if (status == 'ok') {

        // if the assignment succeeds, update the view to show the new state
        assignments.review_assignment_list(
          'unassigned',
          assignments.unassigned_page,
        );
        assignments.review_assignment_list(
          'assigned',
          assignments.assigned_page[userid],
          userid
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
  unassign_objects_from_userid: function (userid) {

    // look up the select associated with this userid
    let $userid_select = $(`#assigned-reviewlist-userid-${userid}`);
    let selected_objects = $userid_select.val();

    let posturl = '/api/review-assign';
    let _xsrf = $('#admin-review-assign-update-form > input[type="hidden"]').val();
    let postparams = {
      _xsrf:_xsrf,
      userid: userid,
      assigned_objects: JSON.stringify(selected_objects),
      unassign_flag: 1
    };

    $.post(posturl, postparams, function (data) {

      let status = data.status;
      let result = data.result;
      let message = data.message;

      if (status == 'ok') {

        // if the unassignment succeeds, update the view to show the new state
        assignments.review_assignment_list(
          'unassigned',
          assignments.unassigned_page,
        );
        assignments.review_assignment_list(
          'assigned',
          assignments.assigned_page[userid],
          userid
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

  }

};


var admin = {

  // this sets up the admin form actions
  action_setup: function () {

    // handle the assign button
    $('#review-assign-objects').on('click', function (evt) {

      evt.preventDefault();

      let userid = parseInt($('#assign-objects-userid').val());

      if (!isNaN(userid)) {

        assignments.assign_objects_to_userid(userid);

      }

    });

    // handle the unassign button
    $('.review-unassign-objects').on('click', function (evt) {

      evt.preventDefault();

      let userid = parseInt($(this).attr('data-userid'));

      if (!isNaN(userid)) {

        assignments.unassign_objects_from_userid(userid);

      }


    });


    // handle the next unassigned page link
    $('#next-list-page-unassigned').on('click', function (evt) {

      if (assignments.unassigned_page < assignments.unassigned_npages) {
        ui.debounce(
          assignments.review_assignment_list(
            'unassigned',
            assignments.unassigned_page + 1
          ),
          100
        );
      }

    });

    // handle the prev unassigned page link
    $('#prev-list-page-unassigned').on('click', function (evt) {

      if (assignments.unassigned_page > 0) {
        ui.debounce(
          assignments.review_assignment_list(
            'unassigned',
            assignments.unassigned_page - 1
          ),
          100
        );
      }

    });

    // handle the jump to button for unassigned objects
    $('#jump-to-list-page-unassigned').on('click', function (evt) {

      let page_to_jump_to = $('#current-list-page-unassigned').val();

      if (page_to_jump_to >= 0 && page_to_jump_to < assignments.unassigned_npages) {
        ui.debounce(
          assignments.review_assignment_list(
            'unassigned',
            page_to_jump_to
          ),
          100
        );
      }

    });

    // handle enter key in the jump box for unassigned objects
    $('#current-list-page-unassigned').on('keyup', function (evt) {

      if (evt.keyCode == 13) {

        let page_to_jump_to = $(this).val();

        if (page_to_jump_to >= 0 && page_to_jump_to < assignments.unassigned_npages) {
          ui.debounce(
            assignments.review_assignment_list(
              'unassigned',
              page_to_jump_to
            ),
            100
          );
        }

      }

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
            result.allowed_user_emailaddr.join(', ')
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
