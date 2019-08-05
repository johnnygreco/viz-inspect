/*global $, moment, oboe, setTimeout, clearTimeout, Image, Cookies, localStorage */

/*
  vizinspect.js - Waqas Bhatti (wbhatti@astro.princeton.edu) - Jun 2018
  License: MIT. See the LICENSE file for details.

  This contains JS to drive the interface elements.

*/

var ui = {

  // this holds imagedata for the canvas so we can restore changed parts of
  // the image
  pixeltracker: null,

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
  b64_to_canvas: function (str, targetelem, xpix, ypix) {

    var datauri = 'data:image/png;base64,' + str;
    var newimg = new Image();
    var canvas = document.getElementById(targetelem.replace('#',''));

    var imgheight = xpix;
    var imgwidth = ypix;
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
          `<a target="_blank" rel="noopener noreferer" href="https://ui.adsabs.harvard.edu/#abs/${match}/abstract">${match}</a>`);
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
          `<a target="_blank" rel="noopener noreferer" href="https://dx.doi.org/${match.replace(/doi:/g,'')}">${match}</a>`);
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

    let one = ui.bibcode_linkify(text);
    let two = ui.doi_linkify(one);

    return two;

  },


  // this loads UI preferences from the vizinspect_prefs cookie
  // target is one of 'main-page', 'prefs-page' to switch between the controls
  // to set
  load_cookie_prefs: function (target) {

    let prefs = Cookies.getJSON('vizinspect_prefs');
    return prefs;

  },


  // this wires up all the controls
  action_setup: function () {

    /////////////////////////
    // USER PREFS BINDINGS //
    /////////////////////////

    // delete the API key on session end
    $('#user-logout-form').on('submit', function(evt) {
      localStorage.clear();
    });

    //////////////////////
    // CONTROL BINDINGS //
    //////////////////////

    // handle the next object link
    $('#next-object-link').on('click', function (evt) {

      review.get_next_object();

    });

    // handle the previous object link
    $('#prev-object-link').on('click', function (evt) {

      review.get_prev_object();

    });

    // handle the jump button
    $('#jump-source-index').on('click', function (evt) {

      let jump_to = parseInt($('#current-source-index').val());

      if (!isNaN(jump_to) && (jump_to > 0) && (jump_to < review.total_objectcount)) {

        // see if we should also change the objectlist to all
        if (review.objectlist.indexOf(jump_to) == -1) {

          review.get_object_list(
              'all',
              'start',
              1,
              false,
              0
          );

        }

        // fire the get object function
        ui.debounce(review.get_object(jump_to), 200);

      }

    });

    // handle enter key in the jump box
    $('#current-source-index').on('keyup', function (evt) {

      if (evt.keyCode == 13) {

        let jump_to = parseInt($('#current-source-index').val());

        if (!isNaN(jump_to) && (jump_to > 0) && (jump_to < review.total_objectcount)) {

          // see if we should also change the objectlist to all
          if (review.objectlist.indexOf(jump_to) == -1) {

            let list_page = parseInt(jump_to/review.current_rows_per_page);
            review.get_object_list(
              'all',
              'start',
              1,
              false,
              0
            );

          }

          // fire the get object function
          ui.debounce(review.get_object(jump_to), 200);

        }

      }

    });

    // handle clicking on a link in the objectid list
    $('#objectid-list').on('click','.objectid-link', function (evt) {

      evt.preventDefault();

      let this_objectid = $(this).attr('data-objectid');
      ui.debounce(review.get_object(this_objectid), 200);

    });

    // handle selecting a object-view type
    // this auto-loads the first object in the specified list
    $('#objectlist-pref-select').on('change', function (evt) {

      let selected = $(this).val();

      if (selected === 'complete-good') {
        ui.debounce(
          review.get_object_list(
            'complete-good',
            'start',
            1,
            'first',
            0
          ),
          100
        );
      }
      else if (selected === 'complete-bad') {
        ui.debounce(
          review.get_object_list(
            'complete-bad',
            'start',
            1,
            'first',
            0
          ),
          100
        );
      }
      else if (selected === 'incomplete') {
        ui.debounce(
          review.get_object_list(
            'incomplete',
            'start',
            1,
            'first',
            0
          ),
          100
        );
      }
      else if (selected === 'self-incomplete') {
        ui.debounce(
          review.get_object_list(
            'self-incomplete',
            'start',
            1,
            'first',
            0
          ),
          100
        );
      }
      else if (selected === 'other-incomplete') {
        ui.debounce(
          review.get_object_list(
            'other-incomplete',
            'start',
            1,
            'first',
            0
          ),
          100
        );
      }
      else if (selected === 'self-complete-good') {
        ui.debounce(
          review.get_object_list(
            'self-complete-good',
            'start',
            1,
            'first',
            0
          ),
          100
        );
      }
      else if (selected === 'self-complete-bad') {
        ui.debounce(
          review.get_object_list(
            'self-complete-bad',
            'start',
            1,
            'first',
            0
          ),
          100
        );
      }
      else {
        ui.debounce(
          review.get_object_list(
            'all',
            'start',
            1,
            'first',
            0
          ),
          100
        );
      }

    });


    $('#next-list-page').on('click', function (evt) {

      if (review.current_page < review.current_npages) {
        ui.debounce(
          review.get_object_list(
            review.current_list_reviewstatus,
            'start',
            review.objectlist_end_keyid,
            'first',
            review.current_page + 1
          ),
          100
        );
      }

    });


    $('#prev-list-page').on('click', function (evt) {

      if (review.current_page > 0) {
        ui.debounce(
          review.get_object_list(
            review.current_list_reviewstatus,
            'end',
            review.objectlist_start_keyid,
            'last',
            review.current_page - 1
          ),
          100
        );
      }

    });


    //////////////////////////////////
    // COMMENT FORM SUBMIT BINDINGS //
    //////////////////////////////////

    // bind the click event for the flag buttons
    // we now always jump to the next object on clicking a button
    $('#comment-form').on('click', '.object-flags-button', function (evt) {

      // set the state of this button
      $(this).attr('data-state','active');

      // get the current objectid
      let this_objectid = review.current_objectid;

      // fire the save object handler
      ui.debounce(review.save_object_comments_flags(this_objectid, true), 200);

    });


    // bind the form submit for the review
    $('#comment-form').on('submit', function (event) {

      event.preventDefault();

      // get the current objectid
      let this_objectid = review.current_objectid;

      // fire the save object handler
      ui.debounce(review.save_object_comments_flags(this_objectid), 200);

    });

  }


};


// this contains functions to drive the main controls and send the requests to
// the backend
var review = {

  current_objectid: null,
  current_keyid: null,
  current_object_reviewstatus: null,

  objectlist: null,
  objectlist_start_keyid: 1,
  objectlist_end_keyid: 100,

  current_page: 0,
  current_objectcount: null,
  current_npages: null,
  current_rows_per_page: null,
  current_list_reviewstatus: null,

  // this fetches the full object list. if load_object is true, will load
  // the appropriate object in the list right after fetching the list
  get_object_list: function (review_status,
                             keytype,
                             keyid,
                             load_object,
                             set_page_to) {

    let url = `/api/list-objects?review_status=${review_status}&keytype=${keytype}&keyid=${keyid}`;

    $.getJSON(url, function (data) {

      let status = data.status;
      let result = data.result;
      let message = data.message;

      // if all's good, set the current object list and update the page
      if (status == 'ok') {

        review.objectlist = result.objectlist;
        review.objectlist_start_keyid = result.start_keyid;
        review.objectlist_end_keyid = result.end_keyid;
        review.current_objectcount = result.object_count;
        review.current_npages = result.n_pages;
        review.current_rows_per_page = result.rows_per_page;
        review.current_list_reviewstatus = review_status;

        // special case of first load
        if (review_status == 'all' && keytype == 'start' && keyid == 1) {
          review.total_npages = result.n_pages;
          review.total_objectcount = result.object_count;
        }

        // populate the object list on the page

        $('#objectid-list').empty();
        for (let objectid of review.objectlist) {

          let this_elem =
              `<li><a href="#" class="objectid-link" data-objectid="${objectid}">${objectid}</a></li>`;
          $('#objectid-list').append(this_elem);

        }

        $('#current-list-page').val(review.current_page);
        $('#current-list-npages').html(review.current_npages);

      }

      // if we failed, alert the user
      else {

        ui.alert_box(message, "danger");

      }

    }).done(function (xhr) {

      // get the appropriate object if load_object is not undefined
      if (load_object === 'first') {

        review.get_object(review.objectlist[0]);

      }

      else if (load_object === 'last') {

        review.get_object(review.objectlist[review.objectlist.length-1]);

      }

      // update the object list type
      let index_label = 'Current (browsing all objects)';

      if (review_status === 'complete-good') {
        index_label = 'Current (in all closed good objects)';
      }
      else if (review_status === 'complete-bad') {
        index_label = 'Current (in all closed bad objects)';
      }
      else if (review_status === 'incomplete') {
        index_label = 'Current (in objects still open for votes)';
      }
      else if (review_status === 'self-complete-good') {
        index_label = 'Current (in closed good objects with your vote)';
      }
      else if (review_status === 'self-complete-bad') {
        index_label = 'Current (in closed bad objects with your vote)';
      }
      else if (review_status === 'self-incomplete') {
        index_label = 'Current (in open objects with your vote)';
      }
      else if (review_status === 'other-incomplete') {
        index_label = 'Current (in open objects without your vote)';
      }
      $('#current-index-label').html(index_label);


    }).done(function () {

      if (set_page_to !== undefined) {
        review.current_page = parseInt(set_page_to);
      }

      $('#current-pagenum').html(review.current_page + 1);

    }).fail( function (xhr) {

      ui.alert_box("Could not load object list from the backend.", "danger");

    });

  },


  get_object: function (source_index) {

    let url = `/api/load-object/${source_index}`;

    // set up a spinner
    $('#spinner-block').html(
      '<div class="spinner-border" role="status">' +
        '<span class="sr-only">Loading...</span>' +
        '</div>'
    );

    // fire the request to the backend
    $.getJSON(url, function(data) {

      let status = data.status;
      let result = data.result;
      let message = data.message;

      if (status == 'ok') {

        let objectinfo = result.info;
        let comments = result.comments;
        let object_review_status = result.review_status;
        let object_already_reviewed = result.already_reviewed;

        // update the status
        review.current_objectid = objectinfo.objectid;
        review.current_keyid = objectinfo.keyid;
        review.current_object_reviewstatus = object_review_status;

        // update the plot. this gets it straight from the CDN (probably,
        // explains why it's fast now)
        $('#galaxy-main-plot').attr(
          'src',
          'https://hugs.johnnygreco.space/hugs-' +
            objectinfo.objectid + '.png'
        );
        $('#galaxy-main-plot').attr('data-sourceindex', objectinfo.objectid);
        $('#galaxy-main-plot').attr('data-ra', objectinfo.ra);
        $('#galaxy-main-plot').attr('data-dec', objectinfo.dec);

        // clean out the comment box
        $('#object-notes').val('');

        // update the objectid and keyid
        $('#current-source-index').val(objectinfo.objectid);
        $('#current-source-index').attr('data-keyid', objectinfo.keyid);

        // update the Legacy Survey URL
        $('#legacy-at-loc').attr(
          'href',
          `http://legacysurvey.org/viewer?ra=${objectinfo.ra}&dec=${objectinfo.dec}&zoom=15&layer=hsc2`
        );

        // update the hscMap URL
        var src_ra = objectinfo.ra * Math.PI / 180.0;
        var src_dec = objectinfo.dec * Math.PI / 180.0;

        $('#hsc-map-at-loc').attr(
          'href',
          `https://hscdata.mtk.nao.ac.jp/hsc_ssp/dr2/s18a/hscMap/app/#/?_=%7B%22view%22:%7B%22a%22:${src_ra}%2C%22d%22:${src_dec}%2C%22fovy%22:0.0002%2C%22roll%22:0%7D%2C%22sspParams%22:%7B%22type%22:%22SDSS_TRUE_COLOR%22%2C%22filter%22:%5B%22HSC-I%22%2C%22HSC-R%22%2C%22HSC-G%22%5D%2C%22simpleRgb%22:%7B%22beta%22:22026.465794806718%2C%22a%22:1%2C%22bias%22:0.05%2C%22b0%22:0%7D%2C%22sdssTrueColor%22:%7B%22beta%22:22026.465794806718%2C%22a%22:1%2C%22bias%22:0.05%2C%22b0%22:0%7D%7D%7D`
        );

        // update the main table
        $('#current-objectid-val').html(objectinfo.objectid);
        $('#current-ra-val').html(objectinfo.ra);
        $('#current-dec-val').html(objectinfo.dec);
        $('#current-reff-val').html(
          objectinfo.extra_columns['flux_radius_ave_g']
            .toPrecision(2)
        );
        $('#current-mug0-val').html(
          objectinfo.extra_columns['mu_ave_g'].toPrecision(4)
        );
        $('#current-gicolor-val').html(
          objectinfo.extra_columns['g-i'].toPrecision(2)
        );
        $('#current-grcolor-val').html(
          objectinfo.extra_columns['g-r'].toPrecision(2)
        );

        // clean out the extra info table
        $('#extra-info-cols').empty();

        // update the extra info table
        for (let item in objectinfo.extra_columns) {
          let thisrow = `<tr>
<th>${item}</th><td>${objectinfo.extra_columns[item]}</td>
</tr>`;
          $('#extra-info-cols').append(thisrow);

        }

        // clean out the flag button group
        $('#flag-button-group-1').empty();
        $('#flag-button-group-2').empty();

        if (review.current_object_reviewstatus != 'incomplete') {

          $('#flag-button-group-1').html(
            '<div class="row"><div class="col-12">' +
              'Voting on this object is closed because ' +
              'its review has been completed. Its final status was set to: ' +
              review.current_object_reviewstatus +
              '</div></div>'
          );

        }

        else if (review.current_object_reviewstatus == 'incomplete' &&
                 object_already_reviewed === true) {

          $('#flag-button-group-1').html(
            '<div class="row"><div class="col-12">' +
              'You have already voted on this object. ' +
              'It remains open to votes from other users. ' +
              '</div></div>'
          );

        }

        else {

          // fill in group 1
          for (let item of ['candy', 'galaxy', 'junk']) {

            let color = 'info';

            if (item == 'candy') {
              color = 'primary';
            }
            else if (item == 'galaxy') {
              color = 'success';
            }
            else if (item == 'junk') {
              color = 'danger';
            }

            // the buttons are always loaded in inactive state
            let button_activated = '';
            let button_disabled = '';

            if (review.current_readonly) {
              button_disabled = 'disabled';
            }

            let thisrow = `
<button type="button" data-state="${button_activated}" data-value="${item}" data-toggle="button" autocomplete="off"
id="check-${item}" class="mx-2 mt-1 btn btn-lg btn-block btn-${color} object-flags-button ${button_activated}" ${button_disabled}>
${item}
</button>`;

            $('#flag-button-group-1').append(thisrow);
          }

          // fill in group 2
          for (let item of ['tidal', 'outskirts', 'cirrus']) {

            let color = 'info';

            if (item == 'tidal') {
              color = 'secondary';
            }
            else if (item == 'outskirts') {
              color = 'warning';
            }
            else if (item == 'cirrus') {
              color = 'dark';
            }

            // the buttons are always loaded in inactive state
            let button_activated = '';
            let button_disabled = '';

            if (review.current_readonly) {
              button_disabled = 'disabled';
            }

            let thisrow = `
<button type="button" data-state="${button_activated}" data-value="${item}" data-toggle="button" autocomplete="off"
id="check-${item}" class="mx-2 mt-1 btn btn-lg btn-block btn-${color} object-flags-button ${button_activated}" ${button_disabled}>
${item}
</button>`;

            $('#flag-button-group-2').append(thisrow);

          }

        }

        // handle the object comments and the submit button in case they're
        // readonly
        if (review.current_object_reviewstatus != 'incomplete') {
          $('#object-notes').prop('disabled',true);
          $('#save-current-object').prop('disabled',true);
        }
        else {
          $('#object-notes').prop('disabled',false);
          $('#save-current-object').prop('disabled',false);
        }

        // clear out the comment stream for this object
        $('.all-object-comments').empty();

        for (let comment of comments) {

          if (comment.comment_added_on !== null) {

            let comment_user_flags = '<table class="table table-sm">';

            for (let flag in comment.comment_userset_flags) {

              let this_flag_val = '<span class="text-danger">false</span>';
              if (comment.comment_userset_flags[flag] === true) {
                this_flag_val = '<span class="text-primary">true</span>';
              }

              comment_user_flags += '<tr><th>' + flag + '</th><td>' +
                this_flag_val + '</td></tr>';
            }
            comment_user_flags += '</table>';

            let comment_made_by = '';
            if (comment.comment_by_username !== null) {
              comment_made_by = comment.comment_by_username;
            }
            else {
              comment_made_by = 'User ID ' + comment.comment_by_userid;
            }

            let comment_box = `
            <div class="card mb-3 mx-1">
              <div class="card-header">
                <strong>${comment_made_by}</strong> &mdash; ${moment(comment.comment_added_on).calendar()}
              </div>
              <div class="card-body">
                ${ui.bib_linkify(comment.comment_text)}

                <div class="mt-2">
                  <strong>Set object flags</strong>
                  ${comment_user_flags}
                </div>

              </div>
            </div>`;
            $('.all-object-comments').append(comment_box);

          }

        }

      }

      // if we couldn't load this object, show the error message.
      else {
        ui.alert_box(message, "danger");
      }

    }).always(function () {

      $('#spinner-block').empty();

    }).fail(function(xhr) {
      ui.alert_box("Could not load requested object, " +
                   "possibly because no objects exist in the current list.",
                   "danger");
    });

  },


  get_next_object: function () {

    // find the current object in the objectlist
    let this_object_index = review.objectlist.indexOf(review.current_objectid);

    // move if the current object is not at the end of the list
    if (this_object_index != (review.objectlist.length-1)) {

      let next_objectid = review.objectlist[this_object_index+1];
      review.get_object(next_objectid);

    }

    // if this object is at the end of the current list, check if there's
    // another page and move there
    else if ( (this_object_index == (review.objectlist.length-1)) &&
              (review.current_page < review.current_npages) ) {

      // load the next objectlist and the first element there
      review.get_object_list(
        review.current_list_reviewstatus,
        'start',
        review.objectlist_end_keyid,
        'first',
        review.current_page + 1
      );

    }

  },


  get_prev_object: function () {

    // find the current object in the objectlist
    let this_object_index = review.objectlist.indexOf(review.current_objectid);

    // move if the current object is not at the end of the list
    if (this_object_index != 0) {

      let prev_objectid = review.objectlist[this_object_index-1];
      review.get_object(prev_objectid);

    }

    // if this object is at the start of the current list, check if there's
    // another page behind us and move there
    else if ( (this_object_index == 0) &&
              (review.current_page > 0) ) {

      // load the prev objectlist and the last element there
      review.get_object_list(
        review.current_list_reviewstatus,
        'end',
        review.objectlist_start_keyid,
        'last',
        review.current_page - 1
      );

    }

  },


  save_object_comments_flags: function (objectid, jump_to_next) {

    let _xsrf;
    let posturl = `/api/save-object/${objectid}`;
    let postparams;

    // get the value of the _xsrf token
    _xsrf = $('#comment-form > input[type="hidden"]').val();

    // get the comments
    let comment_text = $('#object-notes').val();

    // get the flags
    let object_flags = {};
    for (let item of $('.object-flags-button')) {
      if (item.dataset.state == 'active' ||
          item.className.indexOf('active') != -1) {
        object_flags[item.dataset.value] = true;
      }

      else {
        object_flags[item.dataset.value] = false;
      }

    }

    object_flags = JSON.stringify(object_flags);

    // put together the request params
    postparams = {
      _xsrf:_xsrf,
      objectid: objectid,
      comment_text:comment_text,
      user_flags:object_flags
    };

    // fire the request to the backend
    $.post(posturl, postparams, function (data) {

      let status = data.status;
      let message = data.message;

      // update the object info
      if (status == 'ok' && (jump_to_next === undefined)) {
        review.get_object(objectid);
      }

      else if (status != 'ok') {
        ui.alert_box(message, "danger");
      }

    }, 'json').done(function () {

      // update the object list
      let objectlist_reviewtype = $('#objectlist-pref-select').val();

      review.get_object_list(
        objectlist_reviewtype,
        'start',
        review.objectlist_start_keyid,
      );

    }).done(function () {

      // jump to the next object if told to do so
      if (jump_to_next !== undefined && jump_to_next === true) {

        review.get_next_object();

      }


    }).fail(function (xhr) {
      ui.alert_box("Could not update this object.", "danger");
    });

  }

};
