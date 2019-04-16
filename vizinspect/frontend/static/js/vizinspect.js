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


  // this saves UI prefs on the user home page to the vizinspect_prefs cookie
  save_prefs_cookie: function () {

    let always_email = $('#prefs-email-when-done').prop('checked');

    let default_visibility = $('[name="prefs-dataset-visibility"]')
        .filter(':checked').attr('id');

    if (default_visibility === undefined) {
      default_visibility = null;
    }

    let cookie_settings = {
      expires: ui.prefs_cookie_expires_days
    };
    if (ui.prefs_cookie_secure) {
      cookie_settings.secure = true;
    }

    Cookies.set('vizinspect_prefs',
                {always_email: always_email,
                 default_visibility: default_visibility},
                cookie_settings);

    ui.alert_box('Your preferences have been saved.','primary');

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

    // bind the cookie setters
    $('#prefs-save').on('click', function(evt) {
      ui.save_prefs_cookie();
    });

    // delete the API key on session end
    $('#user-logout-form').on('submit', function(evt) {
      localStorage.clear();
    });

    // bind the apikey generate button
    $('#prefs-generate-apikey').on('click', function(evt) {
      ui.generate_new_apikey('#api-key','#apikey-expiry');
    });

    //////////////////////
    // CONTROL BINDINGS //
    //////////////////////

    $('#next-object-link').on('click', function (evt) {

      // find the previous object in the objectlist
      let this_object_index = review.objectlist.indexOf(review.current_objectid);

      // only move if the current object is not at the end of the list
      if (this_object_index != (review.objectlist.length-1)) {

        let next_objectid = review.objectlist[this_object_index+1];
        review.get_object(next_objectid);

      }

    });

    $('#prev-object-link').on('click', function (evt) {

      // find the previous object in the objectlist
      let this_object_index = review.objectlist.indexOf(review.current_objectid);

      // only move if the current object is not at the start of the list
      if ( (this_object_index != 0) ) {

        let prev_objectid = review.objectlist[this_object_index-1];
        review.get_object(prev_objectid);

      }


    });


    $('#jump-source-index').on('click', function (evt) {

      let jump_to = parseInt($('#current-source-index').val());

      if (!isNaN(jump_to)) {

        // fire the get object function
        review.get_object(jump_to);

      }

    });

    $('#objectid-list').on('click','.objectid-link', function (evt) {

      evt.preventDefault();

      let this_objectid = $(this).attr('data-objectid');
      review.get_object(this_objectid);

    });


    //////////////////////////
    // FORM SUBMIT BINDINGS //
    //////////////////////////

    // bind the form submit for the review
    $('#review-form').on('submit', function (event) {

      event.preventDefault();
      ui.debounce(review.do_review(), 250);

    });

  }

};


// this contains functions to drive the main controls and send the requests to
// the backend
var review = {

  current_objectid: null,
  current_keyid: null,
  current_readonly: null,

  objectlist: null,
  objectlist_start_keyid: null,
  objectlist_end_keyid: null,

  // this fetches the full object list. if load_first_object is true, will load
  // the first object in the list right after fetching the list
  get_object_list: function (review_status,
                             start_keyid,
                             end_keyid,
                             load_first_object) {

    let url = `/api/list-objects?review_status=${review_status}&start_keyid=${start_keyid}&end_keyid=${end_keyid}`;

    $.getJSON(url, function (data) {

      let status = data.status;
      let result = data.result;
      let message = data.message;

      // if all's good, set the current object list and update the page
      if (status == 'ok') {

        review.objectlist = result.objectlist;
        review.objectlist_start_keyid = result.start_keyid;
        review.objectlist_end_keyid = result.start_keyid;

        // TODO: update the object list controls with the object IDs
        for (let objectid of review.objectlist) {

          let this_elem =
              `<li><a href="#" class="objectid-link" data-objectid="${objectid}">${objectid}</a></li>`;
          $('#objectid-list').append(this_elem);

        }

      }

      // if we failed, alert the user
      else {

        ui.alert_box(message, "danger");

      }



    }).done(function (xhr) {

      // get the first object if load_first_object is true
      if (load_first_object === true) {

        review.get_object(review.objectlist[0]);

      }


    }).fail( function (xhr) {

      ui.alert_box("Could not load object list from the backend.", "danger");

    });

  },


  get_object: function (source_index) {

    let url = `/api/load-object/${source_index}`;

    // fire the request to the backend
    $.getJSON(url, function(data) {

      let status = data.status;
      let result = data.result;
      let message = data.message;

      if (status == 'ok') {

        let objectinfo = result.info;
        let comments = result.comments;
        let readonly = result.readonly;
        let objectplot = result.plot;

        // update the status
        review.current_objectid = objectinfo.objectid;
        review.current_keyid = objectinfo.keyid;
        review.current_readonly = readonly;

        // update the plot
        $('#galaxy-main-plot').attr('src','/viz-inspect-data/' + objectplot);
        $('#galaxy-main-plot').attr('data-sourceindex', objectinfo.objectid);
        $('#galaxy-main-plot').attr('data-ra', objectinfo.ra);
        $('#galaxy-main-plot').attr('data-dec', objectinfo.dec);

        // update the objectid and keyid
        $('#current-source-index').val(objectinfo.objectid);
        $('#current-source-index').attr('data-keyid', objectinfo.keyid);

        // update the main table
        $('#current-objectid-val').html(objectinfo.objectid);
        $('#current-ra-val').html(objectinfo.ra);
        $('#current-dec-val').html(objectinfo.dec);
        $('#current-reff-val').html(objectinfo.extra_columns['r_e']);
        $('#current-mug0-val').html(
          objectinfo.extra_columns['mu_e_ave_forced_g']
        );
        $('#current-gicolor-val').html(objectinfo.extra_columns['g-i']);
        $('#current-grcolor-val').html(objectinfo.extra_columns['g-r']);

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
        $('#flag-checkbox-group').empty();

        // update the flags button group
        for (let item in objectinfo.user_flags) {

          let checked = objectinfo.user_flags[item];
          let checkbox_checked = '';
          if (checked) {
            checkbox_checked = 'checked';
          }
          let checkbox_disabled = '';
          if (review.current_readonly) {
            checkbox_disabled = 'disabled';
            $('#flag-checkbox-group').append(
              "<p>This object is not in your review " +
                "assigment so it has been marked as <em>read-only</em>.</p>"
            )
          }

          let thisrow = `
<div class="custom-control custom-checkbox">
  <input type="checkbox"
         class="custom-control-input"
         id="check-${item}" ${checkbox_checked} ${checkbox_disabled}>
  <label class="custom-control-label" for="check-${item}">${item}</label>
</div>`;
          $('#flag-checkbox-group').append(thisrow);

        }

        // TODO: handle the notes form


        // TODO: handle the object comments


      }

      // if we couldn't load this object, show the error message.
      else {
        ui.alert_box(message, "danger");
      }

    }).fail(function(xhr) {
      ui.alert_box("Could not load this object from the backend.", "danger");
    });

  },


  do_review: function (override_params) {

    var _xsrf;
    var posturl = '/api/review';
    var postparams;

    // get the value of the _xsrf token
    _xsrf = $('#xmatch-form > input[type="hidden"]').val();

    // put together the request params
    postparams = {
      _xsrf:_xsrf,
    };

    // get the rest of the postparams from the controls

    // fire the request to the backend

  }

};
