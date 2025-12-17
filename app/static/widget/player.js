(function () {
  var config = window.YadroWidgetConfig || {};
  var sessionToken = config.sessionToken;
  var playerConfig = config.playerConfig || {};
  var eventEndpoint = config.eventEndpoint || "/widget/event";

  if (!sessionToken) {
    console.error("[YadroPlayer] sessionToken is required");
    return;
  }

  var video = document.getElementById("yadro-video");
  if (!video) {
    console.error("[YadroPlayer] video element not found");
    return;
  }

  var endpointUrl = new URL(eventEndpoint, window.location.origin).toString();
  var quartiles = {25: false, 50: false, 75: false, 100: false};

  function sendEvent(type, videoTime, meta) {
    var payload = {
      session_token: sessionToken,
      event_type: type,
      video_time: videoTime != null ? videoTime : null,
      meta: meta || {}
    };

    fetch(endpointUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    }).catch(function (err) {
      // не спамим консолью
    });
  }

  video.addEventListener("loadeddata", function () {
    sendEvent("impression", 0, {});
  });

  video.addEventListener("play", function () {
    sendEvent("view_start", video.currentTime, {});
  });

  video.addEventListener("pause", function () {
    sendEvent("pause", video.currentTime, {});
  });

  video.addEventListener("ended", function () {
    sendEvent("complete", video.currentTime, {});
    quartiles[25] = quartiles[50] = quartiles[75] = quartiles[100] = true;
  });

  video.addEventListener("timeupdate", function () {
    var duration = video.duration || 0;
    if (!duration || !isFinite(duration)) {
      return;
    }

    var progress = (video.currentTime / duration) * 100;

    [25, 50, 75, 100].forEach(function (q) {
      if (!quartiles[q] && progress >= q) {
        quartiles[q] = true;
        sendEvent("quartile_" + q, video.currentTime, {});
      }
    });
  });

  video.addEventListener("volumechange", function () {
    if (video.muted || video.volume === 0) {
      sendEvent("mute", video.currentTime, {});
    } else {
      sendEvent("unmute", video.currentTime, {});
    }
  });
})();
