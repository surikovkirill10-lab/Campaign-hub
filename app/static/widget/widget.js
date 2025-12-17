(function () {
  function init() {
    var script = document.currentScript;
    if (!script) {
      return;
    }

    var siteToken = script.dataset.siteToken;
    var articleId = script.dataset.articleId || null;
    var containerId = script.dataset.containerId || "yadro-widget";

    if (!siteToken) {
      console.error("[YadroWidget] data-site-token is required");
      return;
    }

    var container = document.getElementById(containerId);
    if (!container) {
      container = document.createElement("div");
      container.id = containerId;
      document.body.appendChild(container);
    }

    var apiBase = new URL(script.src).origin;

    var payload = {
      site_token: siteToken,
      article_id: articleId,
      page_url: window.location.href
    };

    fetch(apiBase + "/widget/init", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(payload)
    })
      .then(function (resp) { return resp.json(); })
      .then(function (data) {
        if (!data || !data.frame_url) {
          console.error("[YadroWidget] bad init response", data);
          return;
        }

        var iframe = document.createElement("iframe");
        iframe.src = new URL(data.frame_url, apiBase).toString();
        iframe.style.border = "0";
        iframe.style.width = ((data.player_config && data.player_config.width) || 400) + "px";
        iframe.style.height = ((data.player_config && data.player_config.height) || 700) + "px";
        iframe.setAttribute("scrolling", "no");
        iframe.setAttribute("frameborder", "0");
        iframe.allow = "autoplay; fullscreen";

        container.innerHTML = "";
        container.appendChild(iframe);
      })
      .catch(function (err) {
        console.error("[YadroWidget] init error", err);
      });
  }

  if (document.readyState === "complete" || document.readyState === "interactive") {
    init();
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
