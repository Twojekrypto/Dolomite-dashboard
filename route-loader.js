/*
 * Shared route loader for all Dolomite dashboard entry points.
 * Each <route>/index.html defines a small config and calls loadDoloRoute(config).
 * Single source of truth for shared navigation, table UX, mobile polish, and
 * protocol footer assets (previously copy-pasted into route loaders).
 *
 * Config:
 *   label    – user-facing name for the error message ("DOLO", "Earn", ...)
 *   target   – preview file to fetch (relative to the loader page)
 *   version  – cache-bust version string for the preview file
 *   base     – value for <base href>, default "../" ("./" for root index.html)
 *   route    – optional window.__DOLO_ROUTE value
 *   flags    – optional raw JS injected into <head> (e.g. supply route flags)
 *   styles   – optional extra stylesheet hrefs injected into <head>
 *   scripts  – optional extra script srcs (or {src, defer}) injected into <head>
 */
(function () {
  var NAV_VERSIONS = {
    nav: "mobile-nav-responsive-20260801",
    polish: "mobile-polish-safari-details-20260805",
    footer: "protocol-footer-20260619-links-mobile"
  };
  var TABLE_UX_VERSION = "20260821-table-ux-v6";
  var ADDRESS_OVERRIDES_VERSION = "20260823-address-type-normalization-v1";
  var POSITION_ACTIVITY_VERSION = "20260821-actions-v1";
  var CLOUDFLARE_ANALYTICS = {
    src: "https://static.cloudflareinsights.com/beacon.min.js",
    token: "930335c0b8864fdf8d9748c2432adaed"
  };

  function buildNavAssets() {
    return '<link rel="stylesheet" href="mobile-nav.css?v=' + NAV_VERSIONS.nav + '">' +
      '<link rel="stylesheet" href="mobile-polish.css?v=' + NAV_VERSIONS.polish + '">' +
      '<link rel="stylesheet" href="protocol-footer.css?v=' + NAV_VERSIONS.footer + '">' +
      '<script defer src="mobile-nav.js?v=' + NAV_VERSIONS.nav + '"><' + '/script>' +
      '<script defer src="mobile-polish.js?v=' + NAV_VERSIONS.polish + '"><' + '/script>' +
      '<script defer src="protocol-footer.js?v=' + NAV_VERSIONS.footer + '"><' + '/script>';
  }

  function replaceAssetVersion(html, assetName, version) {
    var escaped = assetName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    return html.replace(new RegExp(escaped + "\\?v=[^\\\"'\\s>]+", "g"), assetName + "?v=" + version);
  }

  function prepareSharedTableUx(html) {
    var normalized = replaceAssetVersion(html, "wallet-table-ux.css", TABLE_UX_VERSION);
    normalized = replaceAssetVersion(normalized, "wallet-table-ux.js", TABLE_UX_VERSION);
    normalized = replaceAssetVersion(normalized, "vedolo-position-activity.js", POSITION_ACTIVITY_VERSION);
    normalized = replaceAssetVersion(normalized, "dolo-address-overrides.js", ADDRESS_OVERRIDES_VERSION);

    var assets = "";
    if (!/wallet-table-ux\.css(?:\?v=|[\"'])/.test(normalized)) {
      assets += '<link rel="stylesheet" href="wallet-table-ux.css?v=' + TABLE_UX_VERSION + '" data-dolo-table-ux-version="' + TABLE_UX_VERSION + '">';
    }
    if (!/dolo-address-overrides\.js(?:\?v=|[\"'])/.test(normalized)) {
      assets += '<script src="dolo-address-overrides.js?v=' + ADDRESS_OVERRIDES_VERSION + '" data-dolo-address-overrides-version="' + ADDRESS_OVERRIDES_VERSION + '"><' + '/script>';
    }
    if (!/wallet-table-ux\.js(?:\?v=|[\"'])/.test(normalized)) {
      assets += '<script src="wallet-table-ux.js?v=' + TABLE_UX_VERSION + '" data-dolo-table-ux-version="' + TABLE_UX_VERSION + '"><' + '/script>';
    }
    return { html: normalized, assets: assets };
  }

  function installAnalyticsBeacon() {
    if (!document.body) {
      setTimeout(installAnalyticsBeacon, 0);
      return;
    }
    if (document.querySelector('script[data-cf-beacon]')) return;
    var script = document.createElement("script");
    script.type = "module";
    script.src = CLOUDFLARE_ANALYTICS.src;
    script.setAttribute("data-cf-beacon", JSON.stringify({ token: CLOUDFLARE_ANALYTICS.token }));
    document.body.appendChild(script);
  }

  window.loadDoloRoute = async function (config) {
    try {
      var response = await fetch(config.target + "?v=" + config.version, { cache: "no-cache" });
      if (!response.ok) throw new Error("HTTP " + response.status);
      var html = await response.text();
      var prepared = prepareSharedTableUx(html);
      html = prepared.html;

      var headParts = ['<base href="' + (config.base || "../") + '">'];
      if (config.route) {
        window.__DOLO_ROUTE = config.route;
        headParts.push('<script>window.__DOLO_ROUTE=' + JSON.stringify(config.route) + ';<' + '/script>');
      }
      if (config.flags) {
        headParts.push('<script>' + config.flags + '<' + '/script>');
      }
      (config.styles || []).forEach(function (href) {
        headParts.push('<link rel="stylesheet" href="' + href + '">');
      });
      (config.scripts || []).forEach(function (entry) {
        var descriptor = typeof entry === "string" ? { src: entry, defer: true } : entry;
        if (!descriptor || !descriptor.src) return;
        var deferAttribute = descriptor.defer === false ? "" : " defer";
        headParts.push('<script' + deferAttribute + ' src="' + descriptor.src + '"><' + '/script>');
      });

      document.open();
      document.write(
        html
          .replace("<head>", "<head>" + headParts.join(""))
          .replace("</head>", prepared.assets + buildNavAssets() + "</head>")
      );
      document.close();
      setTimeout(installAnalyticsBeacon, 0);
    } catch (error) {
      document.body.innerHTML =
        '<p style="font-family:system-ui,sans-serif;padding:24px;color:#f4f3ef;background:#09090b">Unable to load ' +
        (config.label || "page") + ".</p>";
      console.error((config.label || "route") + " route failed", error);
    }
  };
})();
