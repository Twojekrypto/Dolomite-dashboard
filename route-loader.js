/*
 * Shared route loader for all Dolomite dashboard entry points.
 * Each <route>/index.html defines a small config and calls loadDoloRoute(config).
 * Single source of truth for the mobile-nav / mobile-polish / protocol-footer
 * asset injection (previously copy-pasted into 12 loaders).
 *
 * Config:
 *   label    – user-facing name for the error message ("DOLO", "Earn", ...)
 *   target   – preview file to fetch (relative to the loader page)
 *   version  – cache-bust version string for the preview file
 *   base     – value for <base href>, default "../" ("./" for root index.html)
 *   route    – optional window.__DOLO_ROUTE value
 *   flags    – optional raw JS injected into <head> (e.g. supply route flags)
 *   styles   – optional extra stylesheet hrefs injected into <head>
 *   scripts  – optional extra deferred script srcs injected into <head>
 */
(function () {
  var NAV_VERSIONS = {
    nav: "mobile-nav-20260602-history-tabs",
    polish: "mobile-polish-20260602-history-tabs",
    footer: "protocol-footer-20260619-links-mobile"
  };

  function buildNavAssets() {
    return '<link rel="stylesheet" href="mobile-nav.css?v=' + NAV_VERSIONS.nav + '">' +
      '<link rel="stylesheet" href="mobile-polish.css?v=' + NAV_VERSIONS.polish + '">' +
      '<link rel="stylesheet" href="protocol-footer.css?v=' + NAV_VERSIONS.footer + '">' +
      '<script defer src="mobile-nav.js?v=' + NAV_VERSIONS.nav + '"><' + '/script>' +
      '<script defer src="mobile-polish.js?v=' + NAV_VERSIONS.polish + '"><' + '/script>' +
      '<script defer src="protocol-footer.js?v=' + NAV_VERSIONS.footer + '"><' + '/script>';
  }

  window.loadDoloRoute = async function (config) {
    try {
      var response = await fetch(config.target + "?v=" + config.version, { cache: "no-cache" });
      if (!response.ok) throw new Error("HTTP " + response.status);
      var html = await response.text();

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
      (config.scripts || []).forEach(function (src) {
        headParts.push('<script defer src="' + src + '"><' + '/script>');
      });

      document.open();
      document.write(
        html
          .replace("<head>", "<head>" + headParts.join(""))
          .replace("</head>", buildNavAssets() + "</head>")
      );
      document.close();
    } catch (error) {
      document.body.innerHTML =
        '<p style="font-family:system-ui,sans-serif;padding:24px;color:#f4f3ef;background:#09090b">Unable to load ' +
        (config.label || "page") + ".</p>";
      console.error((config.label || "route") + " route failed", error);
    }
  };
})();
