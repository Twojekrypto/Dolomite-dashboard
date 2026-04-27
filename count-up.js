(function(){
  const DURATION = 760;
  const STORE = new WeakMap();

  function reducedMotion(){
    return !!(window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches);
  }

  function parseMetric(value){
    const text = String(value ?? "").trim();
    const match = text.match(/^([^+\-0-9]*)([+\-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)(.*)$/);
    if(!match) return null;
    const numericText = match[2];
    const decimals = (numericText.split(".")[1] || "").length;
    const explicitPlus = numericText.trim().startsWith("+");
    const useGrouping = numericText.includes(",");
    const number = Number(numericText.replace(/,/g, ""));
    if(!Number.isFinite(number)) return null;
    return {
      text,
      prefix: match[1],
      suffix: match[3],
      number,
      decimals,
      explicitPlus,
      useGrouping
    };
  }

  function easeOutCubic(t){
    return 1 - Math.pow(1 - t, 3);
  }

  function formatMetric(value, spec){
    const sign = value < 0 ? "-" : spec.explicitPlus ? "+" : "";
    const abs = Math.abs(value);
    const body = spec.useGrouping
      ? abs.toLocaleString("en-US", {
          minimumFractionDigits: spec.decimals,
          maximumFractionDigits: spec.decimals
        })
      : abs.toFixed(spec.decimals);
    return spec.prefix + sign + body + spec.suffix;
  }

  function setFinal(el, spec){
    el.textContent = spec.text;
    el.dataset.countValue = String(spec.number);
  }

  function text(el, value, opts){
    if(!el) return;
    const spec = parseMetric(value);
    const current = STORE.get(el);
    if(current) cancelAnimationFrame(current.raf);
    if(!spec || reducedMotion() || document.hidden || !window.requestAnimationFrame){
      el.textContent = String(value ?? "");
      if(spec) el.dataset.countValue = String(spec.number);
      return;
    }

    const stored = Number(el.dataset.countValue);
    const start = Number.isFinite(stored) ? stored : 0;
    const end = spec.number;
    if(Math.abs(end - start) < 0.000001){
      setFinal(el, spec);
      return;
    }

    const duration = Math.max(240, Number(opts?.duration) || DURATION);
    const started = performance.now();
    el.textContent = formatMetric(start, spec);
    const tick = now => {
      const t = Math.min(1, (now - started) / duration);
      const valueNow = start + (end - start) * easeOutCubic(t);
      el.textContent = formatMetric(valueNow, spec);
      if(t < 1){
        STORE.set(el, {raf: requestAnimationFrame(tick)});
      } else {
        STORE.delete(el);
        setFinal(el, spec);
      }
    };
    STORE.set(el, {raf: requestAnimationFrame(tick)});
  }

  function id(id, value, opts){
    text(document.getElementById(id), value, opts);
  }

  window.CountUpMetric = { text, id };
})();
