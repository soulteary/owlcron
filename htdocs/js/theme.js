// OwlCron theme preference. Storage may be unavailable in private contexts.
(function () {
 'use strict';
 var key = 'owlcron.theme', media = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
 var preference = 'system';
 try { preference = localStorage.getItem(key) || 'system'; } catch (ignore) {}
 if (['system', 'light', 'dark'].indexOf(preference) < 0) preference = 'system';
 function apply() {
  var dark = preference === 'dark' || (preference === 'system' && media && media.matches);
  document.body.classList.toggle('dark-theme', !!dark);
  document.body.style.colorScheme = dark ? 'dark' : 'light';
  var select = document.getElementById('theme-preference');
  if (select) select.value = preference;
  if (window.Chart) {
   var color = dark ? '#d4dae3' : '#555555', grid = dark ? '#38404d' : 'rgba(0,0,0,0.1)';
   Chart.defaults.global.defaultFontColor = color;
   if (Chart.defaults.scale) {
    if (Chart.defaults.scale.ticks) Chart.defaults.scale.ticks.fontColor = color;
    if (Chart.defaults.scale.gridLines) Chart.defaults.scale.gridLines.color = grid;
   }
   Object.keys(Chart.instances || {}).forEach(function (id) {
    var chart = Chart.instances[id], options = chart.options;
    if (options.legend && options.legend.labels) options.legend.labels.fontColor = color;
    if (options.scales) ['xAxes', 'yAxes'].forEach(function (axis) {
     (options.scales[axis] || []).forEach(function (scale) {
      if (scale.ticks) scale.ticks.fontColor = color;
      if (scale.gridLines) scale.gridLines.color = grid;
     });
    });
    chart.update(0);
   });
  }
 }
 function set(value) {
  preference = ['system','light','dark'].indexOf(value) >= 0 ? value : 'system';
  try { localStorage.setItem(key, preference); } catch (ignore) {}
  apply();
 }
 var select = document.getElementById('theme-preference');
 if (select) select.addEventListener('change', function () { set(select.value); });
 if (media) {
  var onChange = function () { if (preference === 'system') apply(); };
  if (media.addEventListener) media.addEventListener('change', onChange);
  else if (media.addListener) media.addListener(onChange);
 }
 window.OwlCronTheme = {set: set, refresh: apply};
 apply();
}());
