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
   Chart.defaults.color = color;
   Chart.defaults.scale.ticks.color = color;
   Chart.defaults.scale.grid.color = grid;
   Object.keys(Chart.instances || {}).forEach(function (id) {
    var chart = Chart.instances[id], options = chart.options;
    if (options.plugins && options.plugins.legend) options.plugins.legend.labels.color = color;
    if (options.scales) Object.keys(options.scales).forEach(function (axis) {
     var scale = options.scales[axis];
     scale.ticks.color = color;
     scale.grid.color = grid;
    });
    chart.update('none');
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
