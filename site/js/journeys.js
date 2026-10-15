/* Progressive enhancement: every chart and lesson remains readable without JS. */
(function () {
  'use strict';
  const bar = document.getElementById('journey-tabs');
  if (!bar) return;
  const tabs = [...bar.querySelectorAll('[role="tab"]')];
  const panels = tabs.map(tab => document.getElementById(tab.getAttribute('aria-controls')));
  function select(index, focus = false) {
    tabs.forEach((tab, i) => {
      tab.setAttribute('aria-selected', String(i === index));
      tab.tabIndex = i === index ? 0 : -1;
      panels[i].hidden = i !== index;
      panels[i].setAttribute('role', 'tabpanel');
      panels[i].setAttribute('aria-labelledby', tab.id);
    });
    if (focus) tabs[index].focus();
    // The accuracy SVG measures its visible container.
    window.dispatchEvent(new Event('resize'));
  }
  tabs.forEach((tab, i) => {
    tab.addEventListener('click', () => select(i));
    tab.addEventListener('keydown', event => {
      const index = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 :
        event.key === 'ArrowRight' ? (i + 1) % tabs.length :
        event.key === 'ArrowLeft' ? (i + tabs.length - 1) % tabs.length : null;
      if (index !== null) { event.preventDefault(); select(index, true); }
    });
  });
  select(0); bar.hidden = false;
  const milestoneHost = document.getElementById('speed-log');
  if (milestoneHost) {
    const controls = [...milestoneHost.querySelectorAll('[data-milestone]')];
    const ids = [...new Set(controls.map(control => control.dataset.milestone))];
    function choose(id) {
      controls.forEach(control => control.setAttribute('aria-pressed', String(control.dataset.milestone === id)));
      ids.forEach(key => { document.getElementById('lesson-' + key).hidden = key !== id; });
    }
    controls.forEach(control => {
      const id = control.dataset.milestone;
      control.addEventListener('click', () => choose(id));
      control.addEventListener('focus', () => choose(id));
      control.addEventListener('pointermove', event => { if (event.pointerType !== 'touch') choose(id); });
      control.addEventListener('keydown', event => {
        if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); choose(id); return; }
        const i = ids.indexOf(id);
        const next = event.key === 'Home' ? 0 : event.key === 'End' ? ids.length - 1 :
          ['ArrowRight','ArrowDown'].includes(event.key) ? (i + 1) % ids.length :
          ['ArrowLeft','ArrowUp'].includes(event.key) ? (i + ids.length - 1) % ids.length : null;
        if (next !== null) {
          event.preventDefault();
          const peers = control.tagName.toLowerCase() === 'button' ? '.milestone-lessons ' : '.milestone-chart ';
          milestoneHost.querySelector(peers + '[data-milestone="' + ids[next] + '"]').focus();
        }
      });
    });
    choose(ids[ids.length - 1]);
  }
  document.querySelectorAll('.journey-points:not(.milestone-lessons)').forEach(list => {
    const buttons = [...list.querySelectorAll('.journey-point')];
    function choose(index) {
      buttons.forEach((button, i) => {
        button.setAttribute('aria-pressed', String(i === index));
        document.getElementById(button.getAttribute('aria-controls')).hidden = i !== index;
      });
    }
    buttons.forEach((button, i) => {
      button.addEventListener('click', () => choose(i));
      button.addEventListener('focus', () => choose(i));
      button.addEventListener('pointermove', event => { if (event.pointerType !== 'touch') choose(i); });
      button.addEventListener('keydown', event => {
        const index = event.key === 'Home' ? 0 : event.key === 'End' ? buttons.length - 1 :
          event.key === 'ArrowDown' || event.key === 'ArrowRight' ? (i + 1) % buttons.length :
          event.key === 'ArrowUp' || event.key === 'ArrowLeft' ? (i + buttons.length - 1) % buttons.length : null;
        if (index !== null) { event.preventDefault(); buttons[index].focus(); }
      });
    });
    choose(buttons.length - 1);
  });
})();
