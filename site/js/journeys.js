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
  const host = document.getElementById('speed-log');
  if (!host) return;
  const controls = [...host.querySelectorAll('[data-milestone]')];
  const popup = document.getElementById('milestone-callout');
  let active = null;
  let suppressFocus = false;
  function position() {
    if (popup.hidden || !active) return;
    const anchor = active.getBoundingClientRect();
    const box = popup.getBoundingClientRect();
    const left = Math.max(12, Math.min(innerWidth - box.width - 12, anchor.left + anchor.width / 2 - box.width / 2));
    let top = anchor.bottom + 10;
    if (top + box.height > innerHeight - 12) top = anchor.top - box.height - 10;
    popup.style.left = left + 'px';
    popup.style.top = Math.max(12, Math.min(innerHeight - box.height - 12, top)) + 'px';
  }
  function show(control) {
    active = control;
    controls.forEach(item => item.setAttribute('aria-expanded', String(item === control)));
    document.getElementById('milestone-callout-title').textContent = control.dataset.title;
    ['date', 'result', 'lesson'].forEach(key => {
      popup.querySelector('[data-callout-' + key + ']').textContent = control.dataset[key];
    });
    popup.hidden = false;
    position();
  }
  function close(restore = false) {
    popup.hidden = true;
    controls.forEach(item => item.setAttribute('aria-expanded', 'false'));
    if (restore && active && document.activeElement !== active) {
      suppressFocus = true;
      active.focus({preventScroll: true});
      suppressFocus = false;
    }
  }
  controls.forEach((control, i) => {
    control.addEventListener('focus', () => { if (!suppressFocus) show(control); });
    control.addEventListener('pointermove', event => { if (event.pointerType !== 'touch') show(control); });
    control.addEventListener('click', () => show(control));
    control.addEventListener('keydown', event => {
      if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); show(control); return; }
      // Tab enters the open note so its Close button is reachable immediately.
      if (event.key === 'Tab' && !event.shiftKey && !popup.hidden) {
        event.preventDefault(); popup.querySelector('button').focus(); return;
      }
      const next = event.key === 'Home' ? 0 : event.key === 'End' ? controls.length - 1 :
        ['ArrowRight', 'ArrowDown'].includes(event.key) ? (i + 1) % controls.length :
        ['ArrowLeft', 'ArrowUp'].includes(event.key) ? (i + controls.length - 1) % controls.length : null;
      if (next !== null) { event.preventDefault(); controls[next].focus(); }
    });
  });
  popup.querySelector('button').addEventListener('click', () => close(true));
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape' && !popup.hidden) { event.preventDefault(); close(popup.contains(document.activeElement)); }
  });
  document.addEventListener('pointerdown', event => {
    if (!popup.contains(event.target) && !controls.some(control => control.contains(event.target))) close();
  });
  document.addEventListener('focusin', event => {
    if (!popup.contains(event.target) && !controls.includes(event.target)) close();
  });
  window.addEventListener('resize', position);
  document.addEventListener('scroll', () => requestAnimationFrame(position), true);
})();
