/* Receipt-backed categorical series. Shared by the browser and focused tests. */
(function (root) {
  'use strict';
  const labels = ['Baseline', 'Unary plus and constant SELECT', 'DISTINCT plus broader column admission', 'Whole-table aggregates', 'CASE/IN/BETWEEN/ABS', 'ORDER BY/LIMIT', 'Exact decimal comparisons'];
  const changes = ['Starting measured reference scope.', 'Adds unary plus and constant queries.', 'Adds DISTINCT; broader column admission and SELECT * also contribute.', 'Adds COUNT, SUM, MIN and MAX without GROUP BY.', 'Adds selected scalar constructs and their supported translations.', 'Adds ordering over output columns and LIMIT, with admission rules for ties and NULL placement.', 'Adds exact decimal comparison meanings and broader REAL-column admission; decimal loading/scaling translation remains trusted.'];
  const total = 7421339, applicable = 5939714, set = 'sqlite-slt/full-622';
  function series(entries) {
    if (entries.length < 2) throw Error('Expected a comparable full-suite history');
    return entries.map(({path, receipt: r, label}, i) => {
      const n = r.fixed.matched_verified;
      if (r.test_set.id !== set || r.test_set.files !== 622 || r.fixed.total !== total || r.applicable.total !== applicable || r.applicable.matched_verified !== n) throw Error('Incompatible test scope');
      if (!Number.isInteger(n) || n < 0 || n > total || !Number.isFinite(r.fixed.rate_percent) || Math.abs(r.fixed.rate_percent - 100*n/total) > 1e-8) throw Error('Invalid coverage count/rate');
      if (!/^evidence\/[A-Za-z0-9._-]+\.json$/.test(path)) throw Error('Invalid receipt path');
      const gain = i ? n - entries[i-1].receipt.fixed.matched_verified : null;
      return {path, label: labels[i] || label || r.presentation?.point_label || `Milestone ${i+1}`, change: changes[i] || r.presentation?.log_change || 'See this receipt for measured scope and admission changes.', n, total, applicable, rate: 100*n/total, gain, pp: gain === null ? null : 100*gain/total, date: r.timing.finished_pacific};
    });
  }
  function nextIndex(index, key, length) {
    if (key === 'ArrowRight' || key === 'ArrowDown') return Math.min(length-1, index+1);
    if (key === 'ArrowLeft' || key === 'ArrowUp') return Math.max(0, index-1);
    if (key === 'Home') return 0;
    if (key === 'End') return length-1;
    if (key === 'Enter' || key === ' ') return index;
    return null;
  }
  function bind(controls, select, pointerIndex = (_, i) => i) {
    controls.forEach((control, i) => {
      control.addEventListener('click', event => select(pointerIndex(event, i)));
      control.addEventListener('pointerenter', event => select(pointerIndex(event, i)));
      control.addEventListener('pointermove', event => select(pointerIndex(event, i)));
      control.addEventListener('focus', () => select(i));
      control.addEventListener('keydown', event => {
        const next = nextIndex(i, event.key, controls.length);
        if (next === null) return;
        event.preventDefault(); select(next); controls[next].focus();
      });
    });
  }
  // Hit circles intentionally remain large. Painting order must not decide
  // which measured center a real pointer selects when neighboring circles overlap.
  function nearestPoint(centers, x, y) {
    let best=0, distance=Infinity;
    centers.forEach(([cx,cy],i)=>{const d=(x-cx)**2+(y-cy)**2;if(d<distance){best=i;distance=d;}});
    return best;
  }
  function geometry(width, count) {
    const compact = width < 850;
    const w = Math.max(240, width);
    return {width:w, compact, left:compact?66:80, step:(compact?w-94:w-160)/(count-1),
      gridRight:compact?w-28:w-50, radius:compact?22:24};
  }
  function mount(host, detail, points, doc = document) {
    const svg = doc.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', '0 0 900 360'); svg.setAttribute('role', 'group');
    svg.setAttribute('aria-label', 'Verified coverage, ordered milestones, fixed zero to 100 percent scale');
    const add = (tag, attrs, text, parent = svg) => {
      const node = doc.createElementNS('http://www.w3.org/2000/svg', tag);
      Object.entries(attrs).forEach(([k,v]) => node.setAttribute(k,v));
      if (text != null) node.textContent = text; parent.append(node); return node;
    };
    const xy = points.map((p,i) => [80+i*740/(points.length-1), 300-p.rate*2.6]);
    const grids=[];
    [0,25,50,75,100].forEach(n => {
      const y=300-n*2.6; grids.push(add('line',{x1:65,x2:850,y1:y,y2:y,class:'chart-grid'}));
      add('text',{x:55,y:y+5,'text-anchor':'end',class:'chart-axis'}, n+'%');
    });
    const line=add('polyline',{points:xy.map(p=>p.join(',')).join(' '),class:'coverage-line',fill:'none','aria-hidden':'true'});
    const shapes=[];
    const controls=points.map((p,i) => {
      const [x,y]=xy[i], g=add('g',{role:'button',tabindex:i===points.length-1?'0':'-1','aria-pressed':String(i===points.length-1),'aria-controls':'coverage-detail','aria-describedby':'chart-help','aria-label':`${i+1}. ${p.label}, ${p.rate.toFixed(2)} percent of all records`,class:'coverage-point'});
      const hit=add('circle',{cx:x,cy:y,r:24,class:'point-hit'},null,g);
      const dot=add('circle',{cx:x,cy:y,r:7,class:'point-dot'},null,g);
      const rate=add('text',{x,y:y-20,'text-anchor':'middle',class:'point-rate'},p.rate.toFixed(2)+'%',g);
      const label=add('text',{x,y:335,'text-anchor':'middle',class:'chart-axis'},String(i+1),g);
      shapes.push({hit,dot,rate,label});
      return g;
    });
    const fmt=n=>n.toLocaleString('en-US');
    const select = i => {
      controls.forEach((c,j)=>{c.setAttribute('aria-pressed',String(i===j));c.setAttribute('tabindex',i===j?'0':'-1');});
      detail.replaceChildren(); const p=points[i];
      const text=(tag,value)=>{const n=doc.createElement(tag);n.textContent=value;detail.append(n);return n;};
      text('h3',`${i+1} / ${p.label}`);
      text('p',`${p.rate.toFixed(2)}% · ${fmt(p.n)} / ${fmt(p.total)} full-collection records`).className='detail-value';
      text('p',p.change);
      text('p',p.gain===null?'Baseline; no earlier full-suite point.':`${p.gain>=0?'+':''}${fmt(p.gain)} records · ${p.pp>=0?'+':''}${p.pp.toFixed(2)} percentage points vs previous milestone.`);
      const link=text('a','Inspect this exact receipt →');link.href='https://www.assaydb.org/receipt.html?f='+encodeURIComponent(p.path);
      const dates=doc.createElement('details'), summary=doc.createElement('summary');summary.textContent='Recorded completion date';dates.append(summary);
      const date=doc.createElement('p');date.textContent=p.date;dates.append(date);detail.append(dates);
      detail.dataset.selected=String(i);
    };
    bind(controls,select,(event,i)=>{
      // Keyboard/synthetic activation has no pointer location; preserve its target.
      if(!event||event.detail===0&&event.type==='click'||!Number.isFinite(event.clientX)||!Number.isFinite(event.clientY)||!shapes.every(s=>s.hit.getBoundingClientRect))return i;
      const centers=shapes.map(({hit})=>{const r=hit.getBoundingClientRect();return [r.left+r.width/2,r.top+r.height/2];});
      return nearestPoint(centers,event.clientX,event.clientY);
    }); host.replaceChildren(svg);
    const legend=doc.createElement('ol');legend.className='milestone-labels';
    points.forEach(p=>{const li=doc.createElement('li');li.textContent=p.label;legend.append(li);});host.append(legend);select(points.length-1);
    function resize(width) {
      const layout=geometry(width,points.length);
      svg.setAttribute('viewBox',`0 0 ${layout.width} 360`);
      svg.setAttribute('data-compact',String(layout.compact));
      grids.forEach(grid=>grid.setAttribute('x2',layout.gridRight));
      const coordinates=points.map((p,i)=>[layout.left+i*layout.step,300-p.rate*2.6]);
      line.setAttribute('points',coordinates.map(p=>p.join(',')).join(' '));
      shapes.forEach(({hit,dot,rate,label},i)=>{
        const x=coordinates[i][0];hit.setAttribute('cx',x);hit.setAttribute('r',layout.radius);
        dot.setAttribute('cx',x);rate.setAttribute('x',x);label.setAttribute('x',x);
        // Compact mode uses numbered milestones and the full HTML legend/detail.
        rate.setAttribute('display',layout.compact?'none':'inline');
      });
    }
    resize(host.getBoundingClientRect ? host.getBoundingClientRect().width : 900);
    if (typeof ResizeObserver !== 'undefined') {
      const observer=new ResizeObserver(entries=>resize(entries[0].contentRect.width));
      observer.observe(host);
    }
    return {controls,select,resize};
  }
  const api={series,nextIndex,bind,mount,geometry,nearestPoint,labels,total,applicable};
  if (typeof module !== 'undefined') module.exports=api;
  else root.AssayCoverage=api;
})(typeof window === 'undefined' ? globalThis : window);
