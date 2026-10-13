/* Six receipt-backed checkpoints within the 37-hour build; no invented samples. */
(function(root){
  'use strict';
  const fmt=n=>n.toLocaleString('en-US');
  function records(receipt){
    const pilot=receipt.historical_series.pilot,full=receipt.full_scope;
    const make=(r,scope,hours,files)=>{
      const n=r.applicable.passed,d=r.applicable.total,rate=100*n/d;
      const stored=r.applicable.percent ?? r.applicable.rate_percent;
      if(!Number.isFinite(hours)||hours<31||hours>37||!Number.isSafeInteger(n)||!Number.isSafeInteger(d)||d<=0||n<0||n>d||rate<90||!Number.isSafeInteger(r.wrong_answers)||r.wrong_answers<0||r.wrong_answers>d-n||!Number.isFinite(stored)||Math.abs(rate-stored)>1e-8)throw Error('Invalid recorded checkpoint');
      return {id:r.id,scope,hours,n,d,rate,wrong:r.wrong_answers,files};
    };
    const pilots=pilot.points.map(p=>make(p,'pilot',p.hours_since_first_commit,pilot.file_count));
    const runs=receipt.events.filter(p=>p.hours_since_first_recorded_commit<=37).map(p=>make(p,'full',p.hours_since_first_recorded_commit,full.files));
    if(pilots.length!==4||runs.length!==2||pilot.file_count!==35||full.files!==622||pilots.some(p=>p.d!==pilot.applicable_total)||runs.some(p=>p.d!==full.applicable_records))throw Error('Unexpected checkpoint scope');
    const result=[...pilots.sort((a,b)=>a.hours-b.hours),...runs.sort((a,b)=>a.hours-b.hours)];
    if(result.some((p,i)=>i>0&&p.hours<=result[i-1].hours))throw Error('Unordered checkpoints');
    return result;
  }
  function layout(width=700){return {width,height:width<600?230:280,left:48,right:width-30,top:105,bottom:width<600?180:230};}
  function geometry(p,width=700){const l=layout(width);return {x:l.left+(p.hours-31)/6*(l.right-l.left),y:l.bottom-(p.rate-90)/10*(l.bottom-l.top)};}
  function target(p,i,width=700){const g=geometry(p,width);return g;}
  // Close recorded points can share 44px targets; resolve pointer intent by
  // distance to the actual measured centers rather than SVG stacking order.
  function nearestPoint(points,width,x,y){return points.reduce((best,p,j)=>{const g=geometry(p,width),prev=geometry(points[best],width);return Math.hypot(x-g.x,y-g.y)<Math.hypot(x-prev.x,y-prev.y)?j:best;},0);}
  function keyIndex(i,key){return key==='Home'?0:key==='End'?5:key==='ArrowLeft'?Math.max(0,i-1):key==='ArrowRight'?Math.min(5,i+1):null;}
  function mount(doc,points,motion,playback){
    const host=doc.getElementById('build-checkpoints'),caption=doc.getElementById('build-receipt');
    const buttons=[...host.querySelectorAll('[data-checkpoint]')],status=doc.getElementById('build-status');let selected=5;
    function select(i,announce=false){selected=i;host.dataset.selected=String(i);const p=points[i];
      buttons.forEach((b,j)=>b.setAttribute('aria-pressed',String(j===i)));
      caption.textContent=`${p.scope==='pilot'?'Pilot':'Full test'} · ${p.hours.toFixed(2)}h · ${p.rate.toFixed(3)}% passed (${fmt(p.n)} / ${fmt(p.d)} eligible records) · ${fmt(p.wrong)} observed wrong answers.`;
      if(announce)status.textContent=caption.textContent;
    }
    const player=playback.player({motion,step:i=>{host.dataset.stage='checkpoint';select(i);},finish:announce=>{host.dataset.stage='final';select(5,announce);}});
    function choose(i){player.cancel();host.dataset.stage='final';select(i,true);}
    buttons.forEach((b,i)=>{b.addEventListener('click',e=>{let nearest=i;const svg=host.querySelector('svg');if(e&&Number.isFinite(e.clientX)&&Number.isFinite(e.clientY)&&svg?.getScreenCTM){const at=svg.createSVGPoint();at.x=e.clientX;at.y=e.clientY;const local=at.matrixTransform(svg.getScreenCTM().inverse()),width=svg.getBoundingClientRect().width;nearest=nearestPoint(points,width,local.x,local.y);}choose(nearest);});b.addEventListener('focus',()=>choose(i));b.addEventListener('keydown',e=>{
      if(e.key==='Enter'||e.key===' '){e.preventDefault();choose(i);return;}
      const next=keyIndex(i,e.key);if(next==null)return;e.preventDefault();choose(next);buttons[next].focus();
    });});
    const replay=doc.getElementById('build-replay');if(replay){replay.hidden=false;replay.addEventListener('click',()=>player.play(points.map((_,i)=>[i*350,i]),2100));}
    host.addEventListener('keydown',e=>{if(e.key==='Escape')player.cancel(true);});
    doc.addEventListener('visibilitychange',()=>{if(doc.hidden)player.cancel(true);});
    const observer=playback.observeViewport?.(host,()=>{if(player.playing)player.cancel(true);});
    const svg=host.querySelector('svg');
    function fit(){if(!svg)return;const width=svg.getBoundingClientRect().width;if(!width)return;const l=layout(width);
      svg.setAttribute('viewBox',`0 0 ${width} ${l.height}`);svg.style.height=l.height+'px';
      svg.querySelectorAll('[data-build-point]').forEach((node,i)=>{const g=geometry(points[i],width),t=target(points[i],i,width),marker=node.querySelector('.measured');
        if(points[i].scope==='pilot'){marker.setAttribute('cx',g.x);marker.setAttribute('cy',g.y);}else marker.setAttribute('d',`M${g.x} ${g.y-7} l7 7 l-7 7 l-7 -7 Z`);
        const labelY=g.y+(i===0?-27:i===1?27:i===5?28:18);const line=node.querySelector('.point-leader');Object.entries({x1:g.x,y1:g.y,x2:g.x,y2:labelY-4}).forEach(([k,v])=>line.setAttribute(k,v));
        const hit=node.querySelector('.point-hit');hit.setAttribute('cx',t.x);hit.setAttribute('cy',t.y);hit.setAttribute('r',22);
        const text=node.querySelector('text');text.setAttribute('x',t.x);text.setAttribute('y',labelY);
      });
      svg.querySelectorAll('[data-rate]').forEach(node=>{const y=l.bottom-(Number(node.dataset.rate)-90)/10*(l.bottom-l.top);const line=node.querySelector('line'),text=node.querySelector('text');line.setAttribute('x2',l.right);line.setAttribute('y1',y);line.setAttribute('y2',y);text.setAttribute('y',y+4);});
      svg.querySelectorAll('[data-hour]').forEach(node=>{node.setAttribute('x',l.left+(Number(node.dataset.hour)-31)/6*(l.right-l.left));node.setAttribute('y',l.height-27);});
      svg.querySelector('.elapsed-label').setAttribute('x',width/2);svg.querySelector('.elapsed-label').setAttribute('y',l.height-5);
      svg.querySelectorAll('.endpoint,.endpoint-note').forEach(node=>node.setAttribute('x',l.right));
    }
    fit();if(typeof ResizeObserver==='function')new ResizeObserver(fit).observe(svg);
    select(5);return {choose,player,observer,get selected(){return selected;}};
  }
  async function enhance(root){
    const doc=root.document;
    try {
      const progress=await root.fetch('progress.json',{cache:'no-store'}).then(r=>{if(!r.ok)throw Error('Progress unavailable');return r.json();});
      const path=progress.chapters.build.receipt;
      if(!/^evidence\/[A-Za-z0-9._-]+\.json$/.test(path))throw Error('Invalid receipt path');
      const receipt=await root.fetch(path,{cache:'no-store'}).then(r=>{if(!r.ok)throw Error('Receipt unavailable');return r.json();});
      const points=records(receipt);
      if(doc.fonts)await doc.fonts.ready;
      mount(doc,points,root.matchMedia('(prefers-reduced-motion: reduce)'),root.AssayPlayback);
      doc.getElementById('build-checkpoints').dataset.enhanced='true';
    } catch(_) {doc.getElementById('build-load-status').hidden=false;}
  }
  if(typeof module==='object'&&module.exports)module.exports={records,layout,geometry,target,nearestPoint,keyIndex,mount,enhance};else {root.AssayBuildScene={records,layout,geometry,target,nearestPoint,keyIndex,mount};enhance(root);}
})(typeof window==='object'?window:globalThis);
