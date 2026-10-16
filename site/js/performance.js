/* Audited full-precision export: sum of query-round medians / three rounds. */
(function(root){
 'use strict';
 const path='evidence/completed-speed-macbook-run-001.json';
 const lanes=['sf1-w1','sf1-w10','sf10-w1','sf10-w10'],references=['duckdb','polars'];
 function describe(r){return (100*Math.abs(r-1)).toFixed(1)+'% '+(r<1?'less':r>1?'more':'same')+' total query time';}
 function records(receipt){
  if(receipt.schema!=='assay-completed-speed/1'||receipt.status!=='completed'||receipt.method.ratio_direction!=='assaydb/reference')throw Error('Unsupported speed receipt');
  return lanes.flatMap(lane=>{const run=receipt.lanes[lane];if(run.denominator!==22||run.wrong_answers!==0||run.failed_timing_outcomes!==0)throw Error('Unexpected measurement scope');return references.map(reference=>{
   const ratio=run.comparisons['assaydb_'+reference].total.point;
   if(typeof ratio!=='number'||!Number.isFinite(ratio)||ratio<0||ratio>4.5)throw Error('Invalid direct total ratio');
   return {lane,reference,ratio,label:describe(ratio)};
  });});
 }
 const exportPath='data/speed-macbook-run-001.json',engines=['assaydb','duckdb','polars'];
 function seconds(data,receipt){
  records(receipt);
  if(data.run_id!=='macbook-run-001'||data.provenance.retained_receipt!==path||data.source_hashes_match_retained_receipt!==true||data.schema!=='assay-site-speed-export/1'||data.units!=='seconds'||data.field!=='sum_query_round_medians_seconds'||data.rounds!==3||data.query_count!==22||data.hot_samples_per_round!==5||data.benchmarks_rerun!==false||data.provenance.original_summary_files_independently_rehashed!==false)throw Error('Invalid export scope');
  return lanes.flatMap(lane=>{const d=data.lanes[lane];if(d.source_file!==lane+'/summary.json'||receipt.artifacts_sha256[d.source_file]!==d.source_file_sha256||!/^[a-f0-9]{64}$/.test(d.source_file_sha256))throw Error('Source hash mismatch');
   for(const reference of references){const ratio=d.sum_query_round_medians_seconds.assaydb/d.sum_query_round_medians_seconds[reference];if(Math.abs(ratio-receipt.lanes[lane].comparisons['assaydb_'+reference].total.point)>1e-12)throw Error('Ratio mismatch');}
   return engines.map(engine=>{const sum=d.sum_query_round_medians_seconds[engine];if(typeof sum!=='number'||!Number.isFinite(sum)||sum<=0)throw Error('Invalid seconds');return {lane,engine,seconds:sum/3,max:lane.startsWith('sf1-')?7:80};});
  });
 }
 function render(doc,rows){doc.querySelectorAll('[data-seconds-lane]').forEach(node=>{const row=rows.find(r=>r.lane===node.dataset.secondsLane&&r.engine===node.dataset.secondsEngine);if(!row||row.seconds>row.max)throw Error('Unknown seconds row');node.querySelector('.seconds-bar').style.width=(100*row.seconds/row.max)+'%';node.querySelector('.seconds-value').textContent=row.seconds.toFixed(2)+' s';});}
 function tabs(doc){const bar=doc.getElementById('speed-tabs');if(!bar)return;const buttons=[...bar.querySelectorAll('[role="tab"]')],panels=[...doc.querySelectorAll('[data-speed-panel]')];function select(i,focus=false){buttons.forEach((b,j)=>{b.setAttribute('aria-selected',String(j===i));b.tabIndex=j===i?0:-1;panels[j].hidden=j!==i;panels[j].setAttribute('role','tabpanel');panels[j].setAttribute('aria-labelledby',b.id);});if(focus)buttons[i].focus();}buttons.forEach((b,i)=>{b.addEventListener('click',()=>select(i));b.addEventListener('keydown',e=>{let next=e.key==='Home'?0:e.key==='End'?1:e.key==='ArrowRight'||e.key==='ArrowLeft'?1-i:null;if(next!==null){e.preventDefault();select(next,true);}});});select(0);bar.hidden=false;}
 async function enhance(root){const doc=root.document;tabs(doc);if(!doc.querySelector('[data-seconds-lane]'))return;
  try{const response=await root.fetch(path,{cache:'no-store'});if(!response.ok)throw Error('Receipt unavailable');const receipt=await response.json(),exportResponse=await root.fetch(exportPath,{cache:'no-store'});if(!exportResponse.ok)throw Error('Export unavailable');const rows=seconds(await exportResponse.json(),receipt);if(doc.fonts)await doc.fonts.ready;render(doc,rows);doc.body.dataset.performance='ready';}
  catch(_){doc.body.dataset.performance='unavailable';const status=doc.getElementById('performance-status');if(status)status.textContent='The measurement file could not load. The saved results and source link are still available below.';}
 }
 if(typeof module==='object'&&module.exports)module.exports={path,exportPath,records,seconds,describe,render,tabs,enhance};else enhance(root);
})(typeof window==='object'?window:globalThis);
