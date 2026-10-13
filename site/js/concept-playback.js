/* Finite, user-triggered scenes. Static content never depends on playback. */
(function(root){
  'use strict';
  function player({motion, step, finish, setTimer=setTimeout, clearTimer=clearTimeout}) {
    let timers=[],generation=0,playing=false;
    function cancel(final=false){generation++;timers.forEach(clearTimer);timers=[];playing=false;if(final)finish(false);}
    function play(sequence,duration){
      cancel();
      if(motion.matches){finish(true);return;}
      playing=true;const token=generation;
      sequence.forEach(([time,value])=>{
        const run=()=>{if(token===generation && playing)step(value);};
        if(time===0)run();else timers.push(setTimer(run,time));
      });
      timers.push(setTimer(()=>{if(token!==generation)return;cancel();finish(true);},duration));
    }
    motion.addEventListener('change',()=>{if(motion.matches && playing)cancel(true);});
    return {play,cancel,get playing(){return playing;},get pending(){return timers.length;}};
  }
  function observeViewport(scene,onExit,Observer=root.IntersectionObserver){
    if(typeof Observer!=='function')return null;
    const observer=new Observer(entries=>{if(entries.some(e=>e.target===scene&&(!e.isIntersecting||e.intersectionRatio===0)))onExit();},{threshold:0});
    observer.observe(scene);return observer;
  }
  if(typeof module==='object'&&module.exports)module.exports={player,observeViewport};else root.AssayPlayback={player,observeViewport};
})(typeof window==='object'?window:globalThis);
