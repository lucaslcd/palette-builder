var Ex=Object.defineProperty,xx=Object.defineProperties;var Ix=Object.getOwnPropertyDescriptors;var a_=Object.getOwnPropertySymbols;var Tx=Object.prototype.hasOwnProperty,Mx=Object.prototype.propertyIsEnumerable;var l_=(t,n,e)=>n in t?Ex(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,p=(t,n)=>{for(var e in n||={})Tx.call(n,e)&&l_(t,e,n[e]);if(a_)for(var e of a_(n))Mx.call(n,e)&&l_(t,e,n[e]);return t},N=(t,n)=>xx(t,Ix(n));var Et=null,jl=!1,ir=1,Nx=null,Te=Symbol("SIGNAL");function F(t){let n=Et;return Et=t,n}function Ul(){return Et}var qn={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Yn(t){if(jl)throw new Error("");if(Et===null)return;Et.consumerOnSignalRead(t);let n=Et.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=Et.recomputing;if(i&&(e=n!==void 0?n.nextProducer:Et.producers,e!==void 0&&e.producer===t)){Et.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=ir;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===Et&&(!i||r.knownValidAtEpoch===ir))return;let o=ao(Et),s={producer:t,consumer:Et,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:ir,lastReadVersion:t.version,nextConsumer:void 0};Et.producersTail=s,n!==void 0?n.nextProducer=s:Et.producers=s,o&&h_(t,s)}function c_(){ir++}function sr(t){if(!(ao(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===ir)){if(!t.producerMustRecompute(t)&&!ar(t)){so(t);return}t.producerRecomputeValue(t),so(t)}}function Tf(t){if(t.consumers===void 0)return;let n=jl;jl=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||d_(i)}}finally{jl=n}}function Mf(){return Et?.consumerAllowSignalWrites!==!1}function d_(t){t.dirty=!0,Tf(t),t.consumerMarkedDirty?.(t)}function so(t){t.dirty=!1,t.lastCleanEpoch=ir}function Tn(t){return t&&u_(t),F(t)}function u_(t){if(t.producersTail?.knownValidAtEpoch===ir){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function Zn(t,n){F(n),t&&f_(t)}function f_(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(ao(t))do e=Nf(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function ar(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(sr(e),i!==e.version))return!0}return!1}function Kn(t){if(ao(t)){let n=t.producers;for(;n!==void 0;)n=Nf(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function h_(t,n){let e=t.consumersTail,i=ao(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)h_(r.producer,r)}function Nf(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!ao(n)){let o=n.producers;for(;o!==void 0;)o=Nf(o)}return e}function ao(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Ts(t){Nx?.(t)}function Ms(t,n){return Object.is(t,n)}function Ns(t,n){let e=Object.create(kx);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(sr(e),Yn(e),e.value===In)throw e.error;return e.value};return i[Te]=e,Ts(e),i}var rr=Symbol("UNSET"),or=Symbol("COMPUTING"),In=Symbol("ERRORED"),kx=N(p({},qn),{value:rr,dirty:!0,error:null,equal:Ms,kind:"computed",producerMustRecompute(t){return t.value===rr||t.value===or},producerRecomputeValue(t){if(t.value===or)throw new Error("");let n=t.value;t.value=or;let e=Tn(t),i,r=!1;try{i=t.computation(),F(null),r=n!==rr&&n!==In&&i!==In&&t.equal(n,i)}catch(o){i=In,t.error=o}finally{Zn(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Ax(){throw new Error}var m_=Ax;function p_(t){m_(t)}function kf(t){m_=t}var Rx=null;function Af(t,n){let e=Object.create(ks);e.value=t,n!==void 0&&(e.equal=n);let i=()=>g_(e);return i[Te]=e,Ts(e),[i,s=>wi(e,s),s=>Hl(e,s)]}function g_(t){return Yn(t),t.value}function wi(t,n){Mf()||p_(t),t.equal(t.value,n)||(t.value=n,Ox(t))}function Hl(t,n){Mf()||p_(t),wi(t,n(t.value))}var ks=N(p({},qn),{equal:Ms,value:void 0,kind:"signal"});function Ox(t){t.version++,c_(),Tf(t),Rx?.(t)}var Rf=N(p({},qn),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Of(t){if(t.dirty=!1,t.version>0&&!ar(t))return;t.version++;let n=Tn(t);try{t.cleanup(),t.fn()}finally{Zn(t,n)}}var Pf;function zl(){return Pf}function Mn(t){let n=Pf;return Pf=t,n}var v_=Symbol("NotFound");function lo(t){return t===v_||t?.name==="\u0275NotFound"}function Ff(t,n,e){let i=Object.create(Px);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(sr(i),Yn(i),i.value===In)throw i.error;return i.value};return o[Te]=i,Ts(i),o}function Lf(t,n){sr(t),wi(t,n),so(t)}function __(t,n){if(sr(t),t.value===In)throw t.error;Hl(t,n),so(t)}var Px=N(p({},qn),{value:rr,dirty:!0,error:null,equal:Ms,kind:"linkedSignal",producerMustRecompute(t){return t.value===rr||t.value===or},producerRecomputeValue(t){if(t.value===or)throw new Error("");let n=t.value;t.value=or;let e=Tn(t),i,r=!1;try{let o=t.source(),s=n!==rr&&n!==In,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,F(null),r=s&&i!==In&&t.equal(n,i)}catch(o){i=In,t.error=o}finally{Zn(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function b_(t){let n=F(null);try{return t()}finally{F(n)}}function ce(t){return typeof t=="function"}function co(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var $l=co(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function lr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var pe=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(ce(i))try{i()}catch(o){n=o instanceof $l?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{y_(o)}catch(s){n=n??[],s instanceof $l?n=[...n,...s.errors]:n.push(s)}}if(n)throw new $l(n)}}add(n){var e;if(n&&n!==this)if(this.closed)y_(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&lr(e,n)}remove(n){let{_finalizers:e}=this;e&&lr(e,n),n instanceof t&&n._removeParent(this)}};pe.EMPTY=(()=>{let t=new pe;return t.closed=!0,t})();var Vf=pe.EMPTY;function Wl(t){return t instanceof pe||t&&"closed"in t&&ce(t.remove)&&ce(t.add)&&ce(t.unsubscribe)}function y_(t){ce(t)?t():t.unsubscribe()}var dn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var uo={setTimeout(t,n,...e){let{delegate:i}=uo;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=uo;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Gl(t){uo.setTimeout(()=>{let{onUnhandledError:n}=dn;if(n)n(t);else throw t})}function As(){}var C_=Bf("C",void 0,void 0);function S_(t){return Bf("E",void 0,t)}function D_(t){return Bf("N",t,void 0)}function Bf(t,n,e){return{kind:t,value:n,error:e}}var cr=null;function fo(t){if(dn.useDeprecatedSynchronousErrorHandling){let n=!cr;if(n&&(cr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=cr;if(cr=null,e)throw i}}else t()}function w_(t){dn.useDeprecatedSynchronousErrorHandling&&cr&&(cr.errorThrown=!0,cr.error=t)}var dr=class extends pe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Wl(n)&&n.add(this)):this.destination=Vx}static create(n,e,i){return new Xn(n,e,i)}next(n){this.isStopped?Uf(D_(n),this):this._next(n)}error(n){this.isStopped?Uf(S_(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Uf(C_,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},Fx=Function.prototype.bind;function jf(t,n){return Fx.call(t,n)}var Hf=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){ql(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){ql(i)}else ql(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){ql(e)}}},Xn=class extends dr{constructor(n,e,i){super();let r;if(ce(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&dn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&jf(n.next,o),error:n.error&&jf(n.error,o),complete:n.complete&&jf(n.complete,o)}):r=n}this.destination=new Hf(r)}};function ql(t){dn.useDeprecatedSynchronousErrorHandling?w_(t):Gl(t)}function Lx(t){throw t}function Uf(t,n){let{onStoppedNotification:e}=dn;e&&uo.setTimeout(()=>e(t,n))}var Vx={closed:!0,next:As,error:Lx,complete:As};var ho=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Ft(t){return t}function zf(...t){return $f(t)}function $f(t){return t.length===0?Ft:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var J=class t{constructor(n){n&&(this._subscribe=n)}lift(n){let e=new t;return e.source=this,e.operator=n,e}subscribe(n,e,i){let r=jx(n)?n:new Xn(n,e,i);return fo(()=>{let{operator:o,source:s}=this;r.add(o?o.call(r,s):s?this._subscribe(r):this._trySubscribe(r))}),r}_trySubscribe(n){try{return this._subscribe(n)}catch(e){n.error(e)}}forEach(n,e){return e=E_(e),new e((i,r)=>{let o=new Xn({next:s=>{try{n(s)}catch(a){r(a),o.unsubscribe()}},error:r,complete:i});this.subscribe(o)})}_subscribe(n){var e;return(e=this.source)===null||e===void 0?void 0:e.subscribe(n)}[ho](){return this}pipe(...n){return $f(n)(this)}toPromise(n){return n=E_(n),new n((e,i)=>{let r;this.subscribe(o=>r=o,o=>i(o),()=>e(r))})}};J.create=t=>new J(t);function E_(t){var n;return(n=t??dn.Promise)!==null&&n!==void 0?n:Promise}function Bx(t){return t&&ce(t.next)&&ce(t.error)&&ce(t.complete)}function jx(t){return t&&t instanceof dr||Bx(t)&&Wl(t)}function Ux(t){return ce(t?.lift)}function ue(t){return n=>{if(Ux(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function fe(t,n,e,i,r){return new Wf(t,n,e,i,r)}var Wf=class extends dr{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(l){n.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var x_=co(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var S=class extends J{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let e=new Yl(this,this);return e.operator=n,e}_throwIfClosed(){if(this.closed)throw new x_}next(n){fo(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let e of this.currentObservers)e.next(n)}})}error(n){fo(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:e}=this;for(;e.length;)e.shift().error(n)}})}complete(){fo(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:e,isStopped:i,observers:r}=this;return e||i?Vf:(this.currentObservers=null,r.push(n),new pe(()=>{this.currentObservers=null,lr(r,n)}))}_checkFinalizedStatuses(n){let{hasError:e,thrownError:i,isStopped:r}=this;e?n.error(i):r&&n.complete()}asObservable(){let n=new J;return n.source=this,n}};S.create=(t,n)=>new Yl(t,n);var Yl=class extends S{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:Vf}};var Xe=class extends S{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Rs={now(){return(Rs.delegate||Date).now()},delegate:void 0};var Ei=class extends S{constructor(n=1/0,e=1/0,i=Rs){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let l=1;l<i.length&&i[l]<=s;l+=2)a=l;a&&i.splice(0,a+1)}}};var Zl=class extends pe{constructor(n,e){super()}schedule(n,e=0){return this}};var Os={setInterval(t,n,...e){let{delegate:i}=Os;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Os;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var Kl=class extends Zl{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Os.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Os.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,lr(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Gf=(()=>{class t{constructor(e,i=t.now){this.schedulerActionCtor=e,this.now=i}schedule(e,i=0,r){return new this.schedulerActionCtor(this,e).schedule(r,i)}}return t.now=Rs.now,t})();var Xl=class extends Gf{constructor(n,e=Gf.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Ps=new Xl(Kl),I_=Ps;var He=new J(t=>t.complete());function Ql(t){return t&&ce(t.schedule)}function qf(t){return t[t.length-1]}function Jl(t){return ce(qf(t))?t.pop():void 0}function Nn(t){return Ql(qf(t))?t.pop():void 0}function T_(t,n){return typeof qf(t)=="number"?t.pop():n}function N_(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(u){try{c(i.next(u))}catch(f){s(f)}}function l(u){try{c(i.throw(u))}catch(f){s(f)}}function c(u){u.done?o(u.value):r(u.value).then(a,l)}c((i=i.apply(t,n||[])).next())})}function M_(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function ur(t){return this instanceof ur?(this.v=t,this):new ur(t)}function k_(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(m){return function(v){return Promise.resolve(v).then(m,f)}}function a(m,v){i[m]&&(r[m]=function(D){return new Promise(function(A,ne){o.push([m,D,A,ne])>1||l(m,D)})},v&&(r[m]=v(r[m])))}function l(m,v){try{c(i[m](v))}catch(D){h(o[0][3],D)}}function c(m){m.value instanceof ur?Promise.resolve(m.value.v).then(u,f):h(o[0][2],m)}function u(m){l("next",m)}function f(m){l("throw",m)}function h(m,v){m(v),o.shift(),o.length&&l(o[0][0],o[0][1])}}function A_(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof M_=="function"?M_(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,l){s=t[o](s),r(a,l,s.done,s.value)})}}function r(o,s,a,l){Promise.resolve(l).then(function(c){o({value:c,done:a})},s)}}var ec=(t=>t&&typeof t.length=="number"&&typeof t!="function");function tc(t){return ce(t?.then)}function nc(t){return ce(t[ho])}function ic(t){return Symbol.asyncIterator&&ce(t?.[Symbol.asyncIterator])}function rc(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function Hx(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var oc=Hx();function sc(t){return ce(t?.[oc])}function ac(t){return k_(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield ur(e.read());if(r)return yield ur(void 0);yield yield ur(i)}}finally{e.releaseLock()}})}function lc(t){return ce(t?.getReader)}function Ee(t){if(t instanceof J)return t;if(t!=null){if(nc(t))return zx(t);if(ec(t))return $x(t);if(tc(t))return Wx(t);if(ic(t))return R_(t);if(sc(t))return Gx(t);if(lc(t))return qx(t)}throw rc(t)}function zx(t){return new J(n=>{let e=t[ho]();if(ce(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function $x(t){return new J(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function Wx(t){return new J(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Gl)})}function Gx(t){return new J(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function R_(t){return new J(n=>{Yx(t,n).catch(e=>n.error(e))})}function qx(t){return R_(ac(t))}function Yx(t,n){var e,i,r,o;return N_(this,void 0,void 0,function*(){try{for(e=A_(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Rt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function cc(t,n=0){return ue((e,i)=>{e.subscribe(fe(i,r=>Rt(i,t,()=>i.next(r),n),()=>Rt(i,t,()=>i.complete(),n),r=>Rt(i,t,()=>i.error(r),n)))})}function dc(t,n=0){return ue((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function O_(t,n){return Ee(t).pipe(dc(n),cc(n))}function P_(t,n){return Ee(t).pipe(dc(n),cc(n))}function F_(t,n){return new J(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function L_(t,n){return new J(e=>{let i;return Rt(e,n,()=>{i=t[oc](),Rt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>ce(i?.return)&&i.return()})}function uc(t,n){if(!t)throw new Error("Iterable cannot be null");return new J(e=>{Rt(e,n,()=>{let i=t[Symbol.asyncIterator]();Rt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function V_(t,n){return uc(ac(t),n)}function B_(t,n){if(t!=null){if(nc(t))return O_(t,n);if(ec(t))return F_(t,n);if(tc(t))return P_(t,n);if(ic(t))return uc(t,n);if(sc(t))return L_(t,n);if(lc(t))return V_(t,n)}throw rc(t)}function Oe(t,n){return n?B_(t,n):Ee(t)}function H(...t){let n=Nn(t);return Oe(t,n)}function Fs(t,n){let e=ce(t)?t:()=>t,i=r=>r.error(e());return new J(n?r=>n.schedule(i,0,r):i)}function Ls(t){return!!t&&(t instanceof J||ce(t.lift)&&ce(t.subscribe))}var fr=co(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function j_(t){return t instanceof Date&&!isNaN(t)}function ie(t,n){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:Zx}=Array;function Kx(t,n){return Zx(n)?t(...n):t(n)}function fc(t){return ie(n=>Kx(t,n))}var{isArray:Xx}=Array,{getPrototypeOf:Qx,prototype:Jx,keys:eI}=Object;function hc(t){if(t.length===1){let n=t[0];if(Xx(n))return{args:n,keys:null};if(tI(n)){let e=eI(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function tI(t){return t&&typeof t=="object"&&Qx(t)===Jx}function mc(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Vs(...t){let n=Nn(t),e=Jl(t),{args:i,keys:r}=hc(t);if(i.length===0)return Oe([],n);let o=new J(nI(i,n,r?s=>mc(r,s):Ft));return e?o.pipe(fc(e)):o}function nI(t,n,e=Ft){return i=>{U_(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let l=0;l<r;l++)U_(n,()=>{let c=Oe(t[l],n),u=!1;c.subscribe(fe(i,f=>{o[l]=f,u||(u=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function U_(t,n,e){t?Rt(e,t,n):n()}function H_(t,n,e,i,r,o,s,a){let l=[],c=0,u=0,f=!1,h=()=>{f&&!l.length&&!c&&n.complete()},m=D=>c<i?v(D):l.push(D),v=D=>{o&&n.next(D),c++;let A=!1;Ee(e(D,u++)).subscribe(fe(n,ne=>{r?.(ne),o?m(ne):n.next(ne)},()=>{A=!0},void 0,()=>{if(A)try{for(c--;l.length&&c<i;){let ne=l.shift();s?Rt(n,s,()=>v(ne)):v(ne)}h()}catch(ne){n.error(ne)}}))};return t.subscribe(fe(n,m,()=>{f=!0,h()})),()=>{a?.()}}function xt(t,n,e=1/0){return ce(n)?xt((i,r)=>ie((o,s)=>n(i,o,r,s))(Ee(t(i,r))),e):(typeof n=="number"&&(e=n),ue((i,r)=>H_(i,r,t,e)))}function pc(t=1/0){return xt(Ft,t)}function z_(){return pc(1)}function xi(...t){return z_()(Oe(t,Nn(t)))}function un(t){return new J(n=>{Ee(t()).subscribe(n)})}function Bs(...t){let n=Jl(t),{args:e,keys:i}=hc(t),r=new J(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),l=s,c=s;for(let u=0;u<s;u++){let f=!1;Ee(e[u]).subscribe(fe(o,h=>{f||(f=!0,c--),a[u]=h},()=>l--,void 0,()=>{(!l||!f)&&(c||o.next(i?mc(i,a):a),o.complete())}))}});return n?r.pipe(fc(n)):r}function $_(t=0,n,e=I_){let i=-1;return n!=null&&(Ql(n)?e=n:i=n),new J(r=>{let o=j_(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Qn(...t){let n=Nn(t),e=T_(t,1/0),i=t;return i.length?i.length===1?Ee(i[0]):pc(e)(Oe(i,n)):He}function _e(t,n){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>t.call(n,o,r++)&&i.next(o)))})}function W_(t){return ue((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}s&&e.complete()},l=()=>{o=null,s&&e.complete()};n.subscribe(fe(e,c=>{i=!0,r=c,o||Ee(t(c)).subscribe(o=fe(e,a,l))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function gc(t,n=Ps){return W_(()=>$_(t,n))}function hr(t){return ue((n,e)=>{let i=null,r=!1,o;i=n.subscribe(fe(e,void 0,void 0,s=>{o=Ee(t(s,hr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function mo(t,n){return ce(n)?xt(t,n,1):xt(t,1)}function js(t,n=Ps){return ue((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=s+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}a()}e.subscribe(fe(i,c=>{o=c,s=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function G_(t){return ue((n,e)=>{let i=!1;n.subscribe(fe(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function ze(t){return t<=0?()=>He:ue((n,e)=>{let i=0;n.subscribe(fe(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function vc(t,n=Ft){return t=t??iI,ue((e,i)=>{let r,o=!0;e.subscribe(fe(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function iI(t,n){return t===n}function q_(t=rI){return ue((n,e)=>{let i=!1;n.subscribe(fe(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function rI(){return new fr}function mr(t){return ue((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function Jn(t,n){let e=arguments.length>=2;return i=>i.pipe(t?_e((r,o)=>t(r,o,i)):Ft,ze(1),e?G_(n):q_(()=>new fr))}function _c(t){return t<=0?()=>He:ue((n,e)=>{let i=[];n.subscribe(fe(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function bc(){return ue((t,n)=>{let e,i=!1;t.subscribe(fe(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function Us(t={}){let{connector:n=()=>new S,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,l,c=0,u=!1,f=!1,h=()=>{a?.unsubscribe(),a=void 0},m=()=>{h(),s=l=void 0,u=f=!1},v=()=>{let D=s;m(),D?.unsubscribe()};return ue((D,A)=>{c++,!f&&!u&&h();let ne=l=l??n();A.add(()=>{c--,c===0&&!f&&!u&&(a=Yf(v,r))}),ne.subscribe(A),!s&&c>0&&(s=new Xn({next:ke=>ne.next(ke),error:ke=>{f=!0,h(),a=Yf(m,e,ke),ne.error(ke)},complete:()=>{u=!0,h(),a=Yf(m,i),ne.complete()}}),Ee(D).subscribe(s))})(o)}}function Yf(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new Xn({next:()=>{i.unsubscribe(),t()}});return Ee(n(...e)).subscribe(i)}function yc(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,Us({connector:()=>new Ei(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function Hs(t){return _e((n,e)=>t<=e)}function It(...t){let n=Nn(t);return ue((e,i)=>{(n?xi(t,e,n):xi(t,e)).subscribe(i)})}function ct(t,n){return ue((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(fe(i,l=>{r?.unsubscribe();let c=0,u=o++;Ee(t(l,u)).subscribe(r=fe(i,f=>i.next(n?n(l,f,u,c++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function be(t){return ue((n,e)=>{Ee(t).subscribe(fe(e,()=>e.complete(),As)),!e.closed&&n.subscribe(e)})}function Zf(t,n=!1){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function tt(t,n,e){let i=ce(t)||n||e?{next:t,error:n,complete:e}:t;return i?ue((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(fe(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;a=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;a=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;a&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Ft}var Ic="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",y=class extends Error{code;constructor(n,e){super(An(n,e)),this.code=n}};function oI(t){return`NG0${Math.abs(t)}`}function An(t,n){return`${oI(t)}${n?": "+n:""}`}function ye(t){for(let n in t)if(t[n]===ye)return n;throw Error("")}function Q_(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function Ys(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(Ys).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Tc(t,n){return t?n?`${t} ${n}`:t:n||""}var sI=ye({__forward_ref__:ye});function Ot(t){return t.__forward_ref__=Ot,t}function Qe(t){return lh(t)?t():t}function lh(t){return typeof t=="function"&&t.hasOwnProperty(sI)&&t.__forward_ref__===Ot}function se(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function W(t){return{providers:t.providers||[],imports:t.imports||[]}}function Zs(t){return aI(t,Mc)}function ch(t){return Zs(t)!==null}function aI(t,n){return t.hasOwnProperty(n)&&t[n]||null}function lI(t){let n=t?.[Mc]??null;return n||null}function Xf(t){return t&&t.hasOwnProperty(Sc)?t[Sc]:null}var Mc=ye({\u0275prov:ye}),Sc=ye({\u0275inj:ye}),g=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=se({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function dh(t){return t&&!!t.\u0275providers}var Ks=ye({\u0275cmp:ye}),Xs=ye({\u0275dir:ye}),uh=ye({\u0275pipe:ye}),fh=ye({\u0275mod:ye}),$s=ye({\u0275fac:ye}),yr=ye({__NG_ELEMENT_ID__:ye}),Y_=ye({__NG_ENV_ID__:ye});function J_(t){return kc(t,"@NgModule"),t[fh]||null}function Ti(t){return kc(t,"@Component"),t[Ks]||null}function Nc(t){return kc(t,"@Directive"),t[Xs]||null}function eb(t){return kc(t,"@Pipe"),t[uh]||null}function kc(t,n){if(t==null)throw new y(-919,!1)}function hh(t){return typeof t=="string"?t:t==null?"":String(t)}var tb=ye({ngErrorCode:ye}),cI=ye({ngErrorMessage:ye}),dI=ye({ngTokenPath:ye});function mh(t,n){return nb("",-200,n)}function Ac(t,n){throw new y(-201,!1)}function nb(t,n,e){let i=new y(n,t);return i[tb]=n,i[cI]=t,e&&(i[dI]=e),i}function uI(t){return t[tb]}var Qf;function ib(){return Qf}function Lt(t){let n=Qf;return Qf=t,n}function ph(t,n,e){let i=Zs(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Ac(t,"")}var vo=globalThis;var fI={},pr=fI,hI="__NG_DI_FLAG__",Jf=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=gr(e)||0;try{return this.injector.get(n,i&8?null:pr,i)}catch(r){if(lo(r))return r;throw r}}};function mI(t,n=0){let e=zl();if(e===void 0)throw new y(-203,!1);if(e===null)return ph(t,void 0,n);{let i=pI(n),r=e.retrieve(t,i);if(lo(r)){if(i.optional)return null;throw r}return r}}function L(t,n=0){return(ib()||mI)(Qe(t),n)}function d(t,n){return L(t,gr(n))}function gr(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function pI(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function eh(t){let n=[];for(let e=0;e<t.length;e++){let i=Qe(t[e]);if(Array.isArray(i)){if(i.length===0)throw new y(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],l=gI(a);typeof l=="number"?l===-1?r=a.token:o|=l:r=a}n.push(L(r,o))}else n.push(L(i))}return n}function gI(t){return t[hI]}function vr(t,n){let e=t.hasOwnProperty($s);return e?t[$s]:null}function rb(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function ob(t){return t.flat(Number.POSITIVE_INFINITY)}function Rc(t,n){t.forEach(e=>Array.isArray(e)?Rc(e,n):n(e))}function gh(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Qs(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function sb(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function ab(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Oc(t,n,e){let i=_o(t,n);return i>=0?t[i|1]=e:(i=~i,ab(t,i,n,e)),i}function Pc(t,n){let e=_o(t,n);if(e>=0)return t[e|1]}function _o(t,n){return vI(t,n,1)}function vI(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Mi={},gt=[],Cr=new g(""),Js=new g("",-1),vh=new g(""),go=class{get(n,e=pr){if(e===pr){let r=nb("",-201);throw r.name="\u0275NotFound",r}return e}};function Rn(t){return{\u0275providers:t}}function lb(t){return Rn([{provide:Cr,multi:!0,useValue:t}])}function cb(...t){return{\u0275providers:_h(!0,t),\u0275fromNgModule:!0}}function _h(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return Rc(n,s=>{let a=s;Dc(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&db(r,o),e}function db(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];bh(r,o=>{n(o,i)})}}function Dc(t,n,e,i){if(t=Qe(t),!t)return!1;let r=null,o=Xf(t),s=!o&&Ti(t);if(!o&&!s){let l=t.ngModule;if(o=Xf(l),o)r=l;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let l=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let c of l)Dc(c,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let c;Rc(o.imports,u=>{Dc(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&db(c,n)}if(!a){let c=vr(r)||(()=>new r);n({provide:r,useFactory:c,deps:gt},r),n({provide:vh,useValue:r,multi:!0},r),n({provide:Cr,useValue:()=>L(r),multi:!0},r)}let l=o.providers;if(l!=null&&!a){let c=t;bh(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function bh(t,n){for(let e of t)dh(e)&&(e=e.\u0275providers),Array.isArray(e)?bh(e,n):n(e)}var _I=ye({provide:String,useValue:ye});function ub(t){return t!==null&&typeof t=="object"&&_I in t}function bI(t){return!!(t&&t.useExisting)}function yI(t){return!!(t&&t.useFactory)}function _r(t){return typeof t=="function"}function fb(t){return!!t.useClass}var ea=new g(""),Cc={},Z_={},Kf;function bo(){return Kf===void 0&&(Kf=new go),Kf}var we=class{},br=class extends we{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,nh(n,s=>this.processProvider(s)),this.records.set(Js,po(void 0,this)),r.has("environment")&&this.records.set(we,po(void 0,this));let o=this.records.get(ea);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(vh,gt,{self:!0}))}retrieve(n,e){let i=gr(e)||0;try{return this.get(n,pr,i)}catch(r){if(lo(r))return r;throw r}}destroy(){zs(this),this._destroyed=!0;let n=F(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),F(n)}}onDestroy(n){return zs(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){zs(this);let e=Mn(this),i=Lt(void 0),r;try{return n()}finally{Mn(e),Lt(i)}}get(n,e=pr,i){if(zs(this),n.hasOwnProperty(Y_))return n[Y_](this);let r=gr(i),o,s=Mn(this),a=Lt(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=EI(n)&&Zs(n);u&&this.injectableDefInScope(u)?c=po(th(n),Cc):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?bo():this.parent;return e=r&8&&e===pr?null:e,l.get(n,e)}catch(l){let c=uI(l);throw c===-200||c===-201?new y(c,null):l}finally{Lt(a),Mn(s)}}resolveInjectorInitializers(){let n=F(null),e=Mn(this),i=Lt(void 0),r;try{let o=this.get(Cr,gt,{self:!0});for(let s of o)s()}finally{Mn(e),Lt(i),F(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Qe(n);let e=_r(n)?n:Qe(n&&n.provide),i=SI(n);if(!_r(n)&&n.multi===!0){let r=this.records.get(e);r||(r=po(void 0,Cc,!0),r.factory=()=>eh(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=F(null);try{if(e.value===Z_)throw mh("");return e.value===Cc&&(e.value=Z_,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&wI(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{F(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Qe(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function th(t){let n=Zs(t),e=n!==null?n.factory:vr(t);if(e!==null)return e;if(t instanceof g)throw new y(-204,!1);if(t instanceof Function)return CI(t);throw new y(-204,!1)}function CI(t){if(t.length>0)throw new y(-204,!1);let e=lI(t);return e!==null?()=>e.factory(t):()=>new t}function SI(t){if(ub(t))return po(void 0,t.useValue);{let n=yh(t);return po(n,Cc)}}function yh(t,n,e){let i;if(_r(t)){let r=Qe(t);return vr(r)||th(r)}else if(ub(t))i=()=>Qe(t.useValue);else if(yI(t))i=()=>t.useFactory(...eh(t.deps||[]));else if(bI(t))i=(r,o)=>L(Qe(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Qe(t&&(t.useClass||t.provide));if(DI(t))i=()=>new r(...eh(t.deps));else return vr(r)||th(r)}return i}function zs(t){if(t.destroyed)throw new y(-205,!1)}function po(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function DI(t){return!!t.deps}function wI(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function EI(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function nh(t,n){for(let e of t)Array.isArray(e)?nh(e,n):e&&dh(e)?nh(e.\u0275providers,n):n(e)}function Le(t,n){let e;t instanceof br?(zs(t),e=t):e=new Jf(t);let i,r=Mn(e),o=Lt(void 0);try{return n()}finally{Mn(r),Lt(o)}}function hb(){return ib()!==void 0||zl()!=null}var fn=0,V=1,$=2,Je=3,qt=4,_t=5,Sr=6,yo=7,$e=8,On=9,Pn=10,Me=11,Co=12,Ch=13,Ni=14,Tt=15,ki=16,Dr=17,Fn=18,Ln=19,Sh=20,ei=21,Fc=22,Ii=23,Vt=24,wr=25,Vn=26,We=27,mb=1,Dh=6,Er=7,ta=8,xr=9,Ve=10;function ni(t){return Array.isArray(t)&&typeof t[mb]=="object"}function Yt(t){return Array.isArray(t)&&t[mb]===!0}function wh(t){return(t.flags&4)!==0}function ii(t){return t.componentOffset>-1}function na(t){return(t.flags&1)===1}function hn(t){return!!t.template}function So(t){return(t[$]&512)!==0}function Ir(t){return(t[$]&256)===256}var nt=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(nt||{});var Eh="svg",pb="math";function dt(t){for(;Array.isArray(t);)t=t[fn];return t}function xh(t,n){return dt(n[t])}function Zt(t,n){return dt(n[t.index])}function Lc(t,n){return t.data[n]}function gb(t,n){return t[n]}function Kt(t,n){let e=n[t];return ni(e)?e:e[fn]}function vb(t){return(t[$]&4)===4}function Vc(t){return(t[$]&128)===128}function _b(t){return Yt(t[Je])}function Xt(t,n){return n==null?null:t[n]}function Ih(t){t[Dr]=0}function Th(t){t[$]&1024||(t[$]|=1024,Vc(t)&&Tr(t))}function bb(t,n){for(;t>0;)n=n[Ni],t--;return n}function ia(t){return!!(t[$]&9216||t[Vt]?.dirty)}function Bc(t){t[Pn].changeDetectionScheduler?.notify(8),t[$]&64&&(t[$]|=1024),ia(t)&&Tr(t)}function Tr(t){t[Pn].changeDetectionScheduler?.notify(0);let n=ti(t);for(;n!==null&&!(n[$]&8192||(n[$]|=8192,!Vc(n)));)n=ti(n)}function jc(t,n){if(Ir(t))throw new y(911,!1);t[ei]===null&&(t[ei]=[]),t[ei].push(n)}function yb(t,n){if(t[ei]===null)return;let e=t[ei].indexOf(n);e!==-1&&t[ei].splice(e,1)}function ti(t){let n=t[Je];return Yt(n)?n[Je]:n}function Mh(t){return t[yo]??=[]}function Nh(t){return t.cleanup??=[]}function Cb(t,n,e,i){let r=Mh(n);r.push(e),t.firstCreatePass&&Nh(t).push(i,r.length-1)}var he={lFrame:Rb(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var ih=!1;function Sb(){return he.lFrame.elementDepthCount}function Db(){he.lFrame.elementDepthCount++}function kh(){he.lFrame.elementDepthCount--}function Ah(){return he.bindingsEnabled}function Rh(){return he.skipHydrationRootTNode!==null}function Oh(t){return he.skipHydrationRootTNode===t}function Ph(){he.skipHydrationRootTNode=null}function ae(){return he.lFrame.lView}function Be(){return he.lFrame.tView}function Bt(t){return he.lFrame.contextLView=t,t[$e]}function jt(t){return he.lFrame.contextLView=null,t}function it(){let t=Fh();for(;t!==null&&t.type===64;)t=t.parent;return t}function Fh(){return he.lFrame.currentTNode}function wb(){let t=he.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Do(t,n){let e=he.lFrame;e.currentTNode=t,e.isParent=n}function Lh(){return he.lFrame.isParent}function Vh(){he.lFrame.isParent=!1}function Eb(){return he.lFrame.contextLView}function Bh(){return ih}function Ws(t){let n=ih;return ih=t,n}function xb(){let t=he.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function Ib(t){return he.lFrame.bindingIndex=t}function Mr(){return he.lFrame.bindingIndex++}function jh(t){let n=he.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Tb(){return he.lFrame.inI18n}function Mb(t,n){let e=he.lFrame;e.bindingIndex=e.bindingRootIndex=t,Uc(n)}function Nb(){return he.lFrame.currentDirectiveIndex}function Uc(t){he.lFrame.currentDirectiveIndex=t}function kb(t){let n=he.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Hc(){return he.lFrame.currentQueryIndex}function ra(t){he.lFrame.currentQueryIndex=t}function xI(t){let n=t[V];return n.type===2?n.declTNode:n.type===1?t[_t]:null}function Uh(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=xI(o),r===null||(o=o[Ni],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=he.lFrame=Ab();return i.currentTNode=n,i.lView=t,!0}function zc(t){let n=Ab(),e=t[V];he.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Ab(){let t=he.lFrame,n=t===null?null:t.child;return n===null?Rb(t):n}function Rb(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function Ob(){let t=he.lFrame;return he.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Hh=Ob;function $c(){let t=Ob();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Pb(t){return(he.lFrame.contextLView=bb(t,he.lFrame.contextLView))[$e]}function ri(){return he.lFrame.selectedIndex}function Ai(t){he.lFrame.selectedIndex=t}function oa(){let t=he.lFrame;return Lc(t.tView,t.selectedIndex)}function wo(){he.lFrame.currentNamespace=Eh}function Wc(){II()}function II(){he.lFrame.currentNamespace=null}function zh(){return he.lFrame.currentNamespace}var Fb=!0;function Gc(){return Fb}function qc(t){Fb=t}function rh(t,n=null,e=null,i){let r=$h(t,n,e,i);return r.resolveInjectorInitializers(),r}function $h(t,n=null,e=null,i,r=new Set){let o=[e||gt,cb(t)],s;return new br(o,n||bo(),s||null,r)}var P=class t{static THROW_IF_NOT_FOUND=pr;static NULL=new go;static create(n,e){if(Array.isArray(n))return rh({name:""},e,n,"");{let i=n.name??"";return rh({name:i},n.parent,n.providers,i)}}static \u0275prov=se({token:t,providedIn:"any",factory:()=>L(Js)});static __NG_ELEMENT_ID__=-1},R=new g(""),Ue=class{static __NG_ELEMENT_ID__=TI;static __NG_ENV_ID__=n=>n},wc=class extends Ue{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Ir(this._lView)}onDestroy(n){let e=this._lView;return jc(e,n),()=>yb(e,n)}};function TI(){return new wc(ae())}var Lb=!1,Vb=new g(""),oi=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Xe(!1);debugTaskTracker=d(Vb,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new J(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=se({token:t,providedIn:"root",factory:()=>new t})}return t})(),oh=class extends S{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,hb()&&(this.destroyRef=d(Ue,{optional:!0})??void 0,this.pendingTasks=d(oi,{optional:!0})??void 0)}emit(n){let e=F(null);try{super.next(n)}finally{F(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),s=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof pe&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},U=oh;function Ec(...t){}function Wh(t){let n,e;function i(){t=Ec;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function Bb(t){return queueMicrotask(()=>t()),()=>{t=Ec}}var Gh="isAngularZone",Gs=Gh+"_ID",MI=0,I=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new U(!1);onMicrotaskEmpty=new U(!1);onStable=new U(!1);onError=new U(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=Lb}=n;if(typeof Zone>"u")throw new y(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,AI(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Gh)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new y(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new y(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,NI,Ec,Ec);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},NI={};function qh(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function kI(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Wh(()=>{t.callbackScheduled=!1,sh(t),t.isCheckStableRunning=!0,qh(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),sh(t)}function AI(t){let n=()=>{kI(t)},e=MI++;t._inner=t._inner.fork({name:"angular",properties:{[Gh]:!0,[Gs]:e,[Gs+e]:!0},onInvokeTask:(i,r,o,s,a,l)=>{if(RI(l))return i.invokeTask(o,s,a,l);try{return K_(t),i.invokeTask(o,s,a,l)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),X_(t)}},onInvoke:(i,r,o,s,a,l,c)=>{try{return K_(t),i.invoke(o,s,a,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!OI(l)&&n(),X_(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,sh(t),qh(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function sh(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function K_(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function X_(t){t._nesting--,qh(t)}var qs=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new U;onMicrotaskEmpty=new U;onStable=new U;onError=new U;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function RI(t){return jb(t,"__ignore_ng_zone__")}function OI(t){return jb(t,"__scheduler_tick__")}function jb(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var vt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Qt=new g("",{factory:()=>{let t=d(I),n=d(we),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(vt),e.handleError(i))})}}}),Ub={provide:Cr,useValue:()=>{let t=d(vt,{optional:!0})},multi:!0},PI=new g("",{factory:()=>{let t=d(R).defaultView;if(!t)return;let n=d(Qt),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(Ue).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function Yh(){return Rn([lb(()=>{d(PI)})])}function z(t,n){let[e,i,r]=Af(t,n?.equal),o=e,s=o[Te];return o.set=i,o.update=r,o.asReadonly=sa.bind(o),o}function sa(){let t=this[Te];if(t.readonlyFn===void 0){let n=()=>this();n[Te]=t,t.readonlyFn=n}return t.readonlyFn}var mn=new g("",{factory:()=>FI}),FI="ng";var Yc=new g(""),Nr=new g("",{providedIn:"platform",factory:()=>"unknown"}),aa=new g(""),si=new g("",{factory:()=>d(R).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Eo=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=LI}return t})();function LI(){return new Eo(ae(),it())}var kn=class{},la=new g("",{factory:()=>!0});var Zh=new g(""),Zc=(()=>{class t{static \u0275prov=se({token:t,providedIn:"root",factory:()=>new ah})}return t})(),ah=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},xc=class{[Te];constructor(n){this[Te]=n}destroy(){this[Te].destroy()}};function et(t,n){let e=n?.injector??d(P),i=n?.manualCleanup!==!0?e.get(Ue):null,r,o=e.get(Eo,null,{optional:!0}),s=e.get(kn);return o!==null?(r=jI(o.view,s,t),i instanceof wc&&i._lView===o.view&&(i=null)):r=UI(t,e.get(Zc),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new xc(r)}var Hb=N(p({},Rf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=Ws(!1);try{Of(this)}finally{Ws(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=F(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],F(t)}}}),VI=N(p({},Hb),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Kn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),BI=N(p({},Hb),{consumerMarkedDirty(){this.view[$]|=8192,Tr(this.view),this.notifier.notify(13)},destroy(){if(Kn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Ii]?.delete(this)}});function jI(t,n,e){let i=Object.create(BI);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=zb(i,e),t[Ii]??=new Set,t[Ii].add(i),i.consumerMarkedDirty(i),i}function UI(t,n,e){let i=Object.create(VI);return i.fn=zb(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function zb(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Ut(t){return typeof t=="function"&&t[Te]!==void 0}var ca=(()=>{class t{internalPendingTasks=d(oi);scheduler=d(kn);errorHandler=d(Qt);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=se({token:t,providedIn:"root",factory:()=>new t})}return t})();function ya(t){return{toString:t}.toString()}var ge=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(ge||{}),id=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function ky(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Ay=null,Ge=(()=>{Ay=$b;let t=()=>$b;return t.ngInherit=!0,t})();function QI(){return Ay}function $b(t){return t.type.prototype.ngOnChanges&&(t.setInput=eT),JI}function JI(){let t=Ry(this),n=t?.current;if(n){let e=t.previous;if(e===Mi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function eT(t,n,e,i,r){let o=this.declaredInputs[i],s=Ry(t)||tT(t,{previous:Mi,current:null}),a=s.current||(s.current={}),l=s.previous,c=l[o];a[o]=new id(c&&c.currentValue,e,l===Mi),ky(t,n,r,e)}var sm="__ngSimpleChanges__";function Ry(t){return Object.hasOwn(t,sm)&&t[sm]||null}function tT(t,n){return t[sm]=n}var Wb=[];var Ce=function(t,n=null,e){for(let i=0;i<Wb.length;i++){let r=Wb[i];r(t,n,e)}};function nT(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=QI()(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function Oy(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function Jc(t,n,e){Py(t,n,3,e)}function ed(t,n,e,i){(t[$]&3)===e&&Py(t,n,e,i)}function Kh(t,n){let e=t[$];(e&3)===n&&(e&=16383,e+=1,t[$]=e)}function Py(t,n,e,i){let r=i!==void 0?t[Dr]&65535:0,o=i??-1,s=n.length-1,a=0;for(let l=r;l<s;l++)if(typeof n[l+1]=="number"){if(a=n[l],i!=null&&a>=i)break}else n[l]<0&&(t[Dr]+=65536),(a<o||o==-1)&&(iT(t,e,n,l),t[Dr]=(t[Dr]&4294901760)+l+2),l++}function Gb(t,n){Ce(ge.LifecycleHookStart,t,n);let e=F(null);try{n.call(t)}finally{F(e),Ce(ge.LifecycleHookEnd,t,n)}}function iT(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[$]>>14<t[Dr]>>16&&(t[$]&3)===n&&(t[$]+=16384,Gb(a,o)):Gb(a,o)}var Io=-1,Rr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function rT(t){return(t.flags&8)!==0}function oT(t){return(t.flags&16)!==0}function sT(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];aT(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function Fy(t){return t===3||t===4||t===6}function aT(t){return t.charCodeAt(0)===64}function To(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?qb(t,e,r,null,n[++i]):qb(t,e,r,null,null))}}return t}function qb(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function Ly(t){return t!==Io}function rd(t){return t&32767}function lT(t){return t>>16}function od(t,n){let e=lT(t),i=n;for(;e>0;)i=i[Ni],e--;return i}var am=!0;function Yb(t){let n=am;return am=t,n}var cT=256,Vy=cT-1,By=5,dT=0,Bn={};function uT(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(yr)&&(i=e[yr]),i==null&&(i=e[yr]=dT++);let r=i&Vy,o=1<<r;n.data[t+(r>>By)]|=o}function sd(t,n){let e=jy(t,n);if(e!==-1)return e;let i=n[V];i.firstCreatePass&&(t.injectorIndex=n.length,Xh(i.data,t),Xh(n,null),Xh(i.blueprint,null));let r=Hm(t,n),o=t.injectorIndex;if(Ly(r)){let s=rd(r),a=od(r,n),l=a[V].data;for(let c=0;c<8;c++)n[o+c]=a[s+c]|l[s+c]}return n[o+8]=r,o}function Xh(t,n){t.push(0,0,0,0,0,0,0,0,n)}function jy(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Hm(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=Wy(r),i===null)return Io;if(e++,r=r[Ni],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Io}function lm(t,n,e){uT(t,n,e)}function fT(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Fy(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function Uy(t,n,e){if(e&8||t!==void 0)return t;Ac(n,"NodeInjector")}function Hy(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[On],o=Lt(void 0);try{return r?r.get(n,i,e&8):ph(n,i,e&8)}finally{Lt(o)}}return Uy(i,n,e)}function zy(t,n,e,i=0,r){if(t!==null){if(n[$]&2048&&!(i&2)){let s=gT(t,n,e,i,Bn);if(s!==Bn)return s}let o=$y(t,n,e,i,Bn);if(o!==Bn)return o}return Hy(n,e,i,r)}function $y(t,n,e,i,r){let o=mT(e);if(typeof o=="function"){if(!Uh(n,t,i))return i&1?Uy(r,e,i):Hy(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Ac(e);else return s}finally{Hh()}}else if(typeof o=="number"){let s=null,a=jy(t,n),l=Io,c=i&1?n[Tt][_t]:null;for((a===-1||i&4)&&(l=a===-1?Hm(t,n):n[a+8],l===Io||!Kb(i,!1)?a=-1:(s=n[V],a=rd(l),n=od(l,n)));a!==-1;){let u=n[V];if(Zb(o,a,u.data)){let f=hT(a,n,e,s,i,c);if(f!==Bn)return f}l=n[a+8],l!==Io&&Kb(i,n[V].data[a+8]===c)&&Zb(o,a,n)?(s=u,a=rd(l),n=od(l,n)):a=-1}}return r}function hT(t,n,e,i,r,o){let s=n[V],a=s.data[t+8],l=i==null?ii(a)&&am:i!=s&&(a.type&3)!==0,c=r&1&&o===a,u=td(a,s,e,l,c);return u!==null?ha(n,s,u,a,r):Bn}function td(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,f=i?a:a+u,h=r?a+u:c;for(let m=f;m<h;m++){let v=s[m];if(m<l&&e===v||m>=l&&v.type===e)return m}if(r){let m=s[l];if(m&&hn(m)&&m.type===e)return l}return null}function ha(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof Rr){let a=o;if(a.resolving)throw mh("");let l=Yb(a.canSeeViewProviders);a.resolving=!0;let c=s[e].type||s[e],u,f=a.injectImpl?Lt(a.injectImpl):null,h=Uh(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&nT(e,s[e],n)}finally{f!==null&&Lt(f),Yb(l),a.resolving=!1,Hh()}}return o}function mT(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(yr)?t[yr]:void 0;return typeof n=="number"?n>=0?n&Vy:pT:n}function Zb(t,n,e){let i=1<<t;return!!(e[n+(t>>By)]&i)}function Kb(t,n){return!(t&2)&&!(t&1&&n)}var Ri=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return zy(this._tNode,this._lView,n,gr(i),e)}};function pT(){return new Ri(it(),ae())}function bt(t){return ya(()=>{let n=t.prototype.constructor,e=n[$s]||cm(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[$s]||cm(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function cm(t){return lh(t)?()=>{let n=cm(Qe(t));return n&&n()}:vr(t)}function gT(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[$]&2048&&!So(s);){let a=$y(o,s,e,i|2,Bn);if(a!==Bn)return a;let l=o.parent;if(!l){let c=s[Sh];if(c){let u=c.get(e,Bn,i&-5);if(u!==Bn)return u}l=Wy(s),s=s[Ni]}o=l}return r}function Wy(t){let n=t[V],e=n.type;return e===2?n.declTNode:e===1?t[_t]:null}function zm(t){return fT(it(),t)}function w(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function vT(){return Ro(it(),ae())}function Ro(t,n){return new k(Zt(t,n))}var k=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=vT}return t})();function Gy(t){return t instanceof k?t.nativeElement:t}function _T(){return this._results[Symbol.iterator]()}var Or=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new S}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=ob(n);(this._changesDetected=!rb(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=_T};function qy(t){return(t.flags&128)===128}var $m=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})($m||{}),Yy=new Map,bT=0;function yT(){return bT++}function CT(t){Yy.set(t[Ln],t)}function dm(t){Yy.delete(t[Ln])}var Xb="__ngContext__";function Mo(t,n){ni(n)?(t[Xb]=n[Ln],CT(n)):t[Xb]=n}function Zy(t){return Xy(t[Co])}function Ky(t){return Xy(t[qt])}function Xy(t){for(;t!==null&&!Yt(t);)t=t[qt];return t}var um;function Wm(t){um=t}function Qy(){if(um!==void 0)return um;if(typeof document<"u")return document;throw new y(210,!1)}var Jy="r";var eC="di";var tC=!1,nC=new g("",{factory:()=>tC});var Qb=new WeakMap;function ST(t,n){if(t==null||typeof t!="object")return;let e=Qb.get(t);e||(e=new WeakSet,Qb.set(t,e)),e.add(n)}var DT=(t,n,e,i)=>{};function wT(t,n,e,i){DT(t,n,e,i)}function bd(t){return(t.flags&32)===32}var ET=()=>null;function iC(t,n,e=!1){return ET(t,n,e)}function rC(t,n){let e=t.contentQueries;if(e!==null){let i=F(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];ra(o),a.contentQueries(2,n[s],s)}}}finally{F(i)}}}function fm(t,n,e){ra(0);let i=F(null);try{n(t,e)}finally{F(i)}}function oC(t,n,e){if(wh(n)){let i=F(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let l=e[s];a.contentQueries(1,l,s)}}}finally{F(i)}}}var vn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(vn||{});var Kc;function xT(){if(Kc===void 0&&(Kc=null,vo.trustedTypes))try{Kc=vo.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Kc}function yd(t){return xT()?.createHTML(t)||t}var ai=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Ic})`}},hm=class extends ai{getTypeName(){return"HTML"}},mm=class extends ai{getTypeName(){return"Style"}},pm=class extends ai{getTypeName(){return"Script"}},gm=class extends ai{getTypeName(){return"URL"}},vm=class extends ai{getTypeName(){return"ResourceURL"}};function li(t){return t instanceof ai?t.changingThisBreaksApplicationSecurity:t}function Lr(t,n){let e=sC(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Ic})`)}return e===n}function sC(t){return t instanceof ai&&t.getTypeName()||null}function Gm(t){return new hm(t)}function qm(t){return new mm(t)}function Ym(t){return new pm(t)}function Zm(t){return new gm(t)}function Km(t){return new vm(t)}function IT(t){let n=new bm(t);return TT()?new _m(n):n}var _m=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(yd(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},bm=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=yd(n),e}};function TT(){try{return!!new window.DOMParser().parseFromString(yd(""),"text/html")}catch{return!1}}var MT=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Cd(t){return t=String(t),t.match(MT)?t:"unsafe:"+t}function ci(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Ca(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var aC=ci("area,br,col,hr,img,wbr"),lC=ci("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),cC=ci("rp,rt"),NT=Ca(cC,lC),kT=Ca(lC,ci("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),AT=Ca(cC,ci("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Jb=Ca(aC,kT,AT,NT),dC=ci("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),RT=ci("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),OT=ci("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),PT=Ca(dC,RT,OT),FT=ci("script,style,template"),ym=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=BT(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=VT(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=ey(n).toLowerCase();if(!Jb.hasOwnProperty(e))return this.sanitizedSomething=!0,!FT.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!PT.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let l=o.value;dC[a]&&(l=Cd(l)),this.buf.push(" ",s,'="',ty(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=ey(n).toLowerCase();Jb.hasOwnProperty(e)&&!aC.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(ty(n))}};function LT(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function VT(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw uC(n);return n}function BT(t){let n=t.firstChild;if(n&&LT(t,n))throw uC(n);return n}function ey(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function uC(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var jT=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,UT=/([^\#-~ |!])/g;function ty(t){return t.replace(/&/g,"&amp;").replace(jT,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(UT,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Xc;function Xm(t,n){let e=null;try{Xc=Xc||IT(t);let i=n?String(n):"";e=Xc.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Xc.getInertBodyElement(i)}while(i!==o);let a=new ym().sanitizeChildren(ny(e)||e);return yd(a)}finally{if(e){let i=ny(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function ny(t){return"content"in t&&HT(t)?t.content:null}function HT(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}function zT(t,n){return t.createText(n)}function $T(t,n,e){t.setValue(n,e)}function fC(t,n,e){return t.createElement(n,e)}function kr(t,n,e,i,r){t.insertBefore(n,e,i,r)}function hC(t,n,e){t.appendChild(n,e)}function iy(t,n,e,i,r){i!==null?kr(t,n,e,i,r):hC(t,n,e)}function mC(t,n,e,i){t.removeChild(null,n,e,i)}function WT(t,n,e){t.setAttribute(n,"style",e)}function GT(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function pC(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&sT(t,n,i),r!==null&&GT(t,n,r),o!==null&&WT(t,n,o)}function Qm(t){return t.ownerDocument.defaultView}function qT(t){return t instanceof Function?t():t}function YT(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var gC="ng-template";function ZT(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&YT(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(Jm(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Jm(t){return t.type===4&&t.value!==gC}function KT(t,n,e){let i=t.type===4&&!e?gC:t.value;return n===i}function XT(t,n,e){let i=4,r=t.attrs,o=r!==null?eM(r):0,s=!1;for(let a=0;a<n.length;a++){let l=n[a];if(typeof l=="number"){if(!s&&!pn(i)&&!pn(l))return!1;if(s&&pn(l))continue;s=!1,i=l|i&1;continue}if(!s)if(i&4){if(i=2|i&1,l!==""&&!KT(t,l,e)||l===""&&n.length===1){if(pn(i))return!1;s=!0}}else if(i&8){if(r===null||!ZT(t,r,l,e)){if(pn(i))return!1;s=!0}}else{let c=n[++a],u=QT(l,r,Jm(t),e);if(u===-1){if(pn(i))return!1;s=!0;continue}if(c!==""){let f;if(u>o?f="":f=r[u+1].toLowerCase(),i&2&&c!==f){if(pn(i))return!1;s=!0}}}}return pn(i)||s}function pn(t){return(t&1)===0}function QT(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return tM(n,t)}function vC(t,n,e=!1){for(let i=0;i<n.length;i++)if(XT(t,n[i],e))return!0;return!1}function JT(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function eM(t){for(let n=0;n<t.length;n++){let e=t[n];if(Fy(e))return n}return t.length}function tM(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function nM(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function ry(t,n){return t?":not("+n.trim()+")":n}function iM(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!pn(s)&&(n+=ry(o,r),r=""),i=s,o=o||!pn(i);e++}return r!==""&&(n+=ry(o,r)),n}function rM(t){return t.map(iM).join(",")}function oM(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!pn(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var Jt={},jn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(jn||{}),sM;function ep(t,n){return sM(t,n)}var I8=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Cm=new WeakMap;function _C(t){return t?t[Ni]??t:null}var da=new WeakSet;function aM(t,n,e){let i=Cm.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=_C(e);for(let a=i.length-1;a>=0;a--){let{el:l,declarationView:c}=i[a],u=l.parentNode;l===n?(i.splice(a,1),da.add(l),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&l===o?(i.splice(a,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l)):u&&r&&u!==r&&(s===null||c===null||s===c)&&(i.splice(a,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l))}}function lM(t,n,e){let i=_C(e),r=Cm.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):Cm.set(t,[{el:n,declarationView:i}])}var Oi=new Set,Sd=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Sd||{}),Hn=new g(""),oy=new Set;function di(t){oy.has(t)||(oy.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Dd=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=se({token:t,providedIn:"root",factory:()=>new t})}return t})(),tp=[0,1,2,3],np=(()=>{class t{ngZone=d(I);scheduler=d(kn);errorHandler=d(vt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(Hn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Ce(ge.AfterRenderHooksStart),this.executing=!0;for(let i of tp)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Ce(ge.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[wr]??=[]).push(e),Tr(i),i[$]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Sd.AFTER_NEXT_RENDER,e):e()}static \u0275prov=se({token:t,providedIn:"root",factory:()=>new t})}return t})(),ma=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[wr];n&&(this.view[wr]=n.filter(e=>e!==this))}};function rt(t,n){let e=n?.injector??d(P);return di("NgAfterNextRender"),dM(t,e,n,!0)}function cM(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function dM(t,n,e,i){let r=n.get(Dd);r.impl??=n.get(np);let o=n.get(Hn,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(Ue):null,a=n.get(Eo,null,{optional:!0}),l=new ma(r.impl,cM(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(l),l}var ip=new g("",{factory:()=>{let t=d(we),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function bC(t,n,e){let i=t.get(ip);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function uM(t,n){let e=t.get(ip);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function fM(t,n){let e=t.get(ip);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function hM(t,n){for(let[e,i]of n)bC(t,i.animateFns)}function sy(t,n,e,i){let r=t?.[Vn]?.enter;n!==null&&r&&r.has(e.index)&&hM(i,r)}function ay(t,n,e,i){try{e.get(Js)}catch{return i(!1)}let r=t?.[Vn];r?.enter?.has(n.index)&&uM(e,r.enter.get(n.index).animateFns);let o=mM(t,n,r);if(o.size===0){let s=!1;if(t){let a=[];wd(t,n,a),s=a.length>0}if(!s)return i(!1)}t&&Oi.add(t[Ln]),bC(e,()=>pM(t,n,r||void 0,o,i),r||void 0)}function mM(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,s]of r){if(i.has(o))continue;let l=t[V].data[o].parent;for(;l;){if(l===n){i.set(o,s);break}l=l.parent}}return i}function pM(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let l of a.animateFns){let{promise:c}=l();o.push(c)}e.detachedLeaveAnimationFns=void 0}if(t&&wd(t,n,o),o.length>0){let s=e||t?.[Vn];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),vM(t,s.running,r)}else Promise.allSettled(o).then(()=>{t&&Oi.delete(t[Ln]),r(!0)})}else t&&Oi.delete(t[Ln]),r(!1)}function wd(t,n,e){if(n.type&12){let r=t[n.index];if(Yt(r))for(let o=Ve;o<r.length;o++){let s=r[o];s[V].type===2&&gM(s,e)}}let i=n.child;for(;i;)wd(t,i,e),i=i.next}function gM(t,n){let e=t[Vn];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=t[V].firstChild;for(;i;)wd(t,i,n),i=i.next}function vM(t,n,e){n.then(()=>{t[Vn]?.running===n&&(t[Vn].running=void 0,Oi.delete(t[Ln])),e(!0)})}function xo(t,n,e,i,r,o,s,a){if(r!=null){let l,c=!1;Yt(r)?l=r:ni(r)&&(c=!0,r=r[fn]);let u=dt(r);t===0&&i!==null?(sy(a,i,o,e),s==null?hC(n,i,u):kr(n,i,u,s||null,!0)):t===1&&i!==null?(sy(a,i,o,e),kr(n,i,u,s||null,!0),aM(o,u,a)):t===2?(a?.[Vn]?.leave?.has(o.index)&&lM(o,u,a),da.delete(u),ay(a,o,e,f=>{if(da.has(u)){da.delete(u);return}mC(n,u,c,f)})):t===3&&(da.delete(u),ay(a,o,e,()=>{n.destroyNode(u)})),l!=null&&TM(n,t,e,l,o,i,s)}}function _M(t,n){yC(t,n),n[fn]=null,n[_t]=null}function bM(t,n,e,i,r,o){i[fn]=r,i[_t]=n,xd(t,i,e,1,r,o)}function yC(t,n){n[Pn].changeDetectionScheduler?.notify(9),xd(t,n,n[Me],2,null,null)}function yM(t){let n=t[Co];if(!n)return Qh(t[V],t);for(;n;){let e=null;if(ni(n))e=n[Co];else{let i=n[Ve];i&&(e=i)}if(!e){for(;n&&!n[qt]&&n!==t;)ni(n)&&Qh(n[V],n),n=n[Je];n===null&&(n=t),ni(n)&&Qh(n[V],n),e=n&&n[qt]}n=e}}function rp(t,n){let e=t[xr],i=e.indexOf(n);e.splice(i,1)}function Ed(t,n){if(Ir(n))return;let e=n[Me];e.destroyNode&&xd(t,n,e,3,null,null),yM(n)}function Qh(t,n){if(Ir(n))return;let e=F(null);try{n[$]&=-129,n[$]|=256,n[Vt]&&Kn(n[Vt]),SM(t,n),CM(t,n),n[V].type===1&&n[Me].destroy();let i=n[ki];if(i!==null&&Yt(n[Je])){i!==n[Je]&&rp(i,n);let r=n[Fn];r!==null&&r.detachView(t)}dm(n)}finally{F(e)}}function CM(t,n){let e=t.cleanup,i=n[yo];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[yo]=null);let r=n[ei];if(r!==null){n[ei]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Ii];if(o!==null){n[Ii]=null;for(let s of o)s.destroy()}}function SM(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Rr)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],l=o[s+1];Ce(ge.LifecycleHookStart,a,l);try{l.call(a)}finally{Ce(ge.LifecycleHookEnd,a,l)}}else{Ce(ge.LifecycleHookStart,r,o);try{o.call(r)}finally{Ce(ge.LifecycleHookEnd,r,o)}}}}}function CC(t,n,e){return DM(t,n.parent,e)}function DM(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[fn];if(ii(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===vn.None||r===vn.Emulated)return null}return Zt(i,e)}function SC(t,n,e){return EM(t,n,e)}function wM(t,n,e){return t.type&40?Zt(t,e):null}var EM=wM,ly;function op(t,n,e,i){let r=CC(t,i,n),o=n[Me],s=i.parent||n[_t],a=SC(s,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)iy(o,r,e[l],a,!1);else iy(o,r,e,a,!1);ly!==void 0&&ly(o,i,n,e,r)}function ua(t,n){if(n!==null){let e=n.type;if(e&3)return Zt(n,t);if(e&4)return Sm(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return ua(t,i);{let r=t[n.index];return Yt(r)?Sm(-1,r):dt(r)}}else{if(e&128)return ua(t,n.next);if(e&32)return ep(n,t)()||dt(t[n.index]);{let i=DC(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=ti(t[Tt]);return ua(r,i)}else return ua(t,n.next)}}}return null}function DC(t,n){if(n!==null){let i=t[Tt][_t],r=n.projection;return i.projection[r]}return null}function Sm(t,n){let e=Ve+t+1;if(e<n.length){let i=n[e],r=i[V].firstChild;if(r!==null)return ua(i,r)}return n[Er]}function sp(t,n,e,i,r,o,s){for(;e!=null;){let a=i[On];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(s&&n===0&&(l&&Mo(dt(l),i),e.flags|=2),!bd(e))if(c&8)sp(t,n,e.child,i,r,o,!1),xo(n,t,a,r,l,e,o,i);else if(c&32){let u=ep(e,i),f;for(;f=u();)xo(n,t,a,r,f,e,o,i);xo(n,t,a,r,l,e,o,i)}else c&16?wC(t,n,i,e,r,o):xo(n,t,a,r,l,e,o,i);e=s?e.projectionNext:e.next}}function xd(t,n,e,i,r,o){t.type===3?xM(e,i,n,r,o):sp(e,i,t.firstChild,n,r,o,!1)}function xM(t,n,e,i,r){let s=e[V].firstChild,a=s.next,l=dt(e[s.index]),c=dt(e[a.index]),u=a.index+1,f=e[u];if(n===1||n===0)i!==null&&(f&&f.hasChildNodes()?kr(t,i,f,r,!0):(kr(t,i,l,r,!0),kr(t,i,c,r,!0)));else if(n===2){if(f||(f=document.createDocumentFragment(),e[u]=f),l&&l.parentNode===f)return;let h=l;for(;h!==null;){let m=h.nextSibling;if(f.appendChild(h),h===c)break;h=m}}}function IM(t,n,e){let i=n[Me],r=CC(t,e,n),o=e.parent||n[_t],s=SC(o,e,n);wC(i,0,n,e,r,s)}function wC(t,n,e,i,r,o){let s=e[Tt],l=s[_t].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];xo(n,t,e[On],r,u,i,o,e)}else{let c=l,u=s[Je];qy(i)&&(c.flags|=128),sp(t,n,c,u,r,o,!0)}}function TM(t,n,e,i,r,o,s){let a=i[Er],l=dt(i);if(a!==l&&xo(n,t,e,o,a,r,s),(i[$]&4)===0)for(let c=Ve;c<i.length;c++){let u=i[c];xd(u[V],u,t,n,o,a)}}function MM(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:jn.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=jn.Important),t.setStyle(e,i,r,o))}}function ap(t,n,e,i,r,o,s,a,l,c,u){let f=We+i,h=f+r,m=NM(f,h),v=typeof c=="function"?c():c;return m[V]={type:t,blueprint:m,template:e,queries:null,viewQuery:a,declTNode:n,data:m.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:l,consts:v,incompleteFirstPass:!1,ssrId:u}}function NM(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Jt);return e}function kM(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=ap(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function lp(t,n,e,i,r,o,s,a,l,c,u){let f=n.blueprint.slice();return f[fn]=r,f[$]=i|4|128|8|64|1024,(c!==null||t&&t[$]&2048)&&(f[$]|=2048),Ih(f),f[Je]=f[Ni]=t,f[$e]=e,f[Pn]=s||t&&t[Pn],f[Me]=a||t&&t[Me],f[On]=l||t&&t[On]||null,f[_t]=o,f[Ln]=yT(),f[Sr]=u,f[Sh]=c,f[Tt]=n.type==2?t[Tt]:f,f}function AM(t,n,e){let i=Zt(n,t),r=kM(e),o=t[Pn].rendererFactory,s=cp(t,lp(t,r,null,EC(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function EC(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function xC(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function cp(t,n){return t[Co]?t[Ch][qt]=n:t[Co]=n,t[Ch]=n,n}function C(t=1){IC(Be(),ae(),ri()+t,!1)}function IC(t,n,e,i){if(!i)if((n[$]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Jc(n,o,e)}else{let o=t.preOrderHooks;o!==null&&ed(n,o,0,e)}Ai(e)}var Id=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Id||{});function Pr(t,n,e,i){let r=F(null);try{let[o,s,a]=t.inputs[e],l=null;(s&Id.SignalBased)!==0&&(l=n[o][Te]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):ky(n,l,o,i)}finally{F(r)}}function TC(t,n,e,i,r){let o=ri(),s=i&2;try{Ai(-1),s&&n.length>We&&IC(t,n,We,!1);let a=s?ge.TemplateUpdateStart:ge.TemplateCreateStart;Ce(a,r,e),e(i,r)}finally{Ai(o);let a=s?ge.TemplateUpdateEnd:ge.TemplateCreateEnd;Ce(a,r,e)}}function dp(t,n,e){BM(t,n,e),(e.flags&64)===64&&jM(t,n,e)}function Td(t,n,e=Zt){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function RM(t,n,e,i){let o=i.get(nC,tC)||e===vn.ShadowDom||e===vn.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return OM(s),s}function OM(t){PM(t)}var PM=()=>null;function FM(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function LM(t,n,e,i,r,o){let s=n[V];if(up(t,s,n,e,i)){ii(t)&&VM(n,t.index);return}t.type&3&&(e=FM(e)),MC(t,n,e,i,r,o)}function MC(t,n,e,i,r,o){if(t.type&3){let s=Zt(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function VM(t,n){let e=Kt(n,t);e[$]&16||(e[$]|=64)}function BM(t,n,e){let i=e.directiveStart,r=e.directiveEnd;ii(e)&&AM(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||sd(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],l=ha(n,t,s,e);if(Mo(l,n),o!==null&&$M(n,s-i,l,a,e,o),hn(a)){let c=Kt(e.index,n);c[$e]=ha(n,t,s,e)}}}function jM(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=Nb();try{Ai(o);for(let a=i;a<r;a++){let l=t.data[a],c=n[a];Uc(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&UM(l,c)}}finally{Ai(-1),Uc(s)}}function UM(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function NC(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];vC(n,o.selectors,!1)&&(i??=[],hn(o)?i.unshift(o):i.push(o))}return i}function HM(t,n,e,i,r,o){let s=Zt(t,n);zM(n[Me],s,o,t.value,e,i,r)}function zM(t,n,e,i,r,o,s){if(o==null)s?.(o,i||"",r),t.removeAttribute(n,r,e);else{let a=s==null?hh(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function $M(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let l=s[a],c=s[a+1];Pr(i,e,l,c)}}function kC(t,n,e,i,r){let o=We+e,s=n[V],a=r(s,n,t,i,e);n[o]=a,Do(t,!0);let l=t.type===2;return l?(pC(n[Me],a,t),(Sb()===0||na(t))&&Mo(a,n),Db()):Mo(a,n),Gc()&&(!l||!bd(t))&&op(s,n,a,t),t}function AC(t){let n=t;return Lh()?Vh():(n=n.parent,Do(n,!1)),n}function WM(t,n){let e=t[On];if(!e)return;let i;try{i=e.get(Qt,null)}catch{i=null}i?.(n)}function up(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let l=0;l<s.length;l+=2){let c=s[l],u=s[l+1],f=n.data[c];Pr(f,e[c],u,r),a=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];Pr(u,c,i,r),a=!0}return a}function GM(t,n,e,i,r,o){let s=null,a=null,l=null,c=!1,u=t.directiveToIndex.get(i.type);if(typeof u=="number"?s=u:[s,a,l]=u,a!==null&&l!==null&&t.hostDirectiveInputs?.hasOwnProperty(r)){let f=t.hostDirectiveInputs[r];for(let h=0;h<f.length;h+=2){let m=f[h];if(m>=a&&m<=l){let v=n.data[m],D=f[h+1];Pr(v,e[m],D,o),c=!0}else if(m>l)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(Pr(i,e[s],r,o),c=!0),c}function qM(t,n){let e=Kt(n,t),i=e[V];YM(i,e);let r=e[fn];r!==null&&e[Sr]===null&&(e[Sr]=iC(r,e[On])),Ce(ge.ComponentStart);try{fp(i,e,e[$e])}finally{Ce(ge.ComponentEnd,e[$e])}}function YM(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function fp(t,n,e){zc(n);try{let i=t.viewQuery;i!==null&&fm(1,i,e);let r=t.template;r!==null&&TC(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Fn]?.finishViewCreation(t),t.staticContentQueries&&rC(t,n),t.staticViewQueries&&fm(2,t.viewQuery,e);let o=t.components;o!==null&&ZM(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[$]&=-5,$c()}}function ZM(t,n){for(let e=0;e<n.length;e++)qM(t,n[e])}function Sa(t,n,e,i){let r=F(null);try{let o=n.tView,a=t[$]&4096?4096:16,l=lp(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[ki]=c;let u=t[Fn];return u!==null&&(l[Fn]=u.createEmbeddedView(o)),fp(o,l,e),l}finally{F(r)}}function No(t,n){return!n||n.firstChild===null||qy(t)}function pa(t,n,e,i,r=!1){if(t.type===3){let o=t.firstChild,s=o.next,a=dt(n[o.index]),l=dt(n[s.index]),c=a;for(;c!==null&&(i.push(c),c!==l);)c=c.nextSibling;return i}for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];if(o!==null)if(Yt(o)){let a=o[Er];a!==o[fn]&&i.push(dt(o)),o[$]&4||RC(o,i),i.push(a)}else i.push(dt(o));let s=e.type;if(s&8)pa(t,n,e.child,i);else if(s&32){let a=ep(e,n),l;for(;l=a();)i.push(l)}else if(s&16){let a=DC(n,e);if(Array.isArray(a))i.push(...a);else{let l=ti(n[Tt]);pa(l[V],l,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function RC(t,n){for(let e=Ve;e<t.length;e++){let i=t[e],r=i[V].firstChild;r!==null&&pa(i[V],i,r,n)}}function OC(t){if(t[wr]!==null){for(let n of t[wr])n.impl.addSequence(n);t[wr].length=0}}var PC=[];function KM(t){return t[Vt]??XM(t)}function XM(t){let n=PC.pop()??Object.create(JM);return n.lView=t,n}function QM(t){t.lView[Vt]!==t&&(t.lView=null,PC.push(t))}var JM=N(p({},qn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Tr(t.lView)},consumerOnSignalRead(){this.lView[Vt]=this}});function eN(t){let n=t[Vt]??Object.create(tN);return n.lView=t,n}var tN=N(p({},qn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=ti(t.lView);for(;n&&!FC(n[V]);)n=ti(n);n&&Th(n)},consumerOnSignalRead(){this.lView[Vt]=this}});function FC(t){return t.type!==2}function LC(t){if(t[Ii]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Ii])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[$]&8192)}}var nN=100;function VC(t,n=0){let i=t[Pn].rendererFactory,r=!1;r||i.begin?.();try{iN(t,n)}finally{r||i.end?.()}}function iN(t,n){let e=Bh();try{Ws(!0),Dm(t,n);let i=0;for(;ia(t);){if(i===nN)throw new y(103,!1);i++,Dm(t,1)}}finally{Ws(e)}}function rN(t,n,e,i){if(Ir(n))return;let r=n[$],o=!1,s=!1;zc(n);let a=!0,l=null,c=null;o||(FC(t)?(c=KM(n),l=Tn(c)):Ul()===null?(a=!1,c=eN(n),l=Tn(c)):n[Vt]&&(Kn(n[Vt]),n[Vt]=null));try{Ih(n),Ib(t.bindingStartIndex),e!==null&&TC(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let m=t.preOrderCheckHooks;m!==null&&Jc(n,m,null)}else{let m=t.preOrderHooks;m!==null&&ed(n,m,0,null),Kh(n,0)}if(s||oN(n),LC(n),BC(n,0),t.contentQueries!==null&&rC(t,n),!o)if(u){let m=t.contentCheckHooks;m!==null&&Jc(n,m)}else{let m=t.contentHooks;m!==null&&ed(n,m,1),Kh(n,1)}aN(t,n);let f=t.components;f!==null&&UC(n,f,0);let h=t.viewQuery;if(h!==null&&fm(2,h,i),!o)if(u){let m=t.viewCheckHooks;m!==null&&Jc(n,m)}else{let m=t.viewHooks;m!==null&&ed(n,m,2),Kh(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Fc]){for(let m of n[Fc])m();n[Fc]=null}o||(OC(n),n[$]&=-73)}catch(u){throw o||Tr(n),u}finally{c!==null&&(Zn(c,l),a&&QM(c)),$c()}}function BC(t,n){for(let e=Zy(t);e!==null;e=Ky(e))for(let i=Ve;i<e.length;i++){let r=e[i];jC(r,n)}}function oN(t){for(let n=Zy(t);n!==null;n=Ky(n)){if(!(n[$]&2))continue;let e=n[xr];for(let i=0;i<e.length;i++){let r=e[i];Th(r)}}}function sN(t,n,e){Ce(ge.ComponentStart);let i=Kt(n,t);try{jC(i,e)}finally{Ce(ge.ComponentEnd,i[$e])}}function jC(t,n){Vc(t)&&Dm(t,n)}function Dm(t,n){let i=t[V],r=t[$],o=t[Vt],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&ar(o)),s||=!1,o&&(o.dirty=!1),t[$]&=-9217,s)rN(i,t,i.template,t[$e]);else if(r&8192){let a=F(null);try{LC(t),BC(t,1);let l=i.components;l!==null&&UC(t,l,1),OC(t)}finally{F(a)}}}function UC(t,n,e){for(let i=0;i<n.length;i++)sN(t,n[i],e)}function aN(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Ai(~r);else{let o=r,s=e[++i],a=e[++i];Mb(s,o);let l=n[o];Ce(ge.HostBindingsUpdateStart,l);try{a(2,l)}finally{Ce(ge.HostBindingsUpdateEnd,l)}}}}finally{Ai(-1)}}function hp(t,n){let e=Bh()?64:1088;for(t[Pn].changeDetectionScheduler?.notify(n);t;){t[$]|=e;let i=ti(t);if(So(t)&&!i)return t;t=i}return null}function HC(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function zC(t,n){let e=Ve+n;if(e<t.length)return t[e]}function Da(t,n,e,i=!0){let r=n[V];if(lN(r,n,t,e),i){let s=Sm(e,t),a=n[Me],l=a.parentNode(t[Er]);l!==null&&bM(r,t[_t],a,n,l,s)}let o=n[Sr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function $C(t,n){let e=ga(t,n);return e!==void 0&&Ed(e[V],e),e}function ga(t,n){if(t.length<=Ve)return;let e=Ve+n,i=t[e];if(i){let r=i[ki];r!==null&&r!==t&&rp(r,i),n>0&&(t[e-1][qt]=i[qt]);let o=Qs(t,Ve+n);_M(i[V],i);let s=o[Fn];s!==null&&s.detachView(o[V]),i[Je]=null,i[qt]=null,i[$]&=-129}return i}function lN(t,n,e,i){let r=Ve+i,o=e.length;i>0&&(e[r-1][qt]=n),i<o-Ve?(n[qt]=e[r],gh(e,Ve+i,n)):(e.push(n),n[qt]=null),n[Je]=e;let s=n[ki];s!==null&&e!==s&&WC(s,n);let a=n[Fn];a!==null&&a.insertView(t),Bc(n),n[$]|=128}function WC(t,n){let e=t[xr],i=n[Je];if(ni(i))t[$]|=2;else{let r=i[Je][Tt];n[Tt]!==r&&(t[$]|=2)}e===null?t[xr]=[n]:e.push(n)}var Pi=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[V];return pa(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[$e]}set context(n){this._lView[$e]=n}get destroyed(){return Ir(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Je];if(Yt(n)){let e=n[ta],i=e?e.indexOf(this):-1;i>-1&&(ga(n,i),Qs(e,i))}this._attachedToViewContainer=!1}Ed(this._lView[V],this._lView)}onDestroy(n){jc(this._lView,n)}markForCheck(){hp(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[$]&=-129}reattach(){Bc(this._lView),this._lView[$]|=128}detectChanges(){this._lView[$]|=1024,VC(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new y(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=So(this._lView),e=this._lView[ki];e!==null&&!n&&rp(e,this._lView),yC(this._lView[V],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new y(902,!1);this._appRef=n;let e=So(this._lView),i=this._lView[ki];i!==null&&!e&&WC(i,this._lView),Bc(this._lView)}};var Pt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=cN;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Sa(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Pi(o)}}return t})();function cN(){return Md(it(),ae())}function Md(t,n){return t.type&4?new Pt(n,t,Ro(t,n)):null}function Oo(t,n,e,i,r){let o=t.data[n];if(o===null)o=dN(t,n,e,i,r),Tb()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=wb();o.injectorIndex=s===null?-1:s.injectorIndex}return Do(o,!0),o}function dN(t,n,e,i,r){let o=Fh(),s=Lh(),a=s?o:o&&o.parent,l=t.data[n]=fN(t,a,e,n,i,r);return uN(t,l,o,s),l}function uN(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function fN(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return Rh()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:zh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function hN(t){let n=t[Dh]??[],i=t[Je][Me],r=[];for(let o of n)o.data[eC]!==void 0?r.push(o):mN(o,i);t[Dh]=r}function mN(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[Jy];for(;e<r;){let o=i.nextSibling;mC(n,i,!1),i=o,e++}}}var pN=()=>null,gN=()=>null;function ad(t,n){return pN(t,n)}function GC(t,n,e){return gN(t,n,e)}var qC=class{},Ze=class{},De=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>vN()};function vN(){let t=ae(),n=it(),e=Kt(n.index,t);return(ni(e)?e:t)[Me]}var YC=(()=>{class t{static \u0275prov=se({token:t,providedIn:"root",factory:()=>null})}return t})();function ZC(t){return t.debugInfo?.className||t.type.name||null}var nd={},ld=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,nd,i);return r!==nd||e===nd?r:this.parentInjector.get(n,e,i)}};function _N(t,n,e){return t[n]=e}function bN(t,n){return t[n]}function Fi(t,n,e){if(e===Jt)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Ar(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&ST(r,o);let s=ii(t)?Kt(t.index,n):n;hp(s,5);let a=n[$e],l=cy(n,a,e,r),c=i.__ngNextListenerFn__;for(;c;)l=cy(n,a,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function cy(t,n,e,i){let r=F(null);try{return Ce(ge.OutputStart,n,e),e(i)!==!1}catch(o){return WM(t,o),!1}finally{Ce(ge.OutputEnd,n,e),F(r)}}function mp(t,n,e,i,r,o,s,a){let l=na(t),c=!1,u=null;if(!i&&l&&(u=CN(n,e,o,t.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,c=!0}else{let f=Zt(t,e),h=i?i(f):f;wT(e,h,o,a),i||(a.__ngNativeEl__=f);let m=r.listen(h,o,a);if(!yN(o)){let v=i?D=>i(dt(D[t.index])):t.index;KC(v,n,e,o,a,m,!1)}}return c}function yN(t){return t.startsWith("animation")||t.startsWith("transition")}function CN(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[yo],l=r[o+2];return a&&a.length>l?a[l]:null}typeof s=="string"&&(o+=2)}return null}function KC(t,n,e,i,r,o,s){let a=n.firstCreatePass?Nh(n):null,l=Mh(e),c=l.length;l.push(r,o),a&&a.push(i,t,c,(c+1)*(s?-1:1))}function dy(t,n,e,i,r){let o=null,s=null,a=null,l=!1,c=t.directiveToIndex.get(e.type);if(typeof c=="number"?o=c:[o,s,a]=c,s!==null&&a!==null&&t.hostDirectiveOutputs?.hasOwnProperty(i)){let u=t.hostDirectiveOutputs[i];for(let f=0;f<u.length;f+=2){let h=u[f];if(h>=s&&h<=a)l=!0,cd(t,n,h,u[f+1],i,r);else if(h>a)break}}return e.outputs.hasOwnProperty(i)&&(l=!0,cd(t,n,o,i,i,r)),l}function cd(t,n,e,i,r,o){let s=n[e],a=n[V],c=a.data[e].outputs[i],f=s[c].subscribe(o);KC(t.index,a,n,r,o,f,!0)}function en(){SN()}function SN(){let t=ae(),n=Be(),e=it();if(n.firstCreatePass&&wN(n,e),e.controlDirectiveIndex===-1)return;di("NgSignalForms");let i=t[e.controlDirectiveIndex];n.data[e.controlDirectiveIndex].controlDef.create(i,new dd(t,n,e))}function tn(){DN()}function DN(){let t=ae(),n=Be(),e=oa();if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef,r=t[e.controlDirectiveIndex];i.update(r,new dd(t,n,e))}var dd=class{lView;tView;tNode;hasPassThrough;constructor(n,e,i){this.lView=n,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return Zt(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,e){let i=this.tView.data[this.tNode.customControlIndex];dy(this.tNode,this.lView,i,n,Ar(this.tNode,this.lView,e))}listenToCustomControlModel(n){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];dy(this.tNode,this.lView,i,e,Ar(this.tNode,this.lView,n))}listenToDom(n,e){mp(this.tNode,this.tView,this.lView,void 0,this.lView[Me],n,e,Ar(this.tNode,this.lView,e))}setInputOnDirectives(n,e){let i=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],l=this.lView[s];Pr(a,l,n,e),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let l=r[s+1],c=this.tView.data[a],u=this.lView[a];Pr(c,u,l,e),o=!0}return o}setCustomControlModelInput(n){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";GM(this.tNode,this.tView,this.lView,e,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[n]===!0}_buildCustomControlInputCache(n){let e={};for(let i in n.inputs)e[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)e[r.inputs[s]]=!0;let o=uy(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let l=o.inputs[a+1]||o.inputs[a];e[l]=!0}let s=uy(o.directive);s!==null&&i.push(...s)}}}return e}};function uy(t){return typeof t=="function"&&"\u0275dir"in t?t.\u0275dir.hostDirectives??null:null}function wN(t,n,e){for(let r=n.directiveStart;r<n.directiveEnd;r++)if(t.data[r].controlDef){n.controlDirectiveIndex=r;break}if(n.controlDirectiveIndex===-1)return;let i=t.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}EN(t,n)}function EN(t,n){for(let e=n.directiveStart;e<n.directiveEnd;e++){let i=t.data[e];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(fy(i,"value")){n.flags|=1024,n.customControlIndex=e;return}if(fy(i,"checked")){n.flags|=2048,n.customControlIndex=e;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let e=(i,r)=>{let o=n.hostDirectiveInputs[i],s=n.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let l=o[a];for(let c=0;c<s.length;c+=2){let u=s[c];if(l===u)for(let f of n.directiveToIndex.values()){if(!Array.isArray(f))continue;let[h,m,v]=f;if(l>=m&&l<=v)return n.flags|=r,n.customControlIndex=h,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function fy(t,n){return xN(t,n)&&IN(t,n+"Change")}function xN(t,n){return n in t.inputs}function IN(t,n){return n in t.outputs}var wm=Symbol("BINDING");var Vr=new g("");function ud(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=Tc(r,a);else if(o==2){let l=a,c=n[++s];i=Tc(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ot(t,n=0){let e=ae();if(e===null)return L(t,n);let i=it();return zy(i,e,Qe(t),n)}function XC(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,l=null,c=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,l,c]=u.resolveHostDirectives(s);break}NN(t,n,e,a,o,l,c)}o!==null&&i!==null&&TN(e,i,o)}function TN(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new y(-301,!1);i.push(n[r],o)}}function MN(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function NN(t,n,e,i,r,o,s){let a=i.length,l=null;for(let h=0;h<a;h++){let m=i[h];l===null&&hn(m)&&(l=m,MN(t,e,h)),lm(sd(e,n),t,m.type)}FN(e,t.data.length,a),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let h=0;h<a;h++){let m=i[h];m.providersResolver&&m.providersResolver(m)}let c=!1,u=!1,f=xC(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let h=0;h<a;h++){let m=i[h];if(e.mergedAttrs=To(e.mergedAttrs,m.hostAttrs),AN(t,e,n,f,m),PN(f,m,r),s!==null&&s.has(m)){let[D,A]=s.get(m);e.directiveToIndex.set(m.type,[f,D+e.directiveStart,A+e.directiveStart])}else(o===null||!o.has(m))&&e.directiveToIndex.set(m.type,f);m.contentQueries!==null&&(e.flags|=4),(m.hostBindings!==null||m.hostAttrs!==null||m.hostVars!==0)&&(e.flags|=64);let v=m.type.prototype;!c&&(v.ngOnChanges||v.ngOnInit||v.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(v.ngOnChanges||v.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),f++}kN(t,e,o)}function kN(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))hy(0,n,r,i),hy(1,n,r,i),py(n,i,!1);else{let o=e.get(r);my(0,n,o,i),my(1,n,o,i),py(n,i,!0)}}}function hy(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),QC(n,o)}}function my(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),QC(n,s)}}function QC(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function py(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||Jm(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let l=i[a];if(l===0){a+=4;continue}else if(l===5){a+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===n){s??=[],s.push(l,i[a+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){s??=[],s.push(c[u+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function AN(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=vr(r.type,!0)),s=new Rr(o,hn(r),ot,null);t.blueprint[i]=s,e[i]=s,RN(t,n,i,xC(t,e,r.hostVars,Jt),r)}function RN(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;ON(s)!=a&&s.push(a),s.push(e,i,o)}}function ON(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function PN(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;hn(n)&&(e[""]=t)}}function FN(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function JC(t,n,e,i,r,o,s,a){let l=n[V],c=l.consts,u=Xt(c,s),f=Oo(l,t,e,i,u);return o&&XC(l,n,f,Xt(c,a),r),f.mergedAttrs=To(f.mergedAttrs,f.attrs),f.attrs!==null&&ud(f,f.attrs,!1),f.mergedAttrs!==null&&ud(f,f.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,f),f}function eS(t,n){Oy(t,n),wh(n)&&t.queries.elementEnd(n)}function LN(t,n,e,i,r,o){let s=n.consts,a=Xt(s,r),l=Oo(n,t,e,i,a);if(l.mergedAttrs=To(l.mergedAttrs,l.attrs),o!=null){let c=Xt(s,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&ud(l,l.attrs,!1),l.mergedAttrs!==null&&ud(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}var tS=typeof ShadowRoot<"u",VN=typeof Document<"u";function BN(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Id.SignalBased)!==0};return r&&(o.transform=r),o})}function jN(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function UN(t,n,e){let i=n instanceof we?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new ld(e,i):e}function HN(t){let n=t.get(Ze,null);if(n===null)throw new y(407,!1);let e=t.get(YC,null),i=t.get(kn,null),r=t.get(Hn,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function zN(t,n){let e=nS(t);return fC(n,e,e==="svg"?Eh:e==="math"?pb:null)}function $N(t){if(t?.toLowerCase()==="script")throw new y(905,!1)}function nS(t){return(t.selectors[0][0]||"div").toLowerCase()}var ko=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=BN(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=jN(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=rM(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){Ce(ge.DynamicComponentStart);let a=F(null);try{let l=this.componentDef,c=UN(l,r||this.ngModule,n),u=HN(c),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(ZC(l),()=>this.createComponentRef(u,c,e,i,o,s)):this.createComponentRef(u,c,e,i,o,s)}finally{F(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,l=WN(r,a,s,o),c=n.rendererFactory.createRenderer(null,a),u=r?RM(c,r,a.encapsulation,e):zN(a,c);$N(u?.tagName);let f=e.get(Vr,null),h=GN(u,()=>e.get(R,null)??Qy());f&&f.addHost(h);let m=s?.some(gy)||o?.some(A=>typeof A!="function"&&A.bindings.some(gy)),v=lp(null,l,null,512|EC(a),null,null,n,c,e,null,iC(u,e,!0));f&&tS&&h instanceof ShadowRoot&&jc(v,()=>{f.removeHost(h)}),v[We]=u,zc(v);let D=null;try{let A=JC(We,v,2,"#host",()=>l.directiveRegistry,!0,0);pC(c,u,A),Mo(u,v),dp(l,v,A),oC(l,A,v),eS(l,A),i!==void 0&&YN(A,this.ngContentSelectors,i),D=Kt(A.index,v),v[$e]=D[$e],fp(l,v,null)}catch(A){throw D!==null&&dm(D),dm(v),A}finally{Ce(ge.DynamicComponentEnd),$c()}return new fd(this.componentType,v,!!m)}};function WN(t,n,e,i){let r=t?["ng-version","22.1.0"]:oM(n.selectors[0]),o=null,s=null,a=0;if(e)for(let u of e)a+=u[wm].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let h of f.bindings){a+=h[wm].requiredVars;let m=u+1;h.create&&(h.targetIdx=m,(o??=[]).push(h)),h.update&&(h.targetIdx=m,(s??=[]).push(h))}}let l=[n];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,h=Nc(f);l.push(h)}return ap(0,null,qN(o,s),1,a,l,null,null,null,[r],null)}function GN(t,n){let e=t.getRootNode?.();return VN&&e instanceof Document?e.head:e&&tS&&e instanceof ShadowRoot?e:n().head}function qN(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function gy(t){let n=t[wm].kind;return n==="input"||n==="twoWay"}var fd=class extends qC{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Lc(e[V],We),this.location=Ro(this._tNode,e),this.instance=Kt(this._tNode.index,e)[$e],this.hostView=this.changeDetectorRef=new Pi(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=up(i,r[V],r,n,e);this.previousInputValues.set(n,e);let s=Kt(i.index,r);hp(s,1)}get injector(){return new Ri(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function YN(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Mt=(()=>{class t{static __NG_ELEMENT_ID__=ZN}return t})();function ZN(){let t=it();return iS(t,ae())}var Em=class t extends Mt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Ro(this._hostTNode,this._hostLView)}get injector(){return new Ri(this._hostTNode,this._hostLView)}get parentInjector(){let n=Hm(this._hostTNode,this._hostLView);if(Ly(n)){let e=od(n,this._hostLView),i=rd(n),r=e[V].data[i+8];return new Ri(r,e)}else return new Ri(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=vy(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-Ve}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=ad(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,No(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let l,c=e||{};l=c.index,i=c.injector,r=c.projectableNodes,o=c.environmentInjector||c.ngModuleRef,s=c.directives,a=c.bindings;let u=new ko(Ti(n)),f=i||this.parentInjector;if(!o&&u.ngModule==null){let ne=this.parentInjector.get(we,null);ne&&(o=ne)}let h=Ti(u.componentType??{}),m=ad(this._lContainer,h?.id??null),v=m?.firstChild??null,D=u.create(f,r,v,o,s,a);return this.insertImpl(D.hostView,l,No(this._hostTNode,m)),D}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(_b(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let l=r[Je],c=new t(l,l[_t],l[Je]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Da(s,r,o,i),n.attachToViewContainerRef(),gh(Jh(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=vy(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=ga(this._lContainer,e);i&&(Qs(Jh(this._lContainer),e),Ed(i[V],i))}detach(n){let e=this._adjustIndex(n,-1),i=ga(this._lContainer,e);return i&&Qs(Jh(this._lContainer),e)!=null?new Pi(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function vy(t){return t[ta]}function Jh(t){return t[ta]||(t[ta]=[])}function iS(t,n){let e,i=n[t.index];return Yt(i)?e=i:(e=HC(i,n,null,t),n[t.index]=e,cp(n,e)),XN(e,n,t,i),new Em(e,t,n)}function KN(t,n){let e=t[Me],i=e.createComment(""),r=Zt(n,t),o=e.parentNode(r);return kr(e,o,i,e.nextSibling(r),!1),i}var XN=ek,QN=()=>!1;function JN(t,n,e){return QN(t,n,e)}function ek(t,n,e,i){if(t[Er])return;let r;e.type&8?r=dt(i):r=KN(n,e),t[Er]=r}var xm=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Im=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)gp(n,e).matches!==null&&this.queries[e].setDirty()}},hd=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=ok(n):this.predicate=n}},Tm=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Mm=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,tk(e,o)),this.matchTNodeWithReadOption(n,e,td(e,n,o,!1,!1))}else i===Pt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,td(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===k||r===Mt||r===Pt&&e.type&4)this.addMatch(e.index,-2);else{let o=td(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function tk(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function nk(t,n){return t.type&11?Ro(t,n):t.type&4?Md(t,n):null}function ik(t,n,e,i){return e===-1?nk(n,t):e===-2?rk(t,n,i):ha(t,t[V],e,n)}function rk(t,n,e){if(e===k)return Ro(n,t);if(e===Pt)return Md(n,t);if(e===Mt)return iS(n,t)}function rS(t,n,e,i){let r=n[Fn].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let l=0;s!==null&&l<s.length;l+=2){let c=s[l];if(c<0)a.push(null);else{let u=o[c];a.push(ik(n,u,s[l+1],e.metadata.read))}}r.matches=a}return r.matches}function Nm(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=rS(t,n,r,e);for(let a=0;a<o.length;a+=2){let l=o[a];if(l>0)i.push(s[a/2]);else{let c=o[a+1],u=n[-l];for(let f=Ve;f<u.length;f++){let h=u[f];h[ki]===h[Je]&&Nm(h[V],h,c,i)}if(u[xr]!==null){let f=u[xr];for(let h=0;h<f.length;h++){let m=f[h];Nm(m[V],m,c,i)}}}}}return i}function pp(t,n){return t[Fn].queries[n].queryList}function oS(t,n,e){let i=new Or((e&4)===4);return Cb(t,n,i,i.destroy),(n[Fn]??=new Im).queries.push(new xm(i))-1}function sS(t,n,e){let i=Be();return i.firstCreatePass&&(lS(i,new hd(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),oS(i,ae(),n)}function aS(t,n,e,i){let r=Be();if(r.firstCreatePass){let o=it();lS(r,new hd(n,e,i),o.index),sk(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return oS(r,ae(),e)}function ok(t){return t.split(",").map(n=>n.trim())}function lS(t,n,e){t.queries===null&&(t.queries=new Tm),t.queries.track(new Mm(n,e))}function sk(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function gp(t,n){return t.queries.getByIndex(n)}function cS(t,n){let e=t[V],i=gp(e,n);return i.crossesNgTemplate?Nm(e,t,n,[]):rS(e,t,i,n)}function dS(t,n,e){let i,r=Ns(()=>{i._dirtyCounter();let o=ak(i,t);if(n&&o===void 0)throw new y(-951,!1);return o});return i=r[Te],i._dirtyCounter=z(0),i._flatValue=void 0,r}function vp(t){return dS(!0,!1,t)}function _p(t){return dS(!0,!0,t)}function uS(t,n){let e=t[Te];e._lView=ae(),e._queryIndex=n,e._queryList=pp(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function ak(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[$]&4)return n?void 0:gt;let r=pp(e,i),o=cS(e,i);return r.reset(o,Gy),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function ui(t){return!!t&&typeof t.then=="function"}function bp(t){return!!t&&typeof t.subscribe=="function"}var Un=class{},Nd=class{};var md=class extends Un{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=J_(n);this._bootstrapComponents=qT(o.bootstrap),this._r3Injector=$h(n,e,[{provide:Un,useValue:this},...i],Ys(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},pd=class extends Nd{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new md(this.moduleType,n,[])}};var va=class extends Un{injector;instance=null;constructor(n){super();let e=new br([...n.providers,{provide:Un,useValue:this}],n.parent||bo(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function wa(t,n,e=null){return new va({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var lk=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=_h(!1,e.type),r=i.length>0?wa([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=se({token:t,providedIn:"environment",factory:()=>new t(L(we))})}return t})();function x(t){return ya(()=>{let n=fS(t),e=N(p({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==$m.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(lk).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||vn.Emulated,styles:t.styles||gt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&di("NgStandalone"),hS(e);let i=t.dependencies;return e.directiveDefs=_y(i,ck),e.pipeDefs=_y(i,eb),e.id=fk(e),e})}function ck(t){return Ti(t)||Nc(t)}function K(t){return ya(()=>({type:t.type,bootstrap:t.bootstrap||gt,declarations:t.declarations||gt,imports:t.imports||gt,exports:t.exports||gt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function dk(t,n){if(t==null)return Mi;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,l;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,l=r[3]||null):(o=r,s=r,a=Id.None,l=null),e[o]=[i,a,l],n[o]=s}return e}function uk(t){if(t==null)return Mi;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function M(t){return ya(()=>{let n=fS(t);return hS(n),n})}function fS(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Mi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||gt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:dk(t.inputs,n),outputs:uk(t.outputs),debugInfo:null}}function hS(t){t.features?.forEach(n=>n(t))}function _y(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function fk(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var mS=new g("");var yp=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(mS,{optional:!0})??[];injector=d(P);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Le(this.injector,r);if(ui(o))e.push(o);else if(bp(o)){let s=new Promise((a,l)=>{o.subscribe({complete:a,error:l})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function kd(t){return n=>{n.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:t}}}function Cp(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=hk,e.hostDirectives=i?t.map(km):[t]):i?e.hostDirectives.unshift(...t.map(km)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function hk(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let s=t[o];if(s.hostDirectives!==null){let a=n.length;i??=new Map,r??=new Map,pS(s,n,i,t),r.set(s,[a,n.length-1])}o===0&&hn(s)&&(e=!0,n.push(s))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return i!==null&&i.forEach((o,s)=>{mk(s.declaredInputs,o.inputs)}),[n,i,r]}function pS(t,n,e,i){if(t.hostDirectives!==null)for(let r of t.hostDirectives)if(typeof r=="function"){let o=r();for(let s of o)by(km(s),n,e,i)}else by(r,n,e,i)}function by(t,n,e,i){let r=Nc(t.directive);if(pS(r,n,e,i),e.has(r)){let o=e.get(r);yy(o,t.inputs,"input"),yy(o,t.outputs,"output")}else i.includes(r)||(e.set(r,t),n.push(r))}function yy(t,n,e){let i=e==="input"?t.inputs:t.outputs;Object.keys(n).forEach(r=>{let o=n[r];(!i.hasOwnProperty(r)||i[r]===o)&&(i[r]=o)})}function km(t){return typeof t=="function"?{directive:Qe(t),inputs:{},outputs:{}}:{directive:Qe(t.directive),inputs:Cy(t.inputs),outputs:Cy(t.outputs)}}function Cy(t){let n={};if(t!==void 0&&t.length>0)for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function mk(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function pk(t){return Object.getPrototypeOf(t.prototype).constructor}function Ne(t){let n=pk(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,Ks)?n[Ks]:void 0,s=Object.hasOwn(n,Xs)?n[Xs]:void 0;if(hn(t))r=o??s;else{if(o)throw new y(903,!1);r=s}if(r){if(e){i.push(r);let l=t;l.inputs=em(t.inputs),l.declaredInputs=em(t.declaredInputs),l.outputs=em(t.outputs);let c=r.hostBindings;c&&yk(t,c);let u=r.viewQuery,f=r.contentQueries;if(u&&_k(t,u),f&&bk(t,f),gk(t,r),Q_(t.outputs,r.outputs),hn(r)&&r.data.animation){let h=t.data;h.animation=(h.animation||[]).concat(r.data.animation)}}let a=r.features;if(a)for(let l=0;l<a.length;l++){let c=a[l];c&&c.ngInherit&&c(t),c===Ne&&(e=!1)}}n=Object.getPrototypeOf(n)}vk(i)}function gk(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function vk(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=To(r.hostAttrs,e=To(e,r.hostAttrs))}}function em(t){return t===Mi?{}:t===gt?[]:t}function _k(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function bk(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function yk(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function gS(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=To(t.mergedAttrs,t.attrs);let u=t.tView=ap(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),Do(t,!1);let l=Sk(e,n,t,i);Gc()&&op(e,n,l,t),Mo(l,n);let c=HC(l,n,l,t);n[i+We]=c,cp(n,c),JN(c,t,n)}function Ck(t,n,e,i,r,o,s,a,l,c,u){let f=e+We,h;return n.firstCreatePass?(h=Oo(n,f,4,s||null,a||null),Ah()&&XC(n,t,h,Xt(n.consts,c),NC),Oy(n,h)):h=n.data[f],gS(h,t,n,e,i,r,o,l),na(h)&&dp(n,t,h),c!=null&&Td(t,h,u),h}function _a(t,n,e,i,r,o,s,a,l,c,u){let f=e+We,h;if(n.firstCreatePass){if(h=Oo(n,f,4,s||null,a||null),c!=null){let m=Xt(n.consts,c);h.localNames=[];for(let v=0;v<m.length;v+=2)h.localNames.push(m[v],-1)}}else h=n.data[f];return gS(h,t,n,e,i,r,o,l),c!=null&&Td(t,h,u),h}function Nt(t,n,e,i,r,o,s,a){let l=ae(),c=Be(),u=Xt(c.consts,o);return Ck(l,c,t,n,e,i,r,u,void 0,s,a),Nt}var Sk=Dk;function Dk(t,n,e,i){return qc(!0),n[Me].createComment("")}var Ad=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var Sp=new g("");var Ea=new g("");function vS(){kf(()=>{let t="";throw new y(600,t)})}var wk=10;var Ht=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(Qt);afterRenderManager=d(Dd);zonelessEnabled=d(la);rootEffectScheduler=d(Zc);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new S;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(oi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ie(e=>!e))}constructor(){d(Hn,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(we);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=P.NULL){return this._injector.get(I).run(()=>{if(Ce(ge.BootstrapComponentStart),!this._injector.get(yp).done){let ne="";throw new y(405,ne)}let a=Ti(e),l=this._injector.get(Un),c=new ko(a,l);this.componentTypes.push(e);let{hostElement:u,directives:f,bindings:h}=Ek(i),m=u||c.selector,v=c.create(r,[],m,l.injector,f,h),D=v.location.nativeElement,A=v.injector.get(Sp,null);return A?.registerApplication(D),v.onDestroy(()=>{this.detachView(v.hostView),fa(this.components,v),A?.unregisterApplication(D)}),this._loadComponent(v),Ce(ge.BootstrapComponentEnd,v),v})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Ce(ge.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Sd.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Ce(ge.ChangeDetectionEnd),new y(101,!1);let e=F(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,F(e),this.afterTick.next(),Ce(ge.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ze,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<wk;){Ce(ge.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Ce(ge.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!ia(r))continue;let o=i&&!this.zonelessEnabled?0:1;VC(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>ia(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;fa(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Ea,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>fa(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new y(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function Ek(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function fa(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function ve(t,n,e,i){let r=ae(),o=Mr();if(Fi(r,o,n)){let s=Be(),a=oa();HM(a,r,t,n,e,i)}return ve}var Am=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function tm(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function xk(t,n,e,i){let r,o,s=0,a=t.length-1,l=void 0;if(Array.isArray(n)){F(i);let c=n.length-1;for(F(null);s<=a&&s<=c;){let u=t.at(s),f=n[s],h=tm(s,u,s,f,e);if(h!==0){h<0&&t.updateValue(s,f),s++;continue}let m=t.at(a),v=n[c],D=tm(a,m,c,v,e);if(D!==0){D<0&&t.updateValue(a,v),a--,c--;continue}let A=e(s,u),ne=e(a,m),ke=e(s,f);if(Object.is(ke,ne)){let Ye=e(c,v);Object.is(Ye,A)?(t.swap(s,a),t.updateValue(a,v),c--,a--):t.move(a,s),t.updateValue(s,f),s++;continue}if(r??=new gd,o??=Dy(t,s,a,e),Rm(t,r,s,ke))t.updateValue(s,f),s++,a++;else if(o.has(ke))r.set(A,t.detach(s)),a--;else{let Ye=t.create(s,n[s]);t.attach(s,Ye),s++,a++}}for(;s<=c;)Sy(t,r,e,s,n[s]),s++}else if(n!=null){F(i);let c=n[Symbol.iterator]();F(null);let u=c.next();for(;!u.done&&s<=a;){let f=t.at(s),h=u.value,m=tm(s,f,s,h,e);if(m!==0)m<0&&t.updateValue(s,h),s++,u=c.next();else{r??=new gd,o??=Dy(t,s,a,e);let v=e(s,h);if(Rm(t,r,s,v))t.updateValue(s,h),s++,a++,u=c.next();else if(!o.has(v))t.attach(s,t.create(s,h)),s++,a++,u=c.next();else{let D=e(s,f);r.set(D,t.detach(s)),a--}}}for(;!u.done;)Sy(t,r,e,t.length,u.value),u=c.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(c=>{t.destroy(c)})}function Rm(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function Sy(t,n,e,i,r){if(Rm(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function Dy(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var gd=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function ee(t,n,e,i,r,o,s,a){di("NgControlFlow");let l=ae(),c=Be(),u=Xt(c.consts,o);return _a(l,c,t,n,e,i,r,u,256,s,a),Dp}function Dp(t,n,e,i,r,o,s,a){di("NgControlFlow");let l=ae(),c=Be(),u=Xt(c.consts,o);return _a(l,c,t,n,e,i,r,u,512,s,a),Dp}function te(t,n){di("NgControlFlow");let e=ae(),i=Mr(),r=e[i]!==Jt?e[i]:-1,o=r!==-1?vd(e,We+r):void 0,s=0;if(Fi(e,i,t)){let a=F(null);try{if(o!==void 0&&$C(o,s),t!==-1){let l=We+t,c=vd(e,l),u=Lm(e[V],l),f=GC(c,u,e),h=Sa(e,u,n,{dehydratedView:f});Da(c,h,s,No(u,f))}}finally{F(a)}}else if(o!==void 0){let a=zC(o,s);a!==void 0&&(a[$e]=n)}}var Om=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-Ve}};function _n(t){return t}function Po(t,n){return n}var Pm=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function st(t,n,e,i,r,o,s,a,l,c,u,f,h){di("NgControlFlow");let m=ae(),v=Be(),D=l!==void 0,A=ae(),ne=a?s.bind(A[Tt][$e]):s,ke=new Pm(D,ne);A[We+t]=ke,_a(m,v,t+1,n,e,i,r,Xt(v.consts,o),256),D&&_a(m,v,t+2,l,c,u,f,Xt(v.consts,h),512)}var Fm=class extends Am{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-Ve}at(n){return this.getLView(n)[$e].$implicit}attach(n,e){let i=e[Sr];this.needsIndexUpdate||=n!==this.length,Da(this.lContainer,e,n,No(this.templateTNode,i)),Ik(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,Tk(this.lContainer,n),Mk(this.lContainer,n)}create(n,e){let i=ad(this.lContainer,this.templateTNode.tView.ssrId);return Sa(this.hostLView,this.templateTNode,new Om(this.lContainer,e,n),{dehydratedView:i})}destroy(n){Ed(n[V],n)}updateValue(n,e){this.getLView(n)[$e].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[$e].$index=n}getLView(n){return Nk(this.lContainer,n)}};function at(t){let n=F(null),e=ri();try{let i=ae(),r=i[V],o=i[e],s=e+1,a=vd(i,s);if(o.liveCollection===void 0){let c=Lm(r,s);o.liveCollection=new Fm(a,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(xk(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=Mr(),u=l.length===0;if(Fi(i,c,u)){let f=e+2,h=vd(i,f);if(u){let m=Lm(r,f),v=GC(h,m,i),D=Sa(i,m,void 0,{dehydratedView:v});Da(h,D,0,No(m,v))}else r.firstUpdatePass&&hN(h),$C(h,0)}}}finally{F(n)}}function vd(t,n){return t[n]}function Ik(t,n){if(t.length<=Ve)return;let e=Ve+n,i=t[e],r=i?i[Vn]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[On];fM(o,r),Oi.delete(i[Ln]),r.detachedLeaveAnimationFns=void 0}}function Tk(t,n){if(t.length<=Ve)return;let e=Ve+n,i=t[e],r=i?i[Vn]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function Mk(t,n){return ga(t,n)}function Nk(t,n){return zC(t,n)}function Lm(t,n){return Lc(t,n)}function j(t,n,e){let i=ae(),r=Mr();if(Fi(i,r,n)){let o=Be(),s=oa();LM(s,i,t,n,i[Me],e)}return j}function Vm(t,n,e,i,r){up(n,t,e,r?"class":"style",i)}function b(t,n,e,i){let r=ae(),o=r[V],s=t+We,a=o.firstCreatePass?JC(s,r,2,n,NC,Ah(),e,i):o.data[s];if(ii(a)){let l=r[Pn].tracingService;if(l&&l.componentCreate){let c=o.data[a.directiveStart+a.componentOffset];return l.componentCreate(ZC(c),()=>(wy(t,n,r,a,i),b))}}return wy(t,n,r,a,i),b}function wy(t,n,e,i,r){if(kC(i,e,t,n,_S),na(i)){let o=e[V];dp(o,e,i),oC(o,i,e)}r!=null&&Td(e,i)}function _(){let t=Be(),n=it(),e=AC(n);return t.firstCreatePass&&eS(t,e),Oh(e)&&Ph(),kh(),e.classesWithoutHost!=null&&rT(e)&&Vm(t,e,ae(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&oT(e)&&Vm(t,e,ae(),e.stylesWithoutHost,!1),_}function O(t,n,e,i){return b(t,n,e,i),_(),O}function ut(t,n,e,i){let r=ae(),o=r[V],s=t+We,a=o.firstCreatePass?LN(s,o,2,n,e,i):o.data[s];return kC(a,r,t,n,_S),i!=null&&Td(r,a),ut}function yt(){let t=it(),n=AC(t);return Oh(n)&&Ph(),kh(),yt}function nn(t,n,e,i){return ut(t,n,e,i),yt(),nn}var _S=(t,n,e,i,r)=>(qc(!0),fC(n[Me],i,zh()));function rn(){return ae()}function zt(t,n,e){let i=ae(),r=Mr();if(Fi(i,r,n)){let o=Be(),s=oa();MC(s,i,t,n,i[Me],e)}return zt}var xa="en-US";var kk=xa;function bS(t){typeof t=="string"&&(kk=t.toLowerCase().replace(/_/g,"-"))}function G(t,n,e){let i=ae(),r=Be(),o=it();return Ak(r,i,i[Me],o,t,n,e),G}function Rd(t,n,e){let i=ae(),r=Be(),o=it();return(o.type&3||e)&&mp(o,r,i,e,i[Me],t,n,Ar(o,i,n)),Rd}function Ak(t,n,e,i,r,o,s){let a=!0,l=null;if((i.type&3||s)&&(l??=Ar(i,n,o),mp(i,t,n,s,e,r,o,l)&&(a=!1)),a){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let h=u[f],m=u[f+1];l??=Ar(i,n,o),cd(i,n,h,m,r,l)}if(c&&c.length)for(let f of c)l??=Ar(i,n,o),cd(i,n,f,r,r,l)}}function re(t=1){return Pb(t)}function Rk(t,n){let e=null,i=JT(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?vC(t,o,!0):nM(i,o))return r}return e}function xe(t){let n=ae()[Tt][_t];if(!n.projection){let e=t?t.length:1,i=n.projection=sb(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?Rk(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function le(t,n=0,e,i,r,o){let s=ae(),a=Be(),l=i?t+1:null;l!==null&&_a(s,a,l,i,r,o,null,e);let c=Oo(a,We+t,16,null,e||null);c.projection===null&&(c.projection=n),Vh();let f=!s[Sr]||Rh();s[Tt][_t].projection[c.projection]===null&&l!==null?Ok(s,a,l):f&&!bd(c)&&IM(a,s,c)}function Ok(t,n,e){let i=We+e,r=n.data[i],o=t[i],s=ad(o,r.tView.ssrId),a=Sa(t,r,void 0,{dehydratedView:s});Da(o,a,0,No(r,s))}function bn(t,n,e,i){return aS(t,n,e,i),bn}function Pe(t,n,e){return sS(t,n,e),Pe}function q(t){let n=ae(),e=Be(),i=Hc();ra(i+1);let r=gp(e,i);if(t.dirty&&vb(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=cS(n,i);t.reset(o,Gy),t.notifyOnChanges()}return!0}return!1}function Y(){return pp(ae(),Hc())}function Od(t,n,e,i,r){return uS(n,aS(t,e,i,r)),Od}function Pd(t,n,e,i){return uS(t,sS(n,e,i)),Pd}function Fd(t=1){ra(Hc()+t)}function fi(t){let n=Eb();return gb(n,We+t)}function Qc(t,n){return t<<17|n<<2}function Fr(t){return t>>17&32767}function Pk(t){return(t&2)==2}function Fk(t,n){return t&131071|n<<17}function Bm(t){return t|2}function Ao(t){return(t&131068)>>2}function nm(t,n){return t&-131069|n<<2}function Lk(t){return(t&1)===1}function jm(t){return t|1}function Vk(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=Fr(s),l=Ao(s);t[i]=e;let c=!1,u;if(Array.isArray(e)){let f=e;u=f[1],(u===null||_o(f,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let h=Fr(t[a+1]);t[i+1]=Qc(h,a),h!==0&&(t[h+1]=nm(t[h+1],i)),t[a+1]=Fk(t[a+1],i)}else t[i+1]=Qc(a,0),a!==0&&(t[a+1]=nm(t[a+1],i)),a=i;else t[i+1]=Qc(l,0),a===0?a=i:t[l+1]=nm(t[l+1],i),l=i;c&&(t[i+1]=Bm(t[i+1])),Ey(t,u,i,!0),Ey(t,u,i,!1),Bk(n,u,t,i,o),s=Qc(a,l),o?n.classBindings=s:n.styleBindings=s}function Bk(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&_o(o,n)>=0&&(e[i+1]=jm(e[i+1]))}function Ey(t,n,e,i){let r=t[e+1],o=n===null,s=i?Fr(r):Ao(r),a=!1;for(;s!==0&&(a===!1||o);){let l=t[s],c=t[s+1];jk(l,n)&&(a=!0,t[s+1]=i?jm(c):Bm(c)),s=i?Fr(c):Ao(c)}a&&(t[e+1]=i?Bm(r):jm(r))}function jk(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?_o(t,n)>=0:!1}var gn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function Uk(t){return t.substring(gn.key,gn.keyEnd)}function Hk(t){return zk(t),yS(t,CS(t,0,gn.textEnd))}function yS(t,n){let e=gn.textEnd;return e===n?-1:(n=gn.keyEnd=$k(t,gn.key=n,e),CS(t,n,e))}function zk(t){gn.key=0,gn.keyEnd=0,gn.value=0,gn.valueEnd=0,gn.textEnd=t.length}function CS(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function $k(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function on(t,n,e){return SS(t,n,e,!1),on}function Z(t,n){return SS(t,n,null,!0),Z}function ft(t){Gk(Qk,Wk,t,!0)}function Wk(t,n){for(let e=Hk(n);e>=0;e=yS(n,e))Oc(t,Uk(n),!0)}function SS(t,n,e,i){let r=ae(),o=Be(),s=jh(2);if(o.firstUpdatePass&&wS(o,t,s,i),n!==Jt&&Fi(r,s,n)){let a=o.data[ri()];ES(o,a,r,r[Me],t,r[s+1]=eA(n,e),i,s)}}function Gk(t,n,e,i){let r=Be(),o=jh(2);r.firstUpdatePass&&wS(r,null,o,i);let s=ae();if(e!==Jt&&Fi(s,o,e)){let a=r.data[ri()];if(xS(a,i)&&!DS(r,o)){let l=i?a.classesWithoutHost:a.stylesWithoutHost;l!==null&&(e=Tc(l,e||"")),Vm(r,a,s,e,i)}else Jk(r,a,s,s[Me],s[o+1],s[o+1]=Xk(t,n,e),i,o)}}function DS(t,n){return n>=t.expandoStartIndex}function wS(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[ri()],s=DS(t,e);xS(o,i)&&n===null&&!s&&(n=!1),n=qk(r,o,n,i),Vk(r,o,n,e,s,i)}}function qk(t,n,e,i){let r=kb(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=im(null,t,n,e,i),e=ba(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=im(r,t,n,e,i),o===null){let l=Yk(t,n,i);l!==void 0&&Array.isArray(l)&&(l=im(null,t,n,l[1],i),l=ba(l,n.attrs,i),Zk(t,n,i,l))}else o=Kk(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function Yk(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Ao(i)!==0)return t[Fr(i)]}function Zk(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Fr(r)]=i}function Kk(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=ba(i,s,e)}return ba(i,n.attrs,e)}function im(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=ba(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function ba(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Oc(t,s,e?!0:n[++o]))}return t===void 0?null:t}function Xk(t,n,e){if(e==null||e==="")return gt;let i=[],r=li(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function Qk(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Oc(t,i,e)}function Jk(t,n,e,i,r,o,s,a){r===Jt&&(r=gt);let l=0,c=0,u=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;u!==null||f!==null;){let h=l<r.length?r[l+1]:void 0,m=c<o.length?o[c+1]:void 0,v=null,D;u===f?(l+=2,c+=2,h!==m&&(v=f,D=m)):f===null||u!==null&&u<f?(l+=2,v=u):(c+=2,v=f,D=m),v!==null&&ES(t,n,e,i,v,D,s,a),u=l<r.length?r[l]:null,f=c<o.length?o[c]:null}}function ES(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let l=t.data,c=l[a+1],u=Lk(c)?xy(l,n,e,r,Ao(c),s):void 0;if(!_d(u)){_d(o)||Pk(c)&&(o=xy(l,null,e,r,a,s));let f=xh(ri(),e);MM(i,s,f,r,o)}}function xy(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,f=u===null,h=e[r+1];h===Jt&&(h=f?gt:void 0);let m=f?Pc(h,i):u===i?h:void 0;if(c&&!_d(m)&&(m=Pc(l,i)),_d(m)&&(a=m,s))return a;let v=t[r+1];r=s?Fr(v):Ao(v)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(a=Pc(l,i))}return a}function _d(t){return t!==void 0}function eA(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=Ys(li(t)))),t}function xS(t,n){return(t.flags&(n?8:16))!==0}function X(t,n=""){let e=ae(),i=Be(),r=t+We,o=i.firstCreatePass?Oo(i,r,1,n,null):i.data[r],s=tA(i,e,o,n);e[r]=s,Gc()&&op(i,e,s,o),Do(o,!1)}var tA=(t,n,e,i)=>(qc(!0),zT(n[Me],i));function nA(t,n,e,i=""){return Fi(t,Mr(),e)?n+hh(e)+i:Jt}function Ct(t){return yn("",t),Ct}function yn(t,n,e){let i=ae(),r=nA(i,t,n,e);return r!==Jt&&iA(i,ri(),r),yn}function iA(t,n,e){let i=xh(n,t);$T(t[Me],i,e)}function Iy(t,n,e){let i=Be();i.firstCreatePass&&IS(n,i.data,i.blueprint,hn(t),e)}function IS(t,n,e,i,r){if(t=Qe(t),Array.isArray(t))for(let o=0;o<t.length;o++)IS(t[o],n,e,i,r);else{let o=Be(),s=ae(),a=it(),l=_r(t)?t:Qe(t.provide),c=yh(t),u=a.providerIndexes&1048575,f=a.directiveStart,h=a.providerIndexes>>20;if(_r(t)||!t.multi){let m=new Rr(c,r,ot,null),v=om(l,n,r?u:u+h,f);v===-1?(lm(sd(a,s),o,l),rm(o,t,n.length),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(m),s.push(m)):(e[v]=m,s[v]=m)}else{let m=om(l,n,u+h,f),v=om(l,n,u,u+h),D=m>=0&&e[m],A=v>=0&&e[v];if(r&&!A||!r&&!D){lm(sd(a,s),o,l);let ne=sA(r?oA:rA,e.length,r,i,c,t);!r&&A&&(e[v].providerFactory=ne),rm(o,t,n.length,0),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(ne),s.push(ne)}else{let ne=TS(e[r?v:m],c,!r&&i);rm(o,t,m>-1?m:v,ne)}!r&&i&&A&&e[v].componentProviders++}}}function rm(t,n,e,i){let r=_r(n),o=fb(n);if(r||o){let l=(o?Qe(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function TS(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function om(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function rA(t,n,e,i,r){return Um(this.multi,[])}function oA(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,l=ha(i,i[V],this.providerFactory.index,r);s=l.slice(0,a),Um(o,s);for(let c=a;c<l.length;c++)s.push(l[c])}else s=[],Um(o,s);return s}function Um(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function sA(t,n,e,i,r,o){let s=new Rr(t,e,ot,null);return s.multi=[],s.index=n,s.componentProviders=0,TS(s,r,i&&!e),s}function Ae(t,n){return e=>{e.providersResolver=(i,r)=>Iy(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>Iy(i,r?r(n):n,!0))}}function Ld(t,n){let e=xb()+t,i=ae();return i[e]===Jt?_N(i,e,n()):bN(i,e)}function wp(t,n){return Md(t,n)}var MS=(()=>{class t{applicationErrorHandler=d(Qt);appRef=d(Ht);taskService=d(oi);ngZone=d(I);zonelessEnabled=d(la);tracing=d(Hn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new pe;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Gs):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(Zh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Bb:Wh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Gs+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function NS(){return[{provide:kn,useExisting:MS},{provide:I,useClass:qs},{provide:la,useValue:!0}]}var Ep=(()=>{class t{compileModuleSync(e){return new pd(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function aA(){return typeof $localize<"u"&&$localize.locale||xa}var Vd=new g("",{factory:()=>d(Vd,{optional:!0,skipSelf:!0})||aA()});var Bd=class{destroyed=!1;listeners=null;errorHandler=d(vt,{optional:!0});isEmitting=!1;hasNullListeners=!1;destroyRef=d(Ue);constructor(){this.destroyRef.onDestroy(()=>{this.destroyed=!0,this.listeners=null})}subscribe(n){if(this.destroyed)throw new y(953,!1);return(this.listeners??=[]).push(n),{unsubscribe:()=>{let e=this.listeners?this.listeners.indexOf(n):-1;e>-1&&(this.isEmitting?(this.hasNullListeners=!0,this.listeners[e]=null):this.listeners.splice(e,1))}}}emit(n){if(this.destroyed){console.warn(An(953,!1));return}if(this.listeners===null)return;this.isEmitting=!0;let e=F(null);try{for(let i of this.listeners)try{i!==null&&i(n)}catch(r){this.errorHandler?.handleError(r)}}finally{this.hasNullListeners&&(this.hasNullListeners=!1,this.listeners&&lA(this.listeners)),F(e),this.isEmitting=!1}}};function lA(t){let n=t.length-1;for(;n>-1;)t[n]===null&&t.splice(n,1),n--}function E(t,n){return Ns(t,n?.equal)}function B(t){return b_(t)}var kS=class t extends Error{_brand;constructor(n){super(n)}static IDLE=new t("IDLE");static LOADING=new t("LOADING")},cA=t=>t;function hi(t,n){if(typeof t=="function"){let e=Ff(t,cA,n?.equal);return AS(e,n?.debugName,n?.set)}else{let e=Ff(t.source,t.computation,t.equal);return AS(e,t.debugName,t.set)}}function AS(t,n,e){let i=t[Te],r=t;if(e!==void 0){let o=s=>Lf(i,s);r.set=s=>e(s,o),r.update=s=>e(s(B(t)),o)}else r.set=o=>Lf(i,o),r.update=o=>__(i,o);return r.asReadonly=sa.bind(t),r}var RS=!1;function xp(){return RS}function jd(t){RS=t}var Hd=Symbol("InputSignalNode#UNSET"),US=N(p({},ks),{transformFn:void 0,applyValueToInputSignal(t,n){wi(t,n)}});function HS(t,n){let e=Object.create(US);e.value=t,e.transformFn=n?.transform;function i(){if(Yn(e),e.value===Hd){let r=null;throw new y(-950,r)}return e.value}return i[Te]=e,i}var mi=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>zm(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function Np(t){return MA(t)?t.default:t}function MA(t){return t&&typeof t=="object"&&"default"in t}function OS(t,n){return HS(t,n)}function NA(t){return HS(Hd,t)}var ht=(OS.required=NA,OS);function zS(t,n){let e=Object.create(US),i=new Bd;e.value=t;function r(){return Yn(e),PS(e.value),e.value}return r[Te]=e,r.asReadonly=sa.bind(r),r.set=o=>{e.equal(e.value,o)||(wi(e,o),i.emit(o))},r.update=o=>{PS(e.value),r.set(o(e.value))},r.subscribe=i.subscribe.bind(i),r.destroyRef=i.destroyRef,r}function PS(t){if(t===Hd)throw new y(952,!1)}function FS(t,n){return zS(t,n)}function kA(t){return zS(Hd,t)}var $S=(FS.required=kA,FS);function LS(t,n){return vp(n)}function AA(t,n){return _p(n)}var Ta=(LS.required=AA,LS);function VS(t,n){return vp(n)}function RA(t,n){return _p(n)}var WS=(VS.required=RA,VS);var OA=1e4;var k6=OA-1e3;var Fe=(()=>{class t{static __NG_ELEMENT_ID__=PA}return t})();function PA(t){return FA(it(),ae(),(t&16)===16)}function FA(t,n,e){if(ii(t)&&!e){let i=Kt(t.index,n);return new Pi(i,i)}else if(t.type&175){let i=n[Tt];return new Pi(i,n)}return null}var Tp=new g(""),LA=new g("");function Ia(t){return!t.moduleRef}function VA(t){let n=Ia(t)?t.r3Injector:t.moduleRef.injector,e=n.get(I);return e.run(()=>{Ia(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Qt),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Ia(t)){let o=()=>n.destroy(),s=t.platformInjector.get(Tp);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(Tp);s.add(o),t.moduleRef.onDestroy(()=>{fa(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return jA(i,e,()=>{let o=n.get(oi),s=o.add(),a=n.get(yp);return a.runInitializers(),a.donePromise.then(()=>{let l=n.get(Vd,xa);if(bS(l||xa),!n.get(LA,!0))return Ia(t)?n.get(Ht):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Ia(t)){let u=n.get(Ht);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return BA?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var BA;function jA(t,n,e){try{let i=e();return ui(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Ud=null;function UA(t=[],n){return P.create({name:n,providers:[{provide:ea,useValue:"platform"},{provide:Tp,useValue:new Set([()=>Ud=null])},...t]})}function HA(t=[]){if(Ud)return Ud;let n=UA(t);return Ud=n,vS(),zA(n),n}function zA(t){let n=t.get(Yc,null);Le(t,()=>{n?.forEach(e=>e())})}function GS(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Ce(ge.BootstrapApplicationStart);try{let o=r?.injector??HA(i),s=[NS(),Ub,...e||[]],a=new va({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return VA({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Ce(ge.BootstrapApplicationEnd)}}function Q(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function St(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Ip=Symbol("NOT_SET"),qS=new Set,$A=N(p({},ks),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Ip,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Ip&&!ar(this))return this.signal;try{for(let r of this.cleanup??qS)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=Tn(this),i;try{i=this.userFn.apply(null,n)}finally{Zn(this,e)}return(this.value===Ip||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Mp=class extends ma{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(Ue),s),this.scheduler=r;for(let a of tp){let l=e[a];if(l===void 0)continue;let c=Object.create($A);c.sequence=this,c.phase=a,c.userFn=l,c.dirty=!0,c.signal=()=>(Yn(c),c.value),c.signal[Te]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[a]=c,this.hooks[a]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??qS)e()}finally{Kn(n)}}};function Fo(t,n){let e=n?.injector??d(P),i=e.get(kn),r=e.get(Dd),o=e.get(Hn,null,{optional:!0});r.impl??=e.get(np);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(Eo,null,{optional:!0}),l=new Mp(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function zd(t,n){let e=Ti(t),i=n.elementInjector||bo();return new ko(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var YS=null;function sn(){return YS}function kp(t){YS??=t}var Ma=class{},Lo=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:()=>d(ZS),providedIn:"platform"})}return t})();var ZS=(()=>{class t extends Lo{_location;_history;_doc=d(R);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return sn().getBaseHref(this._doc)}onPopState(e){let i=sn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=sn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function QS(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function KS(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Li(t){return t&&t[0]!=="?"?`?${t}`:t}var $d=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:()=>d(GA),providedIn:"root"})}return t})(),WA=new g(""),GA=(()=>{class t extends $d{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(R).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return QS(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Li(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+Li(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+Li(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(L(Lo),L(WA,8))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Vi=(()=>{class t{_subject=new S;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=ZA(KS(XS(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Li(i))}normalize(e){return t.stripTrailingSlash(YA(this._basePath,XS(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Li(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Li(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Li;static joinWithSlash=QS;static stripTrailingSlash=KS;static \u0275fac=function(i){return new(i||t)(L($d))};static \u0275prov=se({token:t,factory:()=>qA(),providedIn:"root"})}return t})();function qA(){return new Vi(L($d))}function YA(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function XS(t){return t.replace(/\/index\.html$/,"")}function ZA(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Ap=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(P);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ot(Mt))};static \u0275dir=M({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ge]})}return t})();function Na(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var Rp="browser";function JS(t){return t===Rp}var ka=class{_doc;constructor(n){this._doc=n}manager},Wd=(()=>{class t extends ka{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(L(R))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),Yd=new g(""),Lp=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof Wd));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof Wd);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new y(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(L(Yd),L(I))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),Op="ng-app-id";function eD(t){for(let n of t)n.remove()}function tD(t,n){let e=n.createElement("style");return e.textContent=t,e}function eR(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Op}="${n}"],link[${Op}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(Op),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Fp(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Vp=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,eR(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,tD);i?.forEach(r=>this.addUsage(r,this.external,Fp))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(eD(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])eD(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,tD(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Fp(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(L(R),L(mn),L(si,8),L(Nr))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),Pp={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Bp=/%COMP%/g;var iD="%COMP%",tR=`_nghost-${iD}`,nR=`_ngcontent-${iD}`,iR=!0,rR=new g("",{factory:()=>iR}),oR=new g("");function sR(t){return nR.replace(Bp,t)}function aR(t){return tR.replace(Bp,t)}function rD(t,n){return n.map(e=>e.replace(Bp,t))}var jp=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(e,i,r,o,s,a,l=null,c=null,u=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=l,this.tracingService=c,this.cssVarNamespace=u??"",this.defaultRenderer=new Aa(e,s,a,this.tracingService,this.cssVarNamespace)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof qd?r.applyToHost(e):r instanceof Ra&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case vn.Emulated:o=new qd(l,c,i,this.appId,u,s,a,f,this.cssVarNamespace);break;case vn.ShadowDom:return new Gd(l,e,i,s,a,this.nonce,f,this.cssVarNamespace,c);case vn.ExperimentalIsolatedShadowDom:return new Gd(l,e,i,s,a,this.nonce,f,this.cssVarNamespace);default:o=new Ra(l,c,i,u,s,a,f,this.cssVarNamespace);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(L(Lp),L(Vr),L(mn),L(rR),L(R),L(I),L(si),L(Hn,8),L(oR,8))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),Aa=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r,o=""){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r,this.cssVarNamespace=o}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Pp[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(nD(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(nD(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new y(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Pp[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Pp[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){let o=e.startsWith("--");o&&(e=e.replace("%NS%",this.cssVarNamespace)),o||r&(jn.DashCase|jn.Important)?n.style.setProperty(e,i,r&jn.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){let r=e.startsWith("--");r&&(e=e.replace("%NS%",this.cssVarNamespace)),r||i&jn.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=sn().getGlobalEventTarget(this.doc,n),!n))throw new y(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function nD(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Gd=class extends Aa{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,l,c){super(n,r,o,a,l),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let u=i.styles;u=rD(i.id,u).map(h=>h.replace(/%NS%/g,l));for(let h of u){let m=document.createElement("style");s&&m.setAttribute("nonce",s),m.textContent=h,this.shadowRoot.appendChild(m)}let f=i.getExternalStyles?.();if(f)for(let h of f){let m=Fp(h,r);s&&m.setAttribute("nonce",s),this.shadowRoot.appendChild(m)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Ra=class extends Aa{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,l,c){super(n,o,s,a,l),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let u=i.styles,f=c?rD(c,u):u;this.styles=f.map(h=>h.replace(/%NS%/g,l)),this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Oi.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},qd=class extends Ra{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,l,c){let u=r+"-"+i.id;super(n,e,i,o,s,a,l,c,u),this.contentAttr=sR(u),this.hostAttr=aR(u)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Zd=class t extends Ma{supportsDOMEvents=!0;static makeCurrent(){kp(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=lR();return e==null?null:cR(e)}resetBaseElement(){Oa=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Na(document.cookie,n)}},Oa=null;function lR(){return Oa=Oa||document.head.querySelector("base"),Oa?Oa.getAttribute("href"):null}function cR(t){return new URL(t,document.baseURI).pathname}var oD=["alt","control","meta","shift"],dR={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},uR={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},sD=(()=>{class t extends ka{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>sn().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),oD.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),s+=c+".")}),s+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=s,l}static matchEventFullKeyCode(e,i){let r=dR[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),oD.forEach(s=>{if(s!==r){let a=uR[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(L(R))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})();async function Up(t,n,e){let i=p({rootComponent:t},fR(n,e));return GS(i)}function fR(t,n){return{platformRef:n?.platformRef,appProviders:[...vR,...t?.providers??[]],platformProviders:gR}}function hR(){Zd.makeCurrent()}function mR(){return new vt}function pR(){return Wm(document),document}var gR=[{provide:Nr,useValue:Rp},{provide:Yc,useValue:hR,multi:!0},{provide:R,useFactory:pR}];var vR=[{provide:ea,useValue:"root"},{provide:vt,useFactory:mR},{provide:Yd,useClass:Wd,multi:!0},{provide:Yd,useClass:sD,multi:!0},jp,{provide:Vr,useClass:Vp},{provide:Vp,useExisting:Vr},Lp,{provide:Ze,useExisting:jp},[]];var gi=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init();for(let[e,i]of n.headers.entries())this.headers.set(e,i),this.normalizedNames.set(e,n.normalizedNames.get(e))}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=n.op==="a"?(this.headers.get(e)||[]).slice():[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(o===void 0)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=Array.isArray(o)?o:[o],a=this.headers.get(e);if(!a)return;a=a.filter(l=>s.indexOf(l)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var $p=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Wp=class{encodeKey(n){return aD(n)}encodeValue(n){return aD(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function _R(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(s)||[];l.push(a),e.set(s,l)}),e}var bR=/%(\d[a-f0-9])/gi,yR={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function aD(t){return encodeURIComponent(t).replace(bR,(n,e)=>yR[e]??n)}function Kd(t){return`${t}`}var pi=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Wp,n.fromString){if(n.fromObject)throw new y(2805,!1);this.map=_R(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Kd):[Kd(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){if(this.map===null&&(this.map=new Map),this.cloneFrom!==null){this.cloneFrom.init();for(let[n,e]of this.cloneFrom.map.entries())this.map.set(n,e);this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=n.op==="a"?(this.map.get(n.param)||[]).slice():[];e.push(Kd(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=(this.map.get(n.param)||[]).slice(),r=i.indexOf(Kd(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null}}};function CR(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function lD(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function cD(t){return typeof Blob<"u"&&t instanceof Blob}function dD(t){return typeof FormData<"u"&&t instanceof FormData}function SR(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var Hp="Content-Type",uD="Accept",hD="text/plain",mD="application/json",DR=`${mD}, ${hD}, */*`,Vo=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(CR(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new y(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new gi,this.context??=new $p,!this.params)this.params=new pi,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,l="",c=e.indexOf("#");c!==-1&&(l=e.substring(c),a=e.substring(0,c));let u=a.indexOf("?"),f=u===-1?"?":u<a.length-1?"&":"";this.urlWithParams=a+f+s+l}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||lD(this.body)||cD(this.body)||dD(this.body)||SR(this.body)?this.body:this.body instanceof pi?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||dD(this.body)?null:cD(this.body)?this.body.type||null:lD(this.body)?null:typeof this.body=="string"?hD:this.body instanceof pi?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?mD:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,f=n.referrer??this.referrer,h=n.integrity||this.integrity,m=n.referrerPolicy||this.referrerPolicy,v=n.transferCache??this.transferCache,D=n.timeout??this.timeout,A=n.body!==void 0?n.body:this.body,ne=n.withCredentials??this.withCredentials,ke=n.reportProgress??this.reportProgress,Ye=n.reportUploadProgress??this.reportUploadProgress,ro=n.reportDownloadProgress??this.reportDownloadProgress,xs=n.headers||this.headers,tr=n.params||this.params,Bl=n.context??this.context;return n.setHeaders!==void 0&&(xs=Object.keys(n.setHeaders).reduce((oo,nr)=>oo.set(nr,n.setHeaders[nr]),xs)),n.setParams&&(tr=Object.keys(n.setParams).reduce((oo,nr)=>oo.set(nr,n.setParams[nr]),tr)),new t(e,i,A,{params:tr,headers:xs,context:Bl,reportProgress:ke,reportUploadProgress:Ye,reportDownloadProgress:ro,responseType:r,withCredentials:ne,transferCache:v,keepalive:o,cache:a,priority:s,timeout:D,mode:l,redirect:c,credentials:u,referrer:f,integrity:h,referrerPolicy:m})}},Bo=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(Bo||{}),Pa=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new gi,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Gp=class t extends Pa{constructor(n={}){super(n)}type=Bo.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Fa=class t extends Pa{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=Bo.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Br=class extends Pa{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},wR=200;var ER=/^\)\]\}',?\n/,XY=1024*1024,xR=new g("",{factory:()=>null}),IR=(()=>{class t{fetchImpl=d(qp,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=d(I);destroyRef=d(Ue);maxResponseSize=d(xR);handle(e){return new J(i=>{let r=new AbortController;this.doRequest(e,r.signal,i).then(Yp,s=>i.error(new Br({error:s})));let o;return e.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}async doRequest(e,i,r){let o=this.createRequestInit(e),s;try{let A=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,p({signal:i},o)));TR(A),r.next({type:Bo.Sent}),s=await A}catch(A){r.error(new Br({error:A,status:A.status??0,statusText:A.statusText,url:e.urlWithParams,headers:A.headers}));return}let a=new gi(s.headers),l=s.statusText,c=s.url||e.urlWithParams,u=s.status,f=null,h=e.reportProgress||e.reportDownloadProgress;if(h&&r.next(new Gp({headers:a,status:u,statusText:l,url:c})),s.body){let A=s.headers.get("content-length"),ne=A!==null?Number(A):NaN;this.maxResponseSize!==null&&Number.isFinite(ne)&&ne>this.maxResponseSize&&fD(this.maxResponseSize);let ke=[],Ye=s.body.getReader(),ro=0,xs,tr,Bl=typeof Zone<"u"&&Zone.current,oo=!1;if(await this.ngZone.runOutsideAngular(async()=>{for(;;){if(this.destroyRef.destroyed){await Ye.cancel(),oo=!0;break}let{done:Is,value:If}=await Ye.read();if(Is)break;if(ke.push(If),ro+=If.length,this.maxResponseSize!==null&&ro>this.maxResponseSize&&(await Ye.cancel(),fD(this.maxResponseSize)),h){tr=e.responseType==="text"?(tr??"")+(xs??=new TextDecoder).decode(If,{stream:!0}):void 0;let s_=()=>r.next({type:Bo.DownloadProgress,total:Number.isFinite(ne)?ne:void 0,loaded:ro,partialText:tr});Bl?Bl.run(s_):s_()}}}),oo){r.complete();return}let nr=this.concatChunks(ke,ro);try{let Is=s.headers.get(Hp)??"";f=this.parseBody(e,nr,Is,u)}catch(Is){r.error(new Br({error:Is,headers:new gi(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}u===0&&(u=f?wR:0);let m=u>=200&&u<300,v=s.redirected,D=s.type;m?(r.next(new Fa({body:f,headers:a,status:u,statusText:l,url:c,redirected:v,responseType:D})),r.complete()):r.error(new Br({error:f,headers:a,status:u,statusText:l,url:c,redirected:v,responseType:D}))}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(ER,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new y(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(uD)||(i[uD]=DR),!e.headers.has(Hp)){let o=e.detectContentTypeHeader();o!==null&&(i[Hp]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),qp=class{};function Yp(){}function TR(t){t.then(Yp,Yp)}function fD(t){throw new y(-2825,!1)}var MR=new g("",{factory:()=>!0}),NR="XSRF-TOKEN",kR=new g("",{factory:()=>NR}),AR="X-XSRF-TOKEN",RR=new g("",{factory:()=>AR}),OR=(()=>{class t{cookieName=d(kR);doc=d(R);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=Na(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),PR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=L(OR),r},providedIn:"root"})}return t})();function FR(t,n){if(!d(MR)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(Lo).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch{return n(t)}let e=d(PR).getToken(),i=d(RR);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}function LR(t,n){return n(t)}function VR(t,n,e){return(i,r)=>Le(e,()=>n(i,o=>t(o,r)))}var BR=new g("",{factory:()=>[FR]}),pD=new g(""),jR=new g("",{factory:()=>!0});var UR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=L(IR),r},providedIn:"root"})}return t})();var HR=(()=>{class t{backend;injector;chain=null;pendingTasks=d(ca);contributeToStability=d(jR);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let r=Array.from(new Set([...this.injector.get(BR),...this.injector.get(pD,[])]));this.chain=r.reduceRight((o,s)=>VR(o,s,this.injector),LR)}let i=this.chain;if(this.contributeToStability){let r=this.pendingTasks.add();return B(()=>i(e,o=>this.backend.handle(o))).pipe(mr(r))}else return B(()=>i(e,r=>this.backend.handle(r)))}static \u0275fac=function(i){return new(i||t)(L(UR),L(we))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),zR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=L(HR),r},providedIn:"root"})}return t})();function zp(t,n){return p({body:n},t)}var La=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof Vo)o=e;else{let l;r.headers instanceof gi?l=r.headers:l=new gi(r.headers);let c;r.params&&(r.params instanceof pi?c=r.params:c=new pi({fromObject:r.params})),o=new Vo(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=H(o).pipe(mo(l=>this.handler.handle(l)));if(e instanceof Vo||r.observe==="events")return s;let a=s.pipe(_e(l=>l instanceof Fa));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ie(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new y(2806,!1);return l.body}));case"blob":return a.pipe(ie(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new y(2807,!1);return l.body}));case"text":return a.pipe(ie(l=>{if(l.body!==null&&typeof l.body!="string")throw new y(2808,!1);return l.body}));default:return a.pipe(ie(l=>l.body))}case"response":return a;default:throw new y(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new pi().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,zp(r,i))}post(e,i,r={}){return this.request("POST",e,zp(r,i))}put(e,i,r={}){return this.request("PUT",e,zp(r,i))}static \u0275fac=function(i){return new(i||t)(L(zR))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var gD=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(L(R))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Va=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=L(WR),r},providedIn:"root"})}return t})(),WR=(()=>{class t extends Va{_doc=d(R);sanitize(e,i){if(i==null)return null;switch(e){case nt.NONE:return i;case nt.HTML:return Lr(i,"HTML")?li(i):Xm(this._doc,String(i)).toString();case nt.STYLE:return Lr(i,"Style")?li(i):i;case nt.SCRIPT:if(Lr(i,"Script"))return li(i);throw new y(5200,!1);case nt.URL:return Lr(i,"URL")?li(i):Cd(String(i));case nt.RESOURCE_URL:if(Lr(i,"ResourceURL"))return li(i);throw new y(-5201,!1);default:throw new y(5202,!1)}}bypassSecurityTrustHtml(e){return Gm(e)}bypassSecurityTrustStyle(e){return qm(e)}bypassSecurityTrustScript(e){return Ym(e)}bypassSecurityTrustUrl(e){return Zm(e)}bypassSecurityTrustResourceUrl(e){return Km(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var de="primary",Ka=Symbol("RouteTitle"),Jp=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function zo(t){return new Jp(t)}function Zp(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function GR(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return Zp(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!Zp(o,t.slice(0,o.length),a)||!Zp(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function nu(t){return new Promise((n,e)=>{t.pipe(Jn()).subscribe({next:i=>n(i),error:i=>e(i)})})}function qR(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!$n(t[e],n[e]))return!1;return!0}function $n(t,n){let e=t?eg(t):void 0,i=n?eg(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!xD(t[r],n[r]))return!1;return!0}function eg(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function xD(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function YR(t){return t.length>0?t[t.length-1]:null}function $r(t){return Ls(t)?t:ui(t)?Oe(Promise.resolve(t)):H(t)}function ID(t){return Ls(t)?nu(t):Promise.resolve(t)}var ZR={exact:ND,subset:kD},TD={exact:KR,subset:XR,ignored:()=>!0},MD={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},tg={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function _D(t,n,e){return ZR[e.paths](t.root,n.root,e.matrixParams)&&TD[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function KR(t,n){return $n(t,n)}function ND(t,n,e){if(!Ur(t.segments,n.segments)||!Jd(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!ND(t.children[i],n.children[i],e))return!1;return!0}function XR(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>xD(t[e],n[e]))}function kD(t,n,e){return AD(t,n,n.segments,e)}function AD(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Ur(r,e)||n.hasChildren()||!Jd(r,e,i))}else if(t.segments.length===e.length){if(!Ur(t.segments,e)||!Jd(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!kD(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Ur(t.segments,r)||!Jd(t.segments,r,i)||!t.children[de]?!1:AD(t.children[de],n,o,i)}}function Jd(t,n,e){return n.every((i,r)=>TD[e](t[r].parameters,i.parameters))}var Dn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Se([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=zo(this.queryParams),this._queryParamMap}toString(){return eO.serialize(this)}},Se=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return eu(this)}},jr=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=zo(this.parameters),this._parameterMap}toString(){return OD(this)}};function QR(t,n){return Ur(t,n)&&t.every((e,i)=>$n(e.parameters,n[i].parameters))}function Ur(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function JR(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===de&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==de&&(e=e.concat(n(r,i)))}),e}var fu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>new Hr})}return t})(),Hr=class{parse(n){let e=new ig(n);return new Dn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Ba(n.root,!0)}`,i=iO(n.queryParams),r=typeof n.fragment=="string"?`#${tO(n.fragment)}`:"";return`${e}${i}${r}`}},eO=new Hr;function eu(t){return t.segments.map(n=>OD(n)).join("/")}function Ba(t,n){if(!t.hasChildren())return eu(t);if(n){let e=t.children[de]?Ba(t.children[de],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==de&&i.push(`${r}:${Ba(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=JR(t,(i,r)=>r===de?[Ba(t.children[de],!1)]:[`${r}:${Ba(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[de]!=null?`${eu(t)}/${e[0]}`:`${eu(t)}/(${e.join("//")})`}}function RD(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Xd(t){return RD(t).replace(/%3B/gi,";")}function tO(t){return encodeURI(t)}function ng(t){return RD(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function tu(t){return decodeURIComponent(t)}function bD(t){return tu(t.replace(/\+/g,"%20"))}function OD(t){return`${ng(t.path)}${nO(t.parameters)}`}function nO(t){return Object.entries(t).map(([n,e])=>`;${ng(n)}=${ng(e)}`).join("")}function iO(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${Xd(e)}=${Xd(r)}`).join("&"):`${Xd(e)}=${Xd(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var rO=/^[^\/()?;#]+/;function Kp(t){let n=t.match(rO);return n?n[0]:""}var oO=/^[^\/()?;=#]+/;function sO(t){let n=t.match(oO);return n?n[0]:""}var aO=/^[^=?&#]+/;function lO(t){let n=t.match(aO);return n?n[0]:""}var cO=/^[^&#]+/;function dO(t){let n=t.match(cO);return n?n[0]:""}var ig=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Se([],{}):new Se([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new y(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[de]=new Se(e,i)),r}parseSegment(){let n=Kp(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new y(4009,!1);return this.capture(n),new jr(tu(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=sO(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=Kp(this.remaining);r&&(i=r,this.capture(i))}n[tu(e)]=tu(i)}parseQueryParam(n){let e=lO(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=dO(this.remaining);s&&(i=s,this.capture(i))}let r=bD(e),o=bD(i);if(Object.hasOwn(n,r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i=Object.create(null);for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Kp(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new y(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=de);let a=this.parseChildren(e+1);i[s??de]=Object.keys(a).length===1&&a[de]?a[de]:new Se([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new y(4011,!1)}};function PD(t){return t.segments.length>0?new Se([],{[de]:t}):t}function FD(t){let n=Object.create(null);for(let[i,r]of Object.entries(t.children)){let o=FD(r);if(i===de&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Se(t.segments,n);return uO(e)}function uO(t){if(t.numberOfChildren===1&&t.children[de]){let n=t.children[de];return new Se(t.segments.concat(n.segments),n.children)}return t}function $o(t){return t instanceof Dn}function fO(t,n,e=null,i=null,r=new Hr){let o=LD(t);return VD(o,n,e,i,r)}function LD(t){let n;function e(o){let s={};for(let l of o.children){let c=e(l);s[l.outlet]=c}let a=new Se(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=PD(i);return n??r}function VD(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return Xp(o,o,o,e,i,r);let s=hO(n);if(s.toRoot())return Xp(o,o,new Se([],{}),e,i,r);let a=mO(s,o,t),l=a.processChildren?Ua(a.segmentGroup,a.index,s.commands):jD(a.segmentGroup,a.index,s.commands);return Xp(o,a.segmentGroup,l,e,i,r)}function iu(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function $a(t){return typeof t=="object"&&t!=null&&t.outlets}function yD(t,n,e){t||="\u0275";let i=new Dn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function Xp(t,n,e,i,r,o){let s={};for(let[c,u]of Object.entries(i??{}))s[c]=Array.isArray(u)?u.map(f=>yD(c,f,o)):yD(c,u,o);let a;t===n?a=e:a=BD(t,n,e);let l=PD(FD(a));return new Dn(l,s,r)}function BD(t,n,e){let i=Object.create(null);return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=BD(o,n,e)}),new Se(t.segments,i)}var ru=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&iu(i[0]))throw new y(4003,!1);let r=i.find($a);if(r&&r!==YR(i))throw new y(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function hO(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new ru(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([l,c])=>{a[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,l)=>{l==0&&a==="."||(l==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new ru(e,n,i)}var Uo=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function mO(t,n,e){if(t.isAbsolute)return new Uo(n,!0,0);if(!e)return new Uo(n,!1,NaN);if(e.parent===null)return new Uo(e,!0,0);let i=iu(t.commands[0])?0:1,r=e.segments.length-1+i;return pO(e,r,t.numberOfDoubleDots)}function pO(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new y(4005,!1);r=i.segments.length}return new Uo(i,!1,r-o)}function gO(t){return $a(t[0])?t[0].outlets:{[de]:t}}function jD(t,n,e){if(t??=new Se([],{}),t.segments.length===0&&t.hasChildren())return Ua(t,n,e);let i=vO(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Se(t.segments.slice(0,i.pathIndex),{});return o.children[de]=new Se(t.segments.slice(i.pathIndex),t.children),Ua(o,0,r)}else return i.match&&r.length===0?new Se(t.segments,{}):i.match&&!t.hasChildren()?rg(t,n,e):i.match?Ua(t,0,r):rg(t,n,e)}function Ua(t,n,e){if(e.length===0)return new Se(t.segments,{});{let i=gO(e),r=Object.create(null);if(Object.keys(i).some(o=>o!==de)&&t.children[de]&&t.numberOfChildren===1&&t.children[de].segments.length===0){let o=Ua(t.children[de],n,e);return new Se(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=jD(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new Se(t.segments,r)}}function vO(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if($a(a))break;let l=`${a}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!SD(l,c,s))return o;i+=2}else{if(!SD(l,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function rg(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if($a(o)){let l=_O(o.outlets);return new Se(i,l)}if(r===0&&iu(e[0])){let l=t.segments[n];i.push(new jr(l.path,CD(e[0]))),r++;continue}let s=$a(o)?o.outlets[de]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&iu(a)?(i.push(new jr(s,CD(a))),r+=2):(i.push(new jr(s,{})),r++)}return new Se(i,{})}function _O(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=rg(new Se([],{}),0,i))}),n}function CD(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function SD(t,n,e){return t==e.path&&$n(n,e.parameters)}var Ha="imperative",mt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(mt||{}),an=class{id;url;constructor(n,e){this.id=n,this.url=e}},Wo=class extends an{type=mt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Bi=class extends an{urlAfterRedirects;type=mt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},kt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(kt||{}),ou=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(ou||{}),Cn=class extends an{reason;code;type=mt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function UD(t){return t instanceof Cn&&(t.code===kt.Redirect||t.code===kt.SupersededByNewNavigation)}var ji=class extends an{reason;code;type=mt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},Go=class extends an{error;target;type=mt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},su=class extends an{urlAfterRedirects;state;type=mt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},og=class extends an{urlAfterRedirects;state;type=mt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},sg=class extends an{urlAfterRedirects;state;shouldActivate;type=mt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},ag=class extends an{urlAfterRedirects;state;type=mt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},lg=class extends an{urlAfterRedirects;state;type=mt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},cg=class{route;type=mt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},dg=class{route;type=mt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},ug=class{snapshot;type=mt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},fg=class{snapshot;type=mt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},hg=class{snapshot;type=mt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},mg=class{snapshot;type=mt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var qo=class{},Wa=class{},Yo=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function bO(t){return!(t instanceof qo)&&!(t instanceof Yo)&&!(t instanceof Wa)}var pg=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Xa(this.rootInjector)}},Xa=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new pg(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(L(we))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),au=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=gg(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=gg(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=vg(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return vg(n,this._root).map(e=>e.value)}};function gg(t,n){if(t===n.value)return n;for(let e of n.children){let i=gg(t,e);if(i)return i}return null}function vg(t,n){if(t===n.value)return[n];for(let e of n.children){let i=vg(t,e);if(i.length)return i.unshift(n),i}return[]}var $t=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function jo(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var lu=class extends au{snapshot;constructor(n,e){super(n),this.snapshot=e,Ig(this,n)}toString(){return this.snapshot.toString()}};function HD(t,n){let e=yO(t,n),i=new Xe([new jr("",{})]),r=new Xe({}),o=new Xe({}),s=new Xe({}),a=new Xe(""),l=new zr(i,r,s,a,o,de,t,e.root);return l.snapshot=e.root,new lu(new $t(l,[]),e)}function yO(t,n){let e={},i={},r={},s=new Ga([],e,r,"",i,de,t,null,{},n);return new cu("",new $t(s,[]))}var zr=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;_localInjector;constructor(n,e,i,r,o,s,a,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(ie(c=>c[Ka]))??H(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(ie(n=>zo(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(ie(n=>zo(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}},CO="always";function xg(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:p(p({},n.params),t.params),data:p(p({},n.data),t.data),resolve:p(p(p(p({},t.data),n.data),r?.data),t._resolvedData)}:i={params:p({},t.params),data:p({},t.data),resolve:p(p({},t.data),t._resolvedData??{})},r&&$D(r)&&(i.resolve[Ka]=r.title),i}var Ga=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Ka]}constructor(n,e,i,r,o,s,a,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=zo(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=zo(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},cu=class extends au{url;constructor(n,e){super(e),this.url=n,Ig(this,e)}toString(){return zD(this._root)}};function Ig(t,n){n.value._routerState=t,n.children.forEach(e=>Ig(t,e))}function zD(t){let n=t.children.length>0?` { ${t.children.map(zD).join(", ")} } `:"";return`${t.value}${n}`}function Qp(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,$n(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),$n(n.params,e.params)||t.paramsSubject.next(e.params),qR(n.url,e.url)||t.urlSubject.next(e.url),$n(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function _g(t,n){let e=$n(t.params,n.params)&&QR(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||_g(t.parent,n.parent))}function $D(t){return typeof t.title=="string"||t.title===null}var SO=new g(""),WD=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=de;activateEvents=new U;deactivateEvents=new U;attachEvents=new U;detachEvents=new U;routerOutletData=ht();parentContexts=d(Xa);location=d(Mt);changeDetector=d(Fe);inputBinder=d(hu,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new y(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new y(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new y(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new y(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,l=new bg(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Ge]})}return t})(),bg=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===zr?this.route:n===Xa?this.childContexts:n===SO?this.outletData:this.parent.get(n,e)}},hu=new g("");var GD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&O(0,"router-outlet")},dependencies:[WD],encapsulation:2,changeDetection:1})}return t})();function Tg(t){let n=t.children&&t.children.map(Tg),e=n?N(p({},t),{children:n}):p({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==de&&(e.component=GD),e}function DO(t,n,e){let i=new Set,r=qa(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new lu(r,n)}}function qa(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._futureSnapshot=n.value;let o=wO(t,n,e,i);return new $t(r,o)}else{if(t.shouldAttach(n.value)){let s=t.retrieve(n.value);if(s!==null){let a=s.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(l=>qa(t,l,void 0,i)),a}}let r=EO(n.value);i.add(r);let o=n.children.map(s=>qa(t,s,void 0,i));return new $t(r,o)}}function wO(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return qa(t,r,o,i);return qa(t,r,void 0,i)})}function EO(t){return new zr(new Xe(t.url),new Xe(t.params),new Xe(t.queryParams),new Xe(t.fragment),new Xe(t.data),t.outlet,t.component,t)}var Ya=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},qD="ngNavigationCancelingError";function du(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=$o(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=YD(!1,kt.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function YD(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[qD]=!0,e.cancellationCode=n,e}function xO(t){return ZD(t)&&$o(t.url)}function ZD(t){return!!t&&t[qD]}var yg=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),Qp(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=jo(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=jo(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=jo(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=jo(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new mg(o.value.snapshot))}),n.children.length&&this.forwardEvent(new fg(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(Qp(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Qp(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},uu=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},Ho=class{component;route;constructor(n,e){this.component=n,this.route=e}};function IO(t,n,e){let i=t._root,r=n?n._root:null;return ja(i,r,e,[i.value])}function TO(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Ko(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!ch(t)?t:n.get(t):i}function ja(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=jo(n);return t.children.forEach(s=>{MO(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>za(a,e.getContext(s),r)),r}function MO(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let l=NO(s,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new uu(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?ja(t,n,a?a.children:null,i,r):ja(t,n,e,i,r),l&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new Ho(a.outlet.component,s))}else s&&za(n,a,r),r.canActivateChecks.push(new uu(i)),o.component?ja(t,null,a?a.children:null,i,r):ja(t,null,e,i,r);return r}function NO(t,n,e){if(typeof e=="function")return Le(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Ur(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Ur(t.url,n.url)||!$n(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!_g(t,n)||!$n(t.queryParams,n.queryParams);default:return!_g(t,n)}}function za(t,n,e){let i=jo(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?za(s,n.children.getContext(o),e):za(s,null,e):za(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new Ho(n.outlet.component,r)):e.canDeactivateChecks.push(new Ho(null,r)):e.canDeactivateChecks.push(new Ho(null,r))}function Qa(t){return typeof t=="function"}function kO(t){return typeof t=="boolean"}function AO(t){return t&&Qa(t.canLoad)}function RO(t){return t&&Qa(t.canActivate)}function OO(t){return t&&Qa(t.canActivateChild)}function PO(t){return t&&Qa(t.canDeactivate)}function FO(t){return t&&Qa(t.canMatch)}function KD(t){return t instanceof fr||t?.name==="EmptyError"}var Qd=Symbol("INITIAL_VALUE");function Zo(){return ct(t=>Vs(t.map(n=>n.pipe(ze(1),It(Qd)))).pipe(ie(n=>{for(let e of n)if(e!==!0){if(e===Qd)return Qd;if(e===!1||LO(e))return e}return!0}),_e(n=>n!==Qd),ze(1)))}function LO(t){return $o(t)||t instanceof Ya}function XD(t){return t.aborted?H(void 0).pipe(ze(1)):new J(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function QD(t){return be(XD(t))}function VO(t){return xt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?H(N(p({},n),{guardsResult:!0})):BO(o,e,i).pipe(xt(s=>s&&kO(s)?jO(e,r,t):H(s)),ie(s=>N(p({},n),{guardsResult:s})))})}function BO(t,n,e){return Oe(t).pipe(xt(i=>WO(i.component,i.route,e,n)),Jn(i=>i!==!0,!0))}function jO(t,n,e){return Oe(n).pipe(mo(i=>xi(HO(i.route.parent,e),UO(i.route,e),$O(t,i.path),zO(t,i.route))),Jn(i=>i!==!0,!0))}function UO(t,n){return t!==null&&n&&n(new hg(t)),H(!0)}function HO(t,n){return t!==null&&n&&n(new ug(t)),H(!0)}function zO(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return H(!0);let i=e.map(r=>un(()=>{let o=n._environmentInjector,s=Ko(r,o),a=RO(s)?s.canActivate(n,t):Le(o,()=>s(n,t));return $r(a).pipe(Jn())}));return H(i).pipe(Zo())}function $O(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>TO(o)).filter(o=>o!==null).map(o=>un(()=>{let s=o.guards.map(a=>{let l=o.node._environmentInjector,c=Ko(a,l),u=OO(c)?c.canActivateChild(e,t):Le(l,()=>c(e,t));return $r(u).pipe(Jn())});return H(s).pipe(Zo())}));return H(r).pipe(Zo())}function WO(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return H(!0);let o=r.map(s=>{let a=n._environmentInjector,l=Ko(s,a),c=PO(l)?l.canDeactivate(t,n,e,i):Le(a,()=>l(t,n,e,i));return $r(c).pipe(Jn())});return H(o).pipe(Zo())}function GO(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return H(!0);let s=o.map(a=>{let l=Ko(a,t),c=AO(l)?l.canLoad(n,e):Le(t,()=>l(n,e)),u=$r(c);return r?u.pipe(QD(r)):u});return H(s).pipe(Zo(),JD(i))}function JD(t){return zf(tt(n=>{if(typeof n!="boolean")throw du(t,n)}),ie(n=>n===!0))}function qO(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return H(!0);let a=s.map(l=>{let c=Ko(l,t),u=FO(c)?c.canMatch(n,e,r):Le(t,()=>c(n,e,r));return $r(u).pipe(QD(o))});return H(a).pipe(Zo(),JD(i))}var vi=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},Za=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function YO(t){throw new y(4e3,!1)}function ZO(t){throw YD(!1,kt.GuardRejected)}var Cg=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[de])throw YO(`${n.redirectTo}`);r=r.children[de]}}async applyRedirectCommands(n,e,i,r,o){let s=await KO(e,r,o);if(s instanceof Dn)throw new Za(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new Za(a);return a}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new Dn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s=Object.create(null);return Object.entries(e.children).forEach(([a,l])=>{s[a]=this.createSegmentGroup(n,l,i,r)}),new Se(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new y(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function KO(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return nu($r(Le(e,()=>i(n))))}function XO(t,n){return t.providers&&!t._injector&&(t._injector=wa(t.providers,n,`Route: ${t.path}`)),t._injector??n}function Sn(t){return t.outlet||de}function QO(t,n){let e=t.filter(i=>Sn(i)===n);return e.push(...t.filter(i=>Sn(i)!==n)),e}var Sg={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function ew(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function JO(t,n,e,i,r,o,s){let a=tw(t,n,e);if(!a.matched)return H(a);let l=ew(o(a));return i=XO(n,i),qO(i,n,e,r,l,s).pipe(ie(c=>c===!0?a:p({},Sg)))}function tw(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?p({},Sg):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||GR)(e,t,n);if(!r)return p({},Sg);let o={};Object.entries(r.posParams??{}).forEach(([a,l])=>{o[a]=l.path});let s=r.consumed.length>0?p(p({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function DD(t,n,e,i,r){return e.length>0&&nP(t,e,i,r)?{segmentGroup:new Se(n,tP(i,new Se(e,t.children))),slicedSegments:[]}:e.length===0&&iP(t,e,i)?{segmentGroup:new Se(t.segments,eP(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Se(t.segments,t.children),slicedSegments:e}}function eP(t,n,e,i){let r={};for(let o of e)if(mu(t,n,o)&&!i[Sn(o)]){let s=new Se([],{});r[Sn(o)]=s}return p(p({},i),r)}function tP(t,n){let e={};e[de]=n;for(let i of t)if(i.path===""&&Sn(i)!==de){let r=new Se([],{});e[Sn(i)]=r}return e}function nP(t,n,e,i){return e.some(r=>!mu(t,n,r)||!(Sn(r)!==de)?!1:!(i!==void 0&&Sn(r)===i))}function iP(t,n,e){return e.some(i=>mu(t,n,i))}function mu(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function rP(t,n,e){return n.length===0&&!t.children[e]}var Dg=class{};async function oP(t,n,e,i,r,o,s,a){return new wg(t,n,e,i,r,s,o,a).recognize()}var sP=31,wg=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=l,this.applyRedirects=new Cg(this.urlSerializer,this.urlTree)}noMatchError(n){return new y(4002,`'${n.segmentGroup}'`)}async recognize(){let n=DD(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new $t(i,e),o=new cu("",r),s=fO(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let e=new Ga([],Object.freeze({}),Object.freeze(p({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),de,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,de,e),rootSnapshot:e}}catch(i){if(i instanceof Za)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof vi?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=await this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof $t?[s]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let s=[];for(let l of o){let c=i.children[l],u=QO(e,l),f=await this.processSegmentGroup(n,u,c,l,r);s.push(...f)}let a=nw(s);return aP(a),a}async processSegment(n,e,i,r,o,s,a){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,s,a)}catch(c){if(c instanceof vi||KD(c))continue;throw c}if(rP(i,r,o))return new Dg;throw new vi(i)}async processSegmentAgainstRoute(n,e,i,r,o,s,a,l){if(Sn(i)!==s&&(s===de||!mu(r,o,i)))throw new vi(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,l);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,l);throw new vi(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:f,remainingSegments:h}=tw(e,r,o);if(!l)throw new vi(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>sP&&(this.allowRedirects=!1));let m=this.createSnapshot(n,r,o,c,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let v=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,f,ew(m),n),D=await this.applyRedirects.lineralizeSegments(r,v);return this.processSegment(n,i,e,D.concat(h),s,!1,a)}createSnapshot(n,e,i,r,o){let s=new Ga(i,r,Object.freeze(p({},this.urlTree.queryParams)),this.urlTree.fragment,cP(e),Sn(e),e.component??e._loadedComponent??null,e,dP(e),n),a=xg(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,e,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Ye=>this.createSnapshot(n,i,Ye.consumedSegments,Ye.parameters,s),l=await nu(JO(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new vi(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:f,consumedSegments:h,remainingSegments:m}=l,v=this.createSnapshot(n,i,h,f,s),{segmentGroup:D,slicedSegments:A}=DD(e,h,m,c,o);if(A.length===0&&D.hasChildren()){let Ye=await this.processChildren(u,c,D,v);return new $t(v,Ye)}if(c.length===0&&A.length===0)return new $t(v,[]);let ne=Sn(i)===o,ke=await this.processSegment(u,c,D,A,ne?de:o,!0,v);return new $t(v,ke instanceof $t?[ke]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await nu(GO(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw ZO(e)}return{routes:[],injector:n}}};function aP(t){t.sort((n,e)=>n.value.outlet===de?-1:e.value.outlet===de?1:n.value.outlet.localeCompare(e.value.outlet))}function lP(t){let n=t.value.routeConfig;return n&&n.path===""}function nw(t){let n=[],e=new Set;for(let i of t){if(!lP(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=nw(i.children);n.push(new $t(i.value,r))}return n.filter(i=>!e.has(i))}function cP(t){return t.data||{}}function dP(t){return t.resolve||{}}function uP(t,n,e,i,r,o,s){return xt(async a=>{let{state:l,tree:c}=await oP(t,n,e,i,a.extractedUrl,r,o,s);return N(p({},a),{targetSnapshot:l,urlAfterRedirects:c})})}function fP(t){return xt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return H(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let l of iw(a))o.add(l);let s=0;return Oe(o).pipe(mo(a=>r.has(a)?hP(a,e,t):(a.data=xg(a,a.parent,t).resolve,H(void 0))),tt(()=>s++),_c(1),xt(a=>s===o.size?H(n):He))})}function iw(t){let n=t.children.map(e=>iw(e)).flat();return[t,...n]}function hP(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!$D(i)&&(r[Ka]=i.title),un(()=>(t.data=xg(t,t.parent,e).resolve,mP(r,t,n).pipe(ie(o=>(t._resolvedData=o,t.data=p(p({},t.data),o),null)))))}function mP(t,n,e){let i=eg(t);if(i.length===0)return H({});let r={};return Oe(i).pipe(xt(o=>pP(t[o],n,e).pipe(Jn(),tt(s=>{if(s instanceof Ya)throw du(new Hr,s);r[o]=s}))),_c(1),ie(()=>r),hr(o=>KD(o)?He:Fs(o)))}function pP(t,n,e){let i=n._environmentInjector,r=Ko(t,i),o=r.resolve?r.resolve(n,e):Le(i,()=>r(n,e));return $r(o)}function wD(t){return ct(n=>{let e=t(n);return e?Oe(e).pipe(ie(()=>n)):H(n)})}var rw=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===de);return i}getResolvedTitleForRoute(e){return e.data[Ka]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(gP)})}return t})(),gP=(()=>{class t extends rw{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(L(gD))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),pu=new g("",{factory:()=>({})}),gu=new g(""),ow=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(Ep);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await ID(Le(e,()=>i.loadComponent())),s=await sw(Np(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await vP(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();async function vP(t,n,e,i){let r=await ID(Le(e,()=>t.loadChildren())),o=await sw(Np(r)),s;o instanceof Nd||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(t);let a,l,c=!1,u;return Array.isArray(s)?(l=s,c=!0):(a=s.create(e).injector,u=s,l=a.get(gu,[],{optional:!0,self:!0}).flat()),{routes:l.map(Tg),injector:a,factory:u}}async function sw(t){return t}var Mg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(_P)})}return t})(),_P=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),aw=new g("");var lw=new g(""),bP=()=>{},cw=new g(""),dw=(()=>{class t{currentNavigation=z(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=z(null);events=new S;transitionAbortWithErrorSubject=new S;configLoader=d(ow);environmentInjector=d(we);destroyRef=d(Ue);urlSerializer=d(fu);rootContexts=d(Xa);location=d(Vi);inputBindingEnabled=d(hu,{optional:!0})!==null;titleStrategy=d(rw);options=d(pu,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||CO;urlHandlingStrategy=d(Mg);createViewTransition=d(aw,{optional:!0});navigationErrorHandler=d(cw,{optional:!0});activatedRouteInjectorFeature=d(lw,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>H(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new cg(r)),i=r=>this.events.next(new dg(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;B(()=>{this.transitions?.next(N(p({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Xe(null),this.transitions.pipe(_e(i=>i!==null),ct(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return H(i).pipe(ct(l=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",kt.SupersededByNewNavigation),He;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:l.id,initialUrl:l.rawUrl,extractedUrl:l.extractedUrl,targetBrowserUrl:typeof l.extras.browserUrl=="string"?this.urlSerializer.parse(l.extras.browserUrl):l.extras.browserUrl,trigger:l.source,extras:l.extras,previousNavigation:c?N(p({},c),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:l.routesRecognizeHandler,beforeActivateHandler:l.beforeActivateHandler});let u=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=l.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!u&&f!=="reload")return this.events.next(new ji(l.id,this.urlSerializer.serialize(l.rawUrl),"",ou.IgnoredSameUrlNavigation)),l.resolve(!1),He;if(this.urlHandlingStrategy.shouldProcessUrl(l.rawUrl))return H(l).pipe(ct(h=>(this.events.next(new Wo(h.id,this.urlSerializer.serialize(h.extractedUrl),h.source,h.restoredState)),h.id!==this.navigationId?He:Promise.resolve(h))),uP(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),tt(h=>{i.targetSnapshot=h.targetSnapshot,i.urlAfterRedirects=h.urlAfterRedirects,this.currentNavigation.update(m=>(m.finalUrl=h.urlAfterRedirects,m)),this.events.next(new Wa)}),ct(h=>Oe(i.routesRecognizeHandler.deferredHandle??H(void 0)).pipe(ie(()=>h))),tt(()=>{let h=new su(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(h)}));if(u&&this.urlHandlingStrategy.shouldProcessUrl(l.currentRawUrl)){let{id:h,extractedUrl:m,source:v,restoredState:D,extras:A}=l,ne=new Wo(h,this.urlSerializer.serialize(m),v,D);this.events.next(ne);let ke=HD(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=N(p({},l),{targetSnapshot:ke,urlAfterRedirects:m,extras:N(p({},A),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Ye=>(Ye.finalUrl=m,Ye)),H(i)}else return this.events.next(new ji(l.id,this.urlSerializer.serialize(l.extractedUrl),"",ou.IgnoredByUrlHandlingStrategy)),l.resolve(!1),He}),ie(l=>{let c=new og(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);return this.events.next(c),this.currentTransition=i=N(p({},l),{guards:IO(l.targetSnapshot,l.currentSnapshot,this.rootContexts)}),i}),VO(l=>this.events.next(l)),ct(l=>{if(i.guardsResult=l.guardsResult,l.guardsResult&&typeof l.guardsResult!="boolean")throw du(this.urlSerializer,l.guardsResult);let c=new sg(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot,!!l.guardsResult);if(this.events.next(c),!a())return He;if(!l.guardsResult)return this.cancelNavigationTransition(l,"",kt.GuardRejected),He;if(l.guards.canActivateChecks.length===0)return H(l);let u=new ag(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);if(this.events.next(u),!a())return He;let f=!1;return H(l).pipe(fP(this.paramsInheritanceStrategy),tt({next:()=>{f=!0;let h=new lg(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(h)},complete:()=>{f||this.cancelNavigationTransition(l,"",kt.NoDataFromResolver)}}))}),wD(l=>{let c=f=>{let h=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let m=f._environmentInjector;h.push(this.configLoader.loadComponent(m,f.routeConfig).then(v=>{f.component=v}))}for(let m of f.children)h.push(...c(m));return h},u=c(l.targetSnapshot.root);return u.length===0?H(l):Oe(Promise.all(u).then(()=>l))}),ct(l=>{let{newlyCreatedRoutes:c,state:u}=DO(e.routeReuseStrategy,l.targetSnapshot,l.currentRouterState);return this.currentTransition=i=l=N(p({},l),{targetRouterState:u,newlyCreatedRoutes:c}),this.currentNavigation.update(f=>(f.targetRouterState=u,f)),H(l)}),this.activatedRouteInjectorFeature?.operator()??(l=>l),wD(()=>this.afterPreactivation()),ct(()=>{let{currentSnapshot:l,targetSnapshot:c}=i,u=this.createViewTransition?.(this.environmentInjector,l.root,c.root);return u?Oe(u).pipe(ie(()=>i)):H(i)}),ze(1),ct(l=>{r=!1,this.events.next(new qo);let c=i.beforeActivateHandler.deferredHandle;return c?Oe(c.then(()=>l)):H(l)}),tt(l=>{new yg(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),l.newlyCreatedRoutes?.clear(),a()&&(o=!0,this.currentNavigation.update(c=>(c.abort=bP,c)),this.lastSuccessfulNavigation.set(B(this.currentNavigation)),this.events.next(new Bi(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects))),this.titleStrategy?.updateTitle(l.targetRouterState.snapshot),l.resolve(!0))}),be(XD(s.signal).pipe(_e(()=>!o&&r),tt(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",kt.Aborted)}))),tt({complete:()=>{o=!0}}),be(this.transitionAbortWithErrorSubject.pipe(tt(l=>{throw l}))),mr(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",kt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),hr(l=>{if(o=!0,ED(i),this.destroyed)return i.resolve(!1),He;if(ZD(l))this.events.next(new Cn(i.id,this.urlSerializer.serialize(i.extractedUrl),l.message,l.cancellationCode)),xO(l)?this.events.next(new Yo(l.url,l.navigationBehaviorOptions)):i.resolve(!1);else{let c=new Go(i.id,this.urlSerializer.serialize(i.extractedUrl),l,i.targetSnapshot??void 0);try{let u=Le(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(u instanceof Ya){let{message:f,cancellationCode:h}=du(this.urlSerializer,u);this.events.next(new Cn(i.id,this.urlSerializer.serialize(i.extractedUrl),f,h)),this.events.next(new Yo(u.redirectTo,u.navigationBehaviorOptions))}else throw this.events.next(c),l}catch(u){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(u)}}return He}))}))}cancelNavigationTransition(e,i,r){ED(e);let o=new Cn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=B(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function yP(t){return t!==Ha}function ED(t){if(t.newlyCreatedRoutes)for(let n of t.newlyCreatedRoutes)n._localInjector?.destroy()}var uw=new g("");var CP=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(SP)})}return t})(),Eg=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},SP=(()=>{class t extends Eg{static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Ng=(()=>{class t{urlSerializer=d(fu);options=d(pu,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(Vi);urlHandlingStrategy=d(Mg);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Dn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof Dn?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=HD(null,d(we));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:()=>d(DP)})}return t})(),DP=(()=>{class t extends Ng{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof Wo?this.updateStateMemento():e instanceof ji?this.commitTransition(i):e instanceof su?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof qo?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Cn&&!UD(e)?this.restoreHistory(i):e instanceof Go?this.restoreHistory(i,!0):e instanceof Bi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let l=this.browserPageId,c=p(p({},a),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=p(p({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?p({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):p({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function fw(t,n){t.events.pipe(_e(e=>e instanceof Bi||e instanceof Cn||e instanceof Go||e instanceof ji),ie(e=>e instanceof Bi||e instanceof ji?0:(e instanceof Cn?e.code===kt.Redirect||e.code===kt.SupersededByNewNavigation:!1)?2:1),_e(e=>e!==2),ze(1)).subscribe(()=>{n()})}var kg=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(Ad);stateManager=d(Ng);options=d(pu,{optional:!0})||{};pendingTasks=d(oi);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(dw);urlSerializer=d(fu);location=d(Vi);urlHandlingStrategy=d(Mg);injector=d(we);_events=new S;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(CP);injectorCleanup=d(uw,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(gu,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(hu,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new pe;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=B(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof Cn&&i.code!==kt.Redirect&&i.code!==kt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Bi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Yo){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=p({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||yP(r.source)},s);this.scheduleNavigation(a,Ha,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}bO(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Ha,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=N(p({},o),{browserUrl:e})),r){let c=p({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(a);this.scheduleNavigation(l,i,s,o).catch(c=>{this.disposed||this.injector.get(Qt)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return B(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Tg),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=p(p({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let f;try{let h=r?r.snapshot:this.routerState.snapshot.root;f=LD(h)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return VD(f,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=$o(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Ha,null,i)}navigate(e,i={skipLocationChange:!1}){return wP(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(An(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=p({},MD):i===!1?r=p({},tg):r=p(p({},tg),i),$o(e))return _D(this.currentUrlTree,e,r);let o=this.parseUrl(e);return _D(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,l,c;s?(a=s.resolve,l=s.reject,c=s.promise):c=new Promise((f,h)=>{a=f,l=h});let u=this.pendingTasks.add();return fw(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function wP(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new y(4008,!1)}var EP=new g("");function Ag(t,...n){return Rn([{provide:gu,multi:!0,useValue:t},{provide:zr,useFactory:xP},{provide:Ea,multi:!0,useFactory:IP},n.map(e=>e.\u0275providers)])}function xP(){return d(kg).routerState.root}function IP(){let t=d(P);return n=>{let e=t.get(Ht);if(n!==e.components[0])return;let i=t.get(kg),r=t.get(TP);t.get(MP)===1&&i.initialNavigation(),t.get(NP,null,{optional:!0})?.setUpPreloading(),t.get(EP,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var TP=new g("",{factory:()=>new S}),MP=new g("",{factory:()=>1});var NP=new g("");var hw=[];var mw={providers:[Yh(),Ag(hw)]};var Cw=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ot(De),ot(k))};static \u0275dir=M({type:t})}return t})(),AP=(()=>{class t extends Cw{static \u0275fac=(()=>{let e;return function(r){return(e||(e=bt(t)))(r||t)}})();static \u0275dir=M({type:t,features:[Ne]})}return t})(),Wr=new g("");var RP={provide:Wr,useExisting:Ot(()=>Sw),multi:!0};function OP(){let t=sn()?sn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var PP=new g(""),Sw=(()=>{class t extends Cw{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!OP())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ot(De),ot(k),ot(PP,8))};static \u0275dir=M({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&G("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Ae([RP]),Ne]})}return t})();function Pg(t){return t==null||Fg(t)===0}function Fg(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Gr=new g(""),Dw=new g(""),FP=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Wn=class{static min(n){return LP(n)}static max(n){return VP(n)}static required(n){return ww(n)}static requiredTrue(n){return BP(n)}static email(n){return jP(n)}static minLength(n){return UP(n)}static maxLength(n){return HP(n)}static pattern(n){return zP(n)}static nullValidator(n){return _u()}static compose(n){return Nw(n)}static composeAsync(n){return kw(n)}};function LP(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function VP(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function ww(t){return Pg(t.value)?{required:!0}:null}function BP(t){return t.value===!0?null:{required:!0}}function jP(t){return Pg(t.value)||FP.test(t.value)?null:{email:!0}}function UP(t){return n=>{let e=n.value?.length??Fg(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function HP(t){return n=>{let e=n.value?.length??Fg(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function zP(t){if(!t)return _u;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(Pg(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function _u(t){return null}function Ew(t){return t!=null}function xw(t){return ui(t)?Oe(t):t}function Iw(t){let n={};return t.forEach(e=>{n=e!=null?p(p({},n),e):n}),Object.keys(n).length===0?null:n}function Tw(t,n){return n.map(e=>e(t))}function $P(t){return!t.validate}function Mw(t){return t.map(n=>$P(n)?n:e=>n.validate(e))}function Nw(t){if(!t)return null;let n=t.filter(Ew);return n.length==0?null:function(e){return Iw(Tw(e,n))}}function Lg(t){return t!=null?Nw(Mw(t)):null}function kw(t){if(!t)return null;let n=t.filter(Ew);return n.length==0?null:function(e){let i=Tw(e,n).map(xw);return Bs(i).pipe(ie(Iw))}}function Vg(t){return t!=null?kw(Mw(t)):null}function pw(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function Aw(t){return t._rawValidators}function Rw(t){return t._rawAsyncValidators}function Rg(t){return t?Array.isArray(t)?t:[t]:[]}function bu(t,n){return Array.isArray(t)?t.includes(n):t===n}function gw(t,n){let e=Rg(n);return Rg(t).forEach(r=>{bu(e,r)||e.push(r)}),e}function vw(t,n){return Rg(n).filter(e=>!bu(t,e))}var yu=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Lg(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Vg(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Jo=class extends yu{name;get formDirective(){return null}get path(){return null}};var Ja="VALID",vu="INVALID",Xo="PENDING",el="DISABLED",Ui=class{},Cu=class extends Ui{value;source;constructor(n,e){super(),this.value=n,this.source=e}},nl=class extends Ui{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},il=class extends Ui{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Qo=class extends Ui{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Su=class extends Ui{source;constructor(n){super(),this.source=n}},es=class extends Ui{source;constructor(n){super(),this.source=n}};function Ow(t){return(Eu(t)?t.validators:t)||null}function WP(t){return Array.isArray(t)?Lg(t):t||null}function Pw(t,n){return(Eu(n)?n.asyncValidators:t)||null}function GP(t){return Array.isArray(t)?Vg(t):t||null}function Eu(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function qP(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new y(1e3,"");if(!Fw(i,e))throw new y(1001,"")}function YP(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new y(-1002,"")})}var ts=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=z(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return B(this.statusReactive)}set status(n){B(()=>this.statusReactive.set(n))}_status=E(()=>this.statusReactive());statusReactive=z(void 0);get valid(){return this.status===Ja}get invalid(){return this.status===vu}get pending(){return this.status===Xo}get disabled(){return this.status===el}get enabled(){return this.status!==el}errors;get pristine(){return B(this.pristineReactive)}set pristine(n){B(()=>this.pristineReactive.set(n))}_pristine=E(()=>this.pristineReactive());pristineReactive=z(!0);get dirty(){return!this.pristine}get touched(){return B(this.touchedReactive)}set touched(n){B(()=>this.touchedReactive.set(n))}_touched=E(()=>this.touchedReactive());touchedReactive=z(!1);get untouched(){return!this.touched}_events=new S;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(gw(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(gw(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(vw(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(vw(n,this._rawAsyncValidators))}hasValidator(n){return bu(this._rawValidators,n)}hasAsyncValidator(n){return bu(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(N(p({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new il(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new il(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(N(p({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new nl(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new nl(!0,i))}markAsPending(n={}){this.status=Xo;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Qo(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(N(p({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=el,this.errors=null,this._forEachChild(r=>{r.disable(N(p({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Cu(this.value,i)),this._events.next(new Qo(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(N(p({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Ja,this._forEachChild(i=>{i.enable(N(p({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(N(p({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Ja||this.status===Xo)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Cu(this.value,e)),this._events.next(new Qo(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(N(p({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?el:Ja}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Xo,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=xw(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Qo(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new U,this.statusChanges=new U}_calculateStatus(){return this._allControlsDisabled()?el:this.errors?vu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Xo)?Xo:this._anyControlsHaveStatus(vu)?vu:Ja}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new nl(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new il(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Eu(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=WP(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=GP(this._rawAsyncValidators)}_updateHasRequiredValidator(){B(()=>this._hasRequired.set(this.hasValidator(Wn.required)))}};function Fw(t,n){return Object.hasOwn(t,n)}function Bg(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function Lw(t){if(t.tagName!=="INPUT")return!1;let n=t.type;return n==="number"||n==="range"||n==="date"||n==="month"}function Vw(t){return t.tagName==="INPUT"||t.tagName==="TEXTAREA"}function ol(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var Og=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var ZP=(()=>{class t{_validator=_u;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):_u,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,features:[Ge]})}return t})();var KP={provide:Gr,useExisting:Ot(()=>Bw),multi:!0};var Bw=(()=>{class t extends ZP{required;inputName="required";normalizeInput=Q;createValidator=e=>ww;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=bt(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&ve("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Ae([KP]),Ne]})}return t})();var jw=new g(""),Uw=new g("",{factory:()=>XP}),XP="always";function _w(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),wu(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Du(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function Hw(t,n){let e=Aw(t);n.validator!==null?t.setValidators(pw(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=Rw(t);n.asyncValidator!==null?t.setAsyncValidators(pw(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Du(n._rawValidators,r),Du(n._rawAsyncValidators,r)}function wu(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=Aw(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=Rw(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Du(n._rawValidators,i),Du(n._rawAsyncValidators,i),e}function zw(t,n){t==null,Hw(t,n)}function QP(t,n){return wu(t,n)}function JP(t){return Object.getPrototypeOf(t.constructor)===AP}function $w(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function jg(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Sw?e=o:JP(o)?i=o:r=o}),r||i||e||null}function eF(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var Hi=class extends yu{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof es&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=jg(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(Ue)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(Fe);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new pe,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof es&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=Bg(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof Bw))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&ol(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new Og({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=E(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),et(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}};var rl=class extends ts{constructor(n,e,i){super(Ow(e),Pw(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){B(()=>{YP(this,!0,n),Object.keys(n).forEach(i=>{qP(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,N(p({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new es(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return Fw(this.controls,n)?this.controls[n]:null}};var tF={provide:Jo,useExisting:Ot(()=>sl)},tl=Promise.resolve(),sl=(()=>{class t extends Jo{callSetDisabledState;get submitted(){return B(this.submittedReactive)}_submitted=E(()=>this.submittedReactive());submittedReactive=z(!1);_directives=new Set;form;ngSubmit=new U;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new rl({},Lg(e),Vg(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){tl.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){tl.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){tl.then(()=>{let i=this._findContainer(e.path),r=new rl({});zw(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){tl.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){tl.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),$w(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Su(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ot(Gr,10),ot(Dw,10),ot(Uw,8))};static \u0275dir=M({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&G("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ae([tF]),Ne]})}return t})();function bw(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function yw(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var nF=class extends ts{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(Ow(e),Pw(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Eu(e)&&(e.nonNullable||e.initialValueIsDefault)&&(yw(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){B(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new es(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){bw(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){bw(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){yw(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var iF=t=>t instanceof nF;var rF=(()=>{class t extends Jo{callSetDisabledState;get submitted(){return B(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=E(()=>this._submittedReactive());_submittedReactive=z(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(wu(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){_w(e.control||null,e,!1),eF(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,$w(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Su(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(_w(i||null,e),iF(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);zw(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&QP(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){Hw(this.form,this),this._oldForm&&wu(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(ot(Gr,10),ot(Dw,10),ot(Uw,8))};static \u0275dir=M({type:t,features:[Ne,Ge]})}return t})();var oF={provide:Jo,useExisting:Ot(()=>al)},al=(()=>{class t extends rF{form=null;ngSubmit=new U;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=bt(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&G("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ae([oF]),Ne]})}return t})();var Zw=Symbol("FIELD_TREE"),Ug=0;function sF(){return Ug}function zi(t,n){return(...e)=>{try{return Ug=n,t(...e)}finally{Ug=0}}}function aF(t){return!t}function Ww(t){return t}function Wi(t){return Array.isArray(t)}function Iu(t){return(typeof t=="object"||typeof t=="function")&&t!=null}var $i=Symbol(),Ou=Symbol(),cl=class{predicates;fns=[];constructor(n){this.predicates=n}push(n){this.fns.push(Gw(this.predicates,n))}mergeIn(n){let e=this.predicates?n.fns.map(i=>Gw(this.predicates,i)):n.fns;this.fns.push(...e)}hasRules(){return this.fns.length>0}},Tu=class extends cl{get defaultValue(){return!1}compute(n){return this.fns.some(e=>{let i=e(n);return i&&i!==Ou})}},is=class t extends cl{ignore;static ignoreNull(n){return new t(n,e=>e===null)}constructor(n,e){super(n),this.ignore=e}get defaultValue(){return[]}compute(n){return this.fns.reduce((e,i)=>{let r=i(n);return r===void 0||r===Ou?e:Wi(r)?[...e,...this.ignore?r.filter(o=>!this.ignore(o)):r]:this.ignore&&this.ignore(r)?e:[...e,r]},[])}},Hg=class extends is{constructor(n){super(n,void 0)}},zg=class extends cl{key;get defaultValue(){return this.key.reducer.getInitial()}constructor(n,e){super(n),this.key=e}compute(n){if(this.fns.length===0)return this.key.reducer.getInitial();let e=this.key.reducer.getInitial();for(let i=0;i<this.fns.length;i++){let r=this.fns[i](n);r!==Ou&&(e=this.key.reducer.reduce(e,r))}return e}};function Gw(t,n){return t.length===0?n:e=>{for(let i of t){let r=e.stateOf(i.path),o=B(r.structure.pathKeys).length-i.depth;for(let s=0;s<o;s++)r=r.structure.parent;if(!i.fn(r.context))return Ou}return n(e)}}var rs=class{predicates;hidden;disabledReasons;readonly;syncErrors;syncTreeErrors;asyncErrors;metadata=new Map;constructor(n){this.predicates=n,this.hidden=new Tu(n),this.disabledReasons=new Hg(n),this.readonly=new Tu(n),this.syncErrors=is.ignoreNull(n),this.syncTreeErrors=is.ignoreNull(n),this.asyncErrors=is.ignoreNull(n)}hasAnyLogic(){return this.hidden.hasRules()||this.disabledReasons.hasRules()||this.readonly.hasRules()||this.syncErrors.hasRules()||this.syncTreeErrors.hasRules()||this.asyncErrors.hasRules()||this.metadata.size>0}hasMetadata(n){return this.metadata.has(n)}hasMetadataKeys(){return this.metadata.size>0}getMetadataKeys(){return this.metadata.keys()}getMetadata(n){return this.metadata.has(n)||this.metadata.set(n,new zg(this.predicates,n)),this.metadata.get(n)}mergeIn(n){this.hidden.mergeIn(n.hidden),this.disabledReasons.mergeIn(n.disabledReasons),this.readonly.mergeIn(n.readonly),this.syncErrors.mergeIn(n.syncErrors),this.syncTreeErrors.mergeIn(n.syncTreeErrors),this.asyncErrors.mergeIn(n.asyncErrors);for(let e of n.getMetadataKeys()){let i=n.metadata.get(e);this.getMetadata(e).mergeIn(i)}}},Mu=class{depth;constructor(n){this.depth=n}build(){return new Nu(this,[],0)}},os=class t extends Mu{constructor(n){super(n)}current;all=[];addHiddenRule(n){this.getCurrent().addHiddenRule(n)}addDisabledReasonRule(n){this.getCurrent().addDisabledReasonRule(n)}addReadonlyRule(n){this.getCurrent().addReadonlyRule(n)}addSyncErrorRule(n){this.getCurrent().addSyncErrorRule(n)}addSyncTreeErrorRule(n){this.getCurrent().addSyncTreeErrorRule(n)}addAsyncErrorRule(n){this.getCurrent().addAsyncErrorRule(n)}addMetadataRule(n,e){this.getCurrent().addMetadataRule(n,e)}getChild(n){if(n===$i){let e=this.getCurrent().children;e.size>(e.has($i)?1:0)&&(this.current=void 0)}return this.getCurrent().getChild(n)}hasLogic(n){return this===n?!0:this.all.some(({builder:e})=>e.hasLogic(n))}hasRules(){return this.all.length>0}anyChildHasLogic(){return this.all.some(({builder:n})=>n.anyChildHasLogic())}mergeIn(n,e){e?this.all.push({builder:n,predicate:{fn:zi(e.fn,this.depth),path:e.path}}):this.all.push({builder:n}),this.current=void 0}getCurrent(){return this.current===void 0&&(this.current=new dl(this.depth),this.all.push({builder:this.current})),this.current}static newRoot(){return new t(0)}},dl=class extends Mu{logic=new rs([]);children=new Map;constructor(n){super(n)}addHiddenRule(n){this.logic.hidden.push(zi(n,this.depth))}addDisabledReasonRule(n){this.logic.disabledReasons.push(zi(n,this.depth))}addReadonlyRule(n){this.logic.readonly.push(zi(n,this.depth))}addSyncErrorRule(n){this.logic.syncErrors.push(zi(n,this.depth))}addSyncTreeErrorRule(n){this.logic.syncTreeErrors.push(zi(n,this.depth))}addAsyncErrorRule(n){this.logic.asyncErrors.push(zi(n,this.depth))}addMetadataRule(n,e){this.logic.getMetadata(n).push(zi(e,this.depth))}getChild(n){return this.children.has(n)||this.children.set(n,new os(this.depth+1)),this.children.get(n)}hasLogic(n){return this===n}hasRules(){return this.logic.hasAnyLogic()||this.children.size>0}anyChildHasLogic(){for(let n of this.children.values())if(n.hasRules())return!0;return!1}},Nu=class t{builder;predicates;depth;logic;constructor(n,e,i){this.builder=n,this.predicates=e,this.depth=i,this.logic=n?lF(n,e,i):new rs([])}getChild(n){let e=this.builder?Kw(this.builder,n):[];if(e.length===0)return new t(void 0,[],this.depth+1);if(e.length===1){let{builder:i,predicates:r}=e[0];return new t(i,[...this.predicates,...r.map(o=>Wg(o,this.depth))],this.depth+1)}else{let i=e.map(({builder:r,predicates:o})=>new t(r,[...this.predicates,...o.map(s=>Wg(s,this.depth))],this.depth+1));return new $g(i)}}hasLogic(n){return this.builder?this.builder.hasLogic(n):!1}hasRules(){return this.builder?this.builder.hasRules():!1}anyChildHasLogic(){return this.builder?this.builder.anyChildHasLogic():!1}},$g=class t{all;logic;constructor(n){this.all=n,this.logic=new rs([]);for(let e of n)this.logic.mergeIn(e.logic)}getChild(n){return new t(this.all.flatMap(e=>e.getChild(n)))}hasLogic(n){return this.all.some(e=>e.hasLogic(n))}hasRules(){return this.all.some(n=>n.hasRules())}anyChildHasLogic(){return this.all.some(n=>n.anyChildHasLogic())}};function Kw(t,n){if(t instanceof os)return t.all.flatMap(({builder:e,predicate:i})=>{let r=Kw(e,n);return i?r.map(({builder:o,predicates:s})=>({builder:o,predicates:[...s,i]})):r});if(t instanceof dl)return[...n!==$i&&t.children.has($i)?[{builder:t.getChild($i),predicates:[]}]:[],...t.children.has(n)?[{builder:t.getChild(n),predicates:[]}]:[]];throw new y(1909,!1)}function lF(t,n,e){let i=new rs(n);if(t instanceof os){let r=t.all.map(({builder:o,predicate:s})=>new Nu(o,s?[...n,Wg(s,e)]:n,e));for(let o of r)i.mergeIn(o.logic)}else if(t instanceof dl)i.mergeIn(t.logic);else throw new y(1909,!1);return i}function Wg(t,n){return N(p({},t),{depth:n})}var Xw=Symbol("PATH"),ln=class t{keys;parent;keyInParent;root;children=new Map;fieldPathProxy=new Proxy(this,cF);logicBuilder;constructor(n,e,i,r){this.keys=n,this.parent=i,this.keyInParent=r,this.root=e??this,i||(this.logicBuilder=os.newRoot())}get builder(){return this.logicBuilder?this.logicBuilder:this.parent.builder.getChild(this.keyInParent)}getChild(n){return this.children.has(n)||this.children.set(n,new t([...this.keys,n],this.root,this,n)),this.children.get(n)}mergeIn(n,e){let i=n.compile();this.builder.mergeIn(i.builder,e)}static unwrapFieldPath(n){return n[Xw]}static newRoot(){return new t([],void 0,void 0,void 0)}},cF={get(t,n){return n===Xw?t:t.getChild(n).fieldPathProxy}},xu,ll=new Map,ul=class t{schemaFn;constructor(n){this.schemaFn=n}compile(){if(ll.has(this))return ll.get(this);let n=ln.newRoot();ll.set(this,n);let e=xu;try{xu=n,this.schemaFn(n.fieldPathProxy)}finally{xu=e}return n}static create(n){return n instanceof t?n:new t(n)}static rootCompile(n){try{return ll.clear(),n===void 0?ln.newRoot():n instanceof t?n.compile():new t(n).compile()}finally{ll.clear()}}};function dF(t){return t instanceof ul||typeof t=="function"}function ss(t){if(xu!==ln.unwrapFieldPath(t).root)throw new y(1908,!1)}function qr(t,n,e){return ss(t),ln.unwrapFieldPath(t).builder.addMetadataRule(n,e),n}var Gi={list(){return{reduce:(t,n)=>n===void 0?t:[...t,n],getInitial:()=>[]}},min(){return{reduce:(t,n)=>t===void 0||n===void 0?t??n:n<t?n:t,getInitial:()=>{}}},max(){return{reduce:(t,n)=>t===void 0||n===void 0?t??n:n>t?n:t,getInitial:()=>{}}},or(){return{reduce:(t,n)=>t||n,getInitial:()=>!1}},and(){return{reduce:(t,n)=>t&&n,getInitial:()=>!0}},override:uF};function uF(t){return{reduce:(n,e)=>e,getInitial:()=>t?.()}}var nv=Symbol("IS_ASYNC_VALIDATION_RESOURCE"),ku=class{reducer;create;brand;[nv];constructor(n,e){this.reducer=n,this.create=e}};function wn(t){return new ku(t??Gi.override())}function iv(){return wn()}var Qw=wn(Gi.or()),rv=iv();var ov=wn(Gi.max()),sv=iv();var av=wn(Gi.min()),Jw=wn(Gi.max()),eE=wn(Gi.min()),tE=wn(Gi.list());function Wt(t,n){if(t===n)return!0;if(!t||!n||t.length!==n.length)return!1;for(let e=0;e<t.length;e++)if(!Object.is(t[e],n[e]))return!1;return!0}function fF(t){return t.errors().length>0?"invalid":t.pending()?"unknown":"valid"}var Gg=class{node;constructor(n){this.node=n}rawSyncTreeErrors=E(()=>this.shouldSkipValidation()?[]:[...this.node.logicNode.logic.syncTreeErrors.compute(this.node.context),...this.node.structure.parent?.validationState.rawSyncTreeErrors()??[]],{equal:Wt});syncErrors=E(()=>this.shouldSkipValidation()?[]:[...this.node.logicNode.logic.syncErrors.compute(this.node.context),...this.syncTreeErrors(),...hF(this.node.submitState.submissionErrors())],{equal:Wt});syncValid=E(()=>this.shouldSkipValidation()?!0:this.node.structure.reduceChildren(this.syncErrors().length===0,(n,e)=>e&&n.validationState.syncValid(),aF));syncTreeErrors=E(()=>this.rawSyncTreeErrors().filter(n=>n.fieldTree===this.node.fieldTree),{equal:Wt});rawAsyncErrors=E(()=>this.shouldSkipValidation()?[]:[...this.node.logicNode.logic.asyncErrors.compute(this.node.context),...this.node.structure.parent?.validationState.rawAsyncErrors()??[]],{equal:Wt});asyncErrors=E(()=>this.shouldSkipValidation()?[]:this.rawAsyncErrors().filter(n=>n==="pending"||n.fieldTree===this.node.fieldTree),{equal:Wt});parseErrors=E(()=>this.node.formFieldBindings().flatMap(n=>n.parseErrors()),{equal:Wt});errors=E(()=>[...this.parseErrors(),...this.syncErrors(),...this.asyncErrors().filter(n=>n!=="pending")],{equal:Wt});errorSummary=E(()=>{let n=this.node.structure.reduceChildren(this.errors(),(e,i)=>[...i,...e.errorSummary()]);return B(()=>n.sort(mF)),n},{equal:Wt});pending=E(()=>this.node.structure.reduceChildren(this.asyncErrors().includes("pending"),(n,e)=>e||n.validationState.pending()));status=E(()=>{if(this.shouldSkipValidation())return"valid";let n=fF(this);return this.node.structure.reduceChildren(n,(e,i)=>i==="invalid"||e.validationState.status()==="invalid"?"invalid":i==="unknown"||e.validationState.status()==="unknown"?"unknown":"valid",e=>e==="invalid")});valid=E(()=>this.status()==="valid");invalid=E(()=>this.status()==="invalid");shouldSkipValidation=E(()=>this.node.hidden()||this.node.disabled()||this.node.readonly()||this.node.structure.isOrphaned())};function hF(t){return t===void 0?[]:Wi(t)?t:[t]}function nE(t,n){if(Wi(t))for(let e of t)e.fieldTree??=n;else t&&(t.fieldTree??=n);return t}function qw(t){return t.formField?t.formField.element:t.fieldTree().formFieldBindings().reduce((n,e)=>!n||!e.element?n??e.element:n.compareDocumentPosition(e.element)&Node.DOCUMENT_POSITION_PRECEDING?e.element:n,void 0)}function mF(t,n){let e=qw(t),i=qw(n);return e===i?0:e===void 0||i===void 0?e===void 0?1:-1:e.compareDocumentPosition(i)&Node.DOCUMENT_POSITION_PRECEDING?1:-1}var qg=wn(),Yg=class{node;cache=new WeakMap;constructor(n){this.node=n,this.fieldTreeOf=this.fieldTreeOf.bind(this),this.stateOf=this.stateOf.bind(this)}resolve(n){if(!this.cache.has(n)){let e=E(()=>{let i=ln.unwrapFieldPath(n),r=this.node,o=sF();for(;o>0||!r.structure.logic.hasLogic(i.root.builder);)if(o--,r=r.structure.parent,r===void 0)throw new y(1900,!1);for(let s of i.keys)if(r=r.structure.getChild(s),r===void 0)throw new y(1901,!1);return r.fieldTree});this.cache.set(n,e)}return this.cache.get(n)()}get fieldTree(){return this.node.fieldProxy}get state(){return this.node}get value(){return this.node.structure.value}get key(){return this.node.structure.keyInParent}get pathKeys(){return this.node.structure.pathKeys}index=E(()=>{let n=this.key();if(!Wi(B(this.node.structure.parent.value)))throw new y(1906,!1);return Number(n)});fieldTreeOf(n){return this.resolve(n)}stateOf(n){return this.resolve(n)()}valueOf=n=>{let e=this.resolve(n)().value();if(e instanceof ts)throw new y(1907,!1);return e}},Zg=class{node;metadata=new Map;constructor(n){this.node=n}runMetadataCreateLifecycle(){if(!this.node.logicNode.logic.hasMetadataKeys())return;let n=xp();n&&jd(!1);try{B(()=>Le(this.node.structure.injector,()=>{for(let e of this.node.logicNode.logic.getMetadataKeys())if(e.create){let i=this.node.logicNode.logic.getMetadata(e),r=e.create(this.node,E(()=>i.compute(this.node.context)));this.metadata.set(e,r)}}))}finally{n&&jd(!0)}}get(n){if(this.has(n)&&!this.metadata.has(n)){if(n.create)throw new y(1912,!1);let e=this.node.logicNode.logic.getMetadata(n);this.metadata.set(n,E(()=>e.compute(this.node.context)))}return this.metadata.get(n)}has(n){return this.node.logicNode.logic.hasMetadata(n)}},pF={get(t,n,e){if(n===Zw)return!0;let i=t(),r=i.structure.getChild(n);if(r!==void 0)return r.fieldTree;let o=B(i.value);if(Wi(o)){if(n==="length")return i.value().length;if(n===Symbol.iterator)return()=>(i.value(),Array.prototype[Symbol.iterator].apply(i.fieldTree))}if(Iu(o)&&n===Symbol.iterator)return function*(){for(let s in e)yield[s,e[s]]}},getOwnPropertyDescriptor(t,n){let e=B(t().value),i=Reflect.getOwnPropertyDescriptor(e,n);return i&&!i.configurable&&(i.configurable=!0),i},ownKeys(t){let n=B(t().value);return typeof n=="object"&&n!==null?Reflect.ownKeys(n):[]}};function gF(t,n){let e=E(()=>t()[n()]);return e[Te]=t[Te],e.set=i=>{Object.is(B(e),i)||t.update(r=>vF(r,i,n()))},e.update=i=>{e.set(i(B(e)))},e.asReadonly=()=>e,e}function vF(t,n,e){if(Wi(t)){let i=[...t];return i[e]=n,i}else return N(p({},t),{[e]:n})}var ns=Symbol(""),iE=E(()=>!1),Au=class{logic;node;createChildNode;identitySymbol=Symbol();_injector=void 0;_anyChildHasLogic;get injector(){return this._injector??=P.create({providers:[],parent:this.fieldManager.injector}),this._injector}constructor(n,e,i){this.logic=n,this.node=e,this.createChildNode=i}children(){this.ensureChildrenMap();let n=this.childrenMap();return n===void 0?[]:Array.from(n.byPropertyKey.values()).map(e=>B(e.reader))}materializedChildren(){let n=this.childrenMap();return n===void 0?[]:Array.from(n.byPropertyKey.values()).map(e=>e.node)}_areChildrenMaterialized(){return B(this.childrenMap)!==void 0}ensureChildrenMap(){this._areChildrenMaterialized()||B(()=>{this.childrenMap.update(n=>this.computeChildrenMap(this.value(),n,!0))})}getChild(n){this.ensureChildrenMap();let e=n.toString(),i=B(this.childrenMap)?.byPropertyKey.get(e)?.reader;return i||(i=this.createReader(e)),i()}reduceChildren(n,e,i){let r=this.childrenMap();if(!r)return n;let o=n;for(let s of r.byPropertyKey.values()){if(i?.(o))break;o=e(B(s.reader),o)}return o}destroy(){this.injector.destroy()}createKeyOrOrphanSignals(n,e,i){if(n==="root")return{keyInParent:rE,isOrphaned:iE};let r=this.parent,o=i,s=E(()=>{if(r.structure.isOrphaned())return ns;let c=r.structure.childrenMap();if(!c)return ns;let u=c.byPropertyKey.get(o);if(u&&u.node===this.node)return o;if(e===void 0)return ns;for(let[f,h]of c.byPropertyKey)if(h.node===this.node)return o=f;return ns}),a=E(()=>s()===ns);return{keyInParent:E(()=>{let c=s();if(c===ns)throw e===void 0?new y(-1902,!1):new y(1904,!1);return c}),isOrphaned:a}}createChildrenMap(){return hi({source:this.value,computation:(n,e)=>this.computeChildrenMap(n,e?.value,!1)})}computeChildrenMap(n,e,i){if(!Iu(n)||!i&&e===void 0&&!(this._anyChildHasLogic??=this.logic.anyChildHasLogic()))return;e??={byPropertyKey:new Map};let r,o=Wi(n);e!==void 0&&(o?r=bF(e,n,this.identitySymbol):r=yF(e,n));for(let s of Object.keys(n)){let a,l=n[s];if(l===void 0){e.byPropertyKey.has(s)&&(r??=p({},e),r.byPropertyKey.delete(s));continue}o&&Iu(l)&&!Wi(l)&&(a=l[this.identitySymbol]??=Symbol(""));let c;a&&(e.byTrackingKey?.has(a)||(r??=p({},e),r.byTrackingKey??=new Map,r.byTrackingKey.set(a,this.createChildNode(s,a,o))),c=(r??e).byTrackingKey.get(a));let u=e.byPropertyKey.get(s);u===void 0?(r??=p({},e),r.byPropertyKey.set(s,{reader:this.createReader(s),node:c??this.createChildNode(s,a,o)})):c&&c!==u.node&&(r??=p({},e),u.node=c)}return r??e}createReader(n){return E(()=>this.childrenMap()?.byPropertyKey.get(n)?.node)}},Kg=class extends Au{fieldManager;value;get parent(){}get root(){return this.node}get pathKeys(){return _F}get keyInParent(){return rE}isOrphaned=iE;childrenMap;constructor(n,e,i,r,o){super(e,n,o),this.fieldManager=i,this.value=r,this.childrenMap=this.createChildrenMap()}},Xg=class extends Au{logic;parent;root;pathKeys;keyInParent;value;childrenMap;isOrphaned;get fieldManager(){return this.root.structure.fieldManager}constructor(n,e,i,r,o,s){super(e,n,s),this.logic=e,this.parent=i,this.root=this.parent.structure.root;let a=this.createKeyOrOrphanSignals("child",r,o);this.isOrphaned=a.isOrphaned,this.keyInParent=a.keyInParent,this.pathKeys=E(()=>[...i.structure.pathKeys(),this.keyInParent()]),this.value=gF(this.parent.structure.value,this.keyInParent),this.childrenMap=this.createChildrenMap(),this.fieldManager.structures.add(this)}};var _F=E(()=>[]),rE=E(()=>{throw new y(1905,!1)});function bF(t,n,e){let i,r=new Set(t.byPropertyKey.keys()),o=t.byTrackingKey&&new Set(t.byTrackingKey.keys());for(let s=0;s<n.length;s++){let a=n[s];r.delete(s.toString()),o&&Iu(a)&&Object.hasOwn(a,e)&&o.delete(a[e])}if(r.size>0){i??=p({},t);for(let s of r)i.byPropertyKey.delete(s)}if(o&&o.size>0){i??=p({},t);for(let s of o)i.byTrackingKey.delete(s)}return i}function yF(t,n){let e;for(let i of t.byPropertyKey.keys())n.hasOwnProperty(i)||(e??=p({},t),e.byPropertyKey.delete(i));return e}var Qg=class{node;selfSubmitting=z(!1);submissionErrors;constructor(n){this.node=n,this.submissionErrors=hi({source:this.node.structure.value,computation:()=>[]})}submitting=E(()=>this.selfSubmitting()||(this.node.structure.parent?.submitting()??!1))},fl=class{structure;validationState;metadataState;nodeState;submitState;fieldAdapter;controlValue;_context=void 0;get context(){return this._context??=new Yg(this)}fieldProxy=new Proxy(()=>this,pF);pathNode;constructor(n){this.pathNode=n.pathNode,this.fieldAdapter=n.fieldAdapter,this.structure=this.fieldAdapter.createStructure(this,n),this.validationState=this.fieldAdapter.createValidationState(this,n),this.nodeState=this.fieldAdapter.createNodeState(this,n),this.metadataState=new Zg(this),this.submitState=new Qg(this),this.controlValue=this.controlValueSignal(),this.metadataState.runMetadataCreateLifecycle()}focusBoundControl(n){this.getBindingForFocus()?.focus(n)}getBindingForFocus(){let n=this.formFieldBindings().filter(e=>e.focus!==void 0).reduce(Yw,void 0);return n||this.structure.children().map(e=>e.getBindingForFocus()).reduce(Yw,void 0)}pendingSync=hi({source:()=>this.value(),computation:(n,e)=>{e?.value?.abort()}});get fieldTree(){return this.fieldProxy}get logicNode(){return this.structure.logic}get value(){return this.structure.value}get keyInParent(){return this.structure.keyInParent}get errors(){return this.validationState.errors}get parseErrors(){return this.validationState.parseErrors}get errorSummary(){return this.validationState.errorSummary}get pending(){return this.validationState.pending}get valid(){return this.validationState.valid}get invalid(){return this.validationState.invalid}get dirty(){return this.nodeState.dirty}get touched(){return this.nodeState.touched}get disabled(){return this.nodeState.disabled}get disabledReasons(){return this.nodeState.disabledReasons}get hidden(){return this.nodeState.hidden}get readonly(){return this.nodeState.readonly}get formFieldBindings(){return this.nodeState.formFieldBindings}get submitting(){return this.submitState.submitting}get name(){return this.nodeState.name}get max(){let n=this.metadata(sv)?.();return n?this.metadata(n):void 0}get maxLength(){return this.metadata(eE)}get min(){let n=this.metadata(rv)?.();return n?this.metadata(n):void 0}get minLength(){return this.metadata(Jw)}get pattern(){return this.metadata(tE)??CF}get required(){return this.metadata(Qw)??SF}metadata(n){return this.metadataState.get(n)}getError(n){return this.errors().find(e=>e.kind===n)}hasMetadata(n){return this.metadataState.has(n)}markAsTouched(n){this.structure.isOrphaned()||B(()=>{this.markAsTouchedInternal(n),this.flushSync()})}markAsTouchedInternal(n){if(!this.structure.isOrphaned()&&!this.validationState.shouldSkipValidation()&&(this.nodeState.markAsTouched(),!n?.skipDescendants))for(let e of this.structure.children())e.markAsTouchedInternal()}markAsDirty(){this.nodeState.markAsDirty()}markAsPristine(){this.nodeState.markAsPristine()}markAsUntouched(){this.nodeState.markAsUntouched()}reset(n){B(()=>this._reset(n))}_reset(n){this.pendingSync()?.abort(),n!==void 0&&this.value.set(n),this.controlValue.rawSet(this.value()),this.nodeState.markAsUntouched(),this.nodeState.markAsPristine();for(let e of this.formFieldBindings())e.reset();for(let e of this.structure.materializedChildren())e._reset()}reloadValidation(){B(()=>this._reloadValidation())}_reloadValidation(){let n=this.logicNode.logic.getMetadataKeys();for(let e of n)e[nv]&&this.metadata(e).reload?.();for(let e of this.structure.children())e._reloadValidation()}controlValueSignal(){let n=hi(this.value);n.rawSet=n.set,n.set=i=>{n.rawSet(i),this.markAsDirty(),this.debounceSync()};let e=n.update;return n.update=i=>{e(i),this.markAsDirty(),this.debounceSync()},n}sync(){this.value.set(this.controlValue())}flushSync(){let n=this.pendingSync();n&&!n.signal.aborted&&(n.abort(),this.sync())}async debounceSync(){let n=B(()=>(this.pendingSync()?.abort(),this.nodeState.debouncer()));if(n){let e=new AbortController,i=n(e.signal);if(i&&(this.pendingSync.set(e),await i,e.signal.aborted))return}this.structure.isOrphaned()||this.sync()}static newRoot(n,e,i,r){return r.newRoot(n,e,i,r)}createStructure(n){return n.kind==="root"?new Kg(this,n.logic,n.fieldManager,n.value,this.newChild.bind(this)):new Xg(this,n.logic,n.parent,n.identityInParent,n.initialKeyInParent,this.newChild.bind(this))}newChild(n,e,i){let r,o;return i?(r=this.pathNode.getChild($i),o=this.structure.logic.getChild($i)):(r=this.pathNode.getChild(n),o=this.structure.logic.getChild(n)),this.fieldAdapter.newChild({kind:"child",parent:this,pathNode:r,logic:o,initialKeyInParent:n,identityInParent:e,fieldAdapter:this.fieldAdapter})}},CF=E(()=>[]),SF=E(()=>!1);function Yw(t,n){return t?n&&t.element.compareDocumentPosition(n.element)&Node.DOCUMENT_POSITION_PRECEDING?n:t:n}var Jg=class{node;selfTouched=z(!1);selfDirty=z(!1);markAsTouched(){this.selfTouched.set(!0)}markAsDirty(){this.selfDirty.set(!0)}markAsPristine(){this.selfDirty.set(!1)}markAsUntouched(){this.selfTouched.set(!1)}formFieldBindings=z([]);constructor(n){this.node=n}dirty=E(()=>{let n=this.selfDirty()&&!this.isNonInteractive();return this.node.structure.reduceChildren(n,(e,i)=>i||e.nodeState.dirty(),Ww)});touched=E(()=>{let n=this.selfTouched()&&!this.isNonInteractive();return this.node.structure.reduceChildren(n,(e,i)=>i||e.nodeState.touched(),Ww)});disabledReasons=E(()=>[...this.node.structure.parent?.nodeState.disabledReasons()??[],...this.node.logicNode.logic.disabledReasons.compute(this.node.context)],{equal:Wt});disabled=E(()=>!!this.disabledReasons().length);readonly=E(()=>(this.node.structure.parent?.nodeState.readonly()||this.node.logicNode.logic.readonly.compute(this.node.context))??!1);hidden=E(()=>(this.node.structure.parent?.nodeState.hidden()||this.node.logicNode.logic.hidden.compute(this.node.context))??!1);name=E(()=>{let n=this.node.structure.parent;return n?`${n.name()}.${this.node.structure.keyInParent()}`:this.node.structure.fieldManager.rootName});debouncer=E(()=>{if(this.node.logicNode.logic.hasMetadata(qg)){let e=this.node.logicNode.logic.getMetadata(qg).compute(this.node.context);if(e)return i=>e(this.node.context,i)}return this.node.structure.parent?.nodeState.debouncer?.()});isNonInteractive=E(()=>this.hidden()||this.disabled()||this.readonly())},ev=class{newRoot(n,e,i,r){return new fl({kind:"root",fieldManager:n,value:e,pathNode:i,logic:i.builder.build(),fieldAdapter:r})}newChild(n){return new fl(n)}createNodeState(n){return new Jg(n)}createValidationState(n){return new Gg(n)}createStructure(n,e){return n.createStructure(e)}},tv=class{injector;rootName;submitOptions;constructor(n,e,i){this.injector=n,this.rootName=e??`${this.injector.get(mn)}.form${DF++}`,this.submitOptions=i}structures=new Set;createFieldManagementEffect(n){et(()=>{let e=new Set;this.markStructuresLive(n,e);for(let i of this.structures)e.has(i)||(this.structures.delete(i),B(()=>i.destroy()))},{injector:this.injector})}markStructuresLive(n,e){e.add(n);for(let i of n.children())this.markStructuresLive(i.structure,e)}},DF=0,oE=new g("");function wF(t){let n,e,i;return t.length===3?[n,e,i]=t:t.length===2?dF(t[1])?[n,e]=t:[n,i]=t:[n]=t,[n,e,i]}function hl(...t){let[n,e,i]=wF(t),r=i?.injector??d(P),o=Le(r,()=>ul.rootCompile(e)),s=new tv(r,i?.name,i?.submission),a=i?.adapter??new ev,l=fl.newRoot(s,n,o,a);s.createFieldManagementEffect(l.structure);let{experimentalWebMcpTool:c}=i??{};if(c){let u=Le(r,()=>d(oE,{optional:!0}));u&&Le(r,()=>u(l.fieldTree,{name:c.name,description:c.description}))}return l.fieldTree}function Pu(t,n){ss(t);let e=ln.unwrapFieldPath(t).getChild($i).fieldPathProxy;sE(e,n)}function sE(t,n){ss(t),ln.unwrapFieldPath(t).mergeIn(ul.create(n))}var Ru=class{kind="compat";control;fieldTree;context;message;constructor({context:n,kind:e,control:i}){this.context=n,this.kind=e,this.control=i}};function aE(t){if(t.length===0)return null;let n={};for(let e of t)n[e.kind]=e instanceof Ru?e.context:e;return n}function lE(t,n){return t===null?[]:Object.entries(t).map(([e,i])=>new Ru({context:i,kind:e,control:n}))}var EF=new g("");function fE(t,n){ss(t);let e=ln.unwrapFieldPath(t),i;typeof n=="function"||typeof n=="string"?i=n:i=n?.when,e.builder.addDisabledReasonRule(r=>{let o=!0;return typeof i=="string"?o=i:i&&(o=i(r)),typeof o=="string"?{fieldTree:r.fieldTree,message:o}:o?{fieldTree:r.fieldTree}:void 0})}function Fu(t,n){return t instanceof Function?t(n):t}function cE(t){return t===void 0?[]:Array.isArray(t)?t:[t]}function hE(t,n){ss(t),ln.unwrapFieldPath(t).builder.addSyncErrorRule(i=>nE(n(i),i.fieldTree))}function xF(t,n){return new lv(t,n)}function IF(t,n){return new cv(t,n)}var ml=class{__brand=void 0;kind="";fieldTree;message;constructor(n){n&&Object.assign(this,n)}};var lv=class extends ml{min;kind="min";constructor(n,e){super(e),this.min=n}};var cv=class extends ml{max;kind="max";constructor(n,e){super(e),this.max=n}};var Lu=class extends ml{kind="parse"};function mE(t,n,e){let i=wn();qr(t,i,r=>{if(!(e?.when&&!e.when(r)))return typeof n=="function"?n(r):n}),qr(t,av,({state:r})=>r.metadata(i)()),qr(t,sv,()=>av),hE(t,r=>{let o=r.value();if(o===null||Number.isNaN(o))return;let s=r.state.metadata(i)();if(!(s===void 0||Number.isNaN(s))&&o>s)return e?.error?Fu(e.error,r):IF(s,{message:Fu(e?.message,r)})})}function pE(t,n,e){let i=wn();qr(t,i,r=>{if(!(e?.when&&!e.when(r)))return typeof n=="function"?n(r):n}),qr(t,ov,({state:r})=>r.metadata(i)()),qr(t,rv,()=>ov),hE(t,r=>{let o=r.value();if(o===null||Number.isNaN(o))return;let s=r.state.metadata(i)();if(!(s===void 0||Number.isNaN(s))&&o<s)return e?.error?Fu(e.error,r):xF(s,{message:Fu(e?.message,r)})})}function TF(t,n,e){let i=hi({source:t,computation:()=>[],equal:Wt}),r=s=>{let a=e(s);i.set(cE(a.error)),a.value!==void 0&&n(a.value),i.set(cE(a.error))},o=()=>{i.set([])};return{errors:i.asReadonly(),setRawValue:r,reset:o}}var dv=class{field;constructor(n){this.field=n}control=this;get value(){return this.field().controlValue()}get valid(){return this.field().valid()}get invalid(){return this.field().invalid()}get pending(){return this.field().pending()}get disabled(){return this.field().disabled()}get enabled(){return!this.field().disabled()}get errors(){return aE(this.field().errors())}get pristine(){return!this.field().dirty()}get dirty(){return this.field().dirty()}get touched(){return this.field().touched()}get untouched(){return!this.field().touched()}get status(){if(this.field().disabled())return"DISABLED";if(this.field().valid())return"VALID";if(this.field().invalid())return"INVALID";if(this.field().pending())return"PENDING";throw new y(1910,!1)}valueAccessor=null;hasValidator(n){return n===Wn.required?this.field().required():!1}updateValueAndValidity(){}},uv={disabled:"disabled",disabledReasons:"disabledReasons",dirty:"dirty",errors:"errors",hidden:"hidden",invalid:"invalid",max:"max",maxLength:"maxLength",min:"min",minLength:"minLength",name:"name",pattern:"pattern",pending:"pending",readonly:"readonly",required:"required",touched:"touched"},MF=(()=>{let t={};for(let n of Object.keys(uv))t[uv[n]]=n;return t})();function fv(t,n){let e=MF[n];return t[e]?.()}var hv=Object.values(uv);function Vu(){return{}}function Yr(t,n,e){return t[n]!==e?(t[n]=e,!0):!1}function NF(t,n,e){let i;if(gE(t)&&e.isBadInput(t))return{error:new Lu};switch(t.type){case"checkbox":return{value:t.checked};case"number":case"range":case"datetime-local":if(i=B(n),typeof i=="number"||i===null)return{value:t.value===""?null:t.valueAsNumber};break;case"date":case"month":case"time":case"week":if(i=B(n),i===null||i instanceof Date)return{value:t.valueAsDate};if(typeof i=="number")return{value:t.valueAsNumber};break}if(t.tagName==="INPUT"&&t.type==="text"&&(i??=B(n),typeof i=="number"||i===null)){if(t.value==="")return{value:null};let r=Number(t.value);return Number.isNaN(r)?{error:new Lu}:{value:r}}return{value:t.value}}function dE(t,n){switch(t.type){case"checkbox":t.checked=n;return;case"radio":t.checked=n===t.value;return;case"number":case"range":case"datetime-local":if(typeof n=="number"){uE(t,n);return}else if(n===null){t.value="";return}break;case"date":case"month":case"time":case"week":if(n===null||n instanceof Date){t.valueAsDate=n;return}else if(typeof n=="number"){uE(t,n);return}}if(t.tagName==="INPUT"&&t.type==="text"){if(typeof n=="number"){t.value=isNaN(n)?"":String(n);return}if(n===null){t.value="";return}}t.value=n}function uE(t,n){isNaN(n)?t.value="":t.valueAsNumber=n}function gE(t){return t.tagName==="INPUT"}function kF(t){return t.type==="date"||t.type==="datetime-local"||t.type==="month"||t.type==="time"||t.type==="week"}function AF(t,n){let e=t.getUTCFullYear(),i=String(t.getUTCMonth()+1).padStart(2,"0");if(n==="month")return`${e}-${i}`;let r=String(t.getUTCDate()).padStart(2,"0");return`${e}-${i}-${r}`}function vE(t,n,e){return n instanceof Date&&(t==="min"||t==="max")&&(e==="date"||e==="month")?AF(n,e):n}function RF(t,n){t.listenToCustomControlModel(i=>n.state().controlValue.set(i)),t.listenToCustomControlOutput("touch",()=>n.state().markAsTouched()),n.registerAsBinding(t.customControl);let e=Vu();return()=>{let i=n.state(),r=i.controlValue();Yr(e,"controlValue",r)&&t.setCustomControlModelInput(r);for(let o of hv){let s;if(o==="errors"?s=n.errors():s=fv(i,o),Yr(e,o,s)&&(t.setInputOnDirectives(o,s),n.elementAcceptsNativeProperty(o)&&!t.customControlHasInput(o))){let a=vE(o,s,n.nativeFormElement.type);ol(n.renderer,n.nativeFormElement,o,a)}}}}function OF(t){return typeof t=="object"&&t!==null}function PF(t,n){let e=Vu();n.controlValueAccessor.registerOnChange(r=>{e.controlValue=r,n.state().controlValue.set(r)}),n.controlValueAccessor.registerOnTouched(()=>n.state().markAsTouched());let i=n.injector.get(Gr,null,{optional:!0,self:!0});if(i){let r;for(let l of i)OF(l)&&l.registerOnValidatorChange&&(r??=z(0),l.registerOnValidatorChange(()=>{r.update(c=>c+1)}));let o=i.map(l=>typeof l=="function"?l:l.validate.bind(l)),s=Wn.compose(o),a=E(()=>{r?.();let l=s?s(n.interopNgControl.control):null;return lE(l,n.interopNgControl.control)});n.parseErrorsSource.set(a)}return n.registerAsBinding({reset:()=>{let r=n.state().value();e.controlValue=r,B(()=>n.controlValueAccessor.writeValue(r))}}),()=>{let r=n.state(),o=r.controlValue();Yr(e,"controlValue",o)&&B(()=>n.controlValueAccessor.writeValue(o));for(let s of hv){let a=fv(r,s);if(Yr(e,s,a)){let l=t.setInputOnDirectives(s,a);s==="disabled"&&n.controlValueAccessor.setDisabledState?B(()=>n.controlValueAccessor.setDisabledState(a)):!l&&n.elementAcceptsNativeProperty(s)&&ol(n.renderer,n.nativeFormElement,s,a)}}}}function FF(t,n,e){if(typeof MutationObserver!="function")return;let i=new MutationObserver(r=>{r.some(o=>LF(o))&&n()});i.observe(t,{attributes:!0,attributeFilter:["value"],characterData:!0,childList:!0,subtree:!0}),e.onDestroy(()=>i.disconnect())}function LF(t){if(t.type==="childList"||t.type==="characterData"){if(t.target instanceof Comment)return!1;for(let n of t.addedNodes)if(!(n instanceof Comment))return!0;for(let n of t.removedNodes)if(!(n instanceof Comment))return!0;return!1}return t.type==="attributes"&&t.target instanceof HTMLOptionElement}function VF(t,n,e,i){let r=!1,o=n.nativeFormElement,s=TF(()=>n.state().value(),l=>n.state().controlValue.set(l),l=>NF(o,n.state().value,i));e.set(s.errors),n.onReset=()=>{s.reset();let l=n.state().value();a.controlValue=l,dE(o,l)},t.listenToDom("input",()=>s.setRawValue(void 0)),t.listenToDom("blur",()=>n.state().markAsTouched()),gE(o)&&kF(o)&&i.watchValidity(n.destroyRef,o,()=>s.setRawValue(void 0)),n.registerAsBinding(),o.tagName==="SELECT"&&FF(o,()=>{r&&(o.value=n.state().controlValue())},n.destroyRef);let a=Vu();return()=>{let l=n.state();for(let u of hv){let f=fv(l,u);if(Yr(a,u,f)&&(t.setInputOnDirectives(u,f),n.elementAcceptsNativeProperty(u))){let h=vE(u,f,o.type);ol(n.renderer,o,u,h)}}let c=l.controlValue();Yr(a,"controlValue",c)&&dE(o,c),r=!0}}var _E=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:e=>BF.\u0275fac(e),providedIn:"root"})}return t})(),BF=(()=>{class t extends _E{document=d(R);cspNonce=d(si,{optional:!0});injectedStyles=new WeakMap;watchValidity(e,i,r){let o=i.getRootNode();this.injectedStyles.has(o)||this.injectedStyles.set(o,this.createTransitionStyle(o));let s=a=>{let l=a;(l.animationName==="ng-valid"||l.animationName==="ng-invalid")&&r()};i.addEventListener("animationstart",s),e.onDestroy(()=>{i.removeEventListener("animationstart",s)})}isBadInput(e){return e.validity?.badInput??!1}createTransitionStyle(e){let i=this.document.createElement("style");return this.cspNonce&&(i.nonce=this.cspNonce),i.textContent=`
      @keyframes ng-valid {}
      @keyframes ng-invalid {}
      input:valid, textarea:valid {
        animation: ng-valid 0.001s;
      }
      input:invalid, textarea:invalid {
        animation: ng-invalid 0.001s;
      }
    `,e.nodeType===9?e.head?.appendChild(i):e.appendChild(i),i}ngOnDestroy(){this.injectedStyles.get(this.document)?.remove()}static \u0275fac=(()=>{let e;return function(r){return(e||(e=bt(t)))(r||t)}})();static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),jF=Symbol(),as=new g(""),Bu=(()=>{class t{field=ht.required({alias:"formField"});state=E(()=>this.field()());renderer=d(De);destroyRef=d(Ue);injector=d(P);element=d(k).nativeElement;elementIsNativeFormElement=Bg(this.element);elementAcceptsTextualValues=Vw(this.element);_elementAcceptsMinMax;nativeFormElement=this.elementIsNativeFormElement?this.element:void 0;focuser=e=>this.element.focus(e);controlValueAccessors=d(Wr,{optional:!0,self:!0});config=d(EF,{optional:!0});validityMonitor=d(_E);parseErrorsSource=z(void 0);_interopNgControl;get interopNgControl(){return this._interopNgControl??=new dv(this.state)}parseErrors=E(()=>this.parseErrorsSource()?.().map(e=>N(p({},e),{fieldTree:B(this.state).fieldTree,formField:this}))??[],{equal:Wt});errors=E(()=>this.state().errors().filter(e=>!e.formField||e.formField===this),{equal:Wt});isFieldBinding=!1;resetter=()=>{};parseErrorsResetCallback;setParseErrors(e){this.parseErrorsSource.set(e)}set onReset(e){this.parseErrorsResetCallback=e}get onReset(){return this.parseErrorsResetCallback}get controlValueAccessor(){return!this.controlValueAccessors||this.controlValueAccessors.length===0?this.interopNgControl?.valueAccessor??void 0:jg(this.interopNgControl,this.controlValueAccessors)??void 0}installClassBindingEffect(){let e=Object.entries(this.config?.classes??{}).map(([r,o])=>[r,E(()=>o(this))]);if(e.length===0)return;let i=Vu();Fo({write:()=>{for(let[r,o]of e){let s=o();Yr(i,r,s)&&(s?this.renderer.addClass(this.element,r):this.renderer.removeClass(this.element,r))}}},{injector:this.injector})}focus(e){this.focuser(e)}reset(){this.resetter(),this.parseErrorsResetCallback?.(this.state().value())}registerAsBinding(e){if(this.isFieldBinding)throw new y(1913,!1);this.isFieldBinding=!0,this.installClassBindingEffect(),e?.focus&&(this.focuser=i=>e.focus(i)),e?.reset&&(this.resetter=()=>e.reset()),et(i=>{let r=this.state();r.nodeState.formFieldBindings.update(o=>[...o,this]),i(()=>{r.nodeState.formFieldBindings.update(o=>o.filter(s=>s!==this))})},{injector:this.injector})}[jF];\u0275ngControlCreate(e){if(!e.hasPassThrough)if(this.controlValueAccessor)this.\u0275ngControlUpdate=PF(e,this);else if(e.customControl)this.\u0275ngControlUpdate=RF(e,this);else if(this.elementIsNativeFormElement)this.\u0275ngControlUpdate=VF(e,this,this.parseErrorsSource,this.validityMonitor);else throw new y(1914,!1)}\u0275ngControlUpdate;elementAcceptsNativeProperty(e){if(!this.elementIsNativeFormElement)return!1;switch(e){case"min":case"max":return this._elementAcceptsMinMax??=Lw(this.element);case"minLength":case"maxLength":return this.elementAcceptsTextualValues;case"disabled":case"required":case"readonly":case"name":return!0;default:return!1}}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","formField",""]],inputs:{field:[1,"formField","field"]},exportAs:["formField"],features:[Ae([{provide:as,useExisting:t},{provide:Hi,useFactory:()=>d(t).interopNgControl},{provide:jw,useFactory:()=>d(as,{self:!0})}]),kd("formField")]})}return t})();var Gn=class t{formatHsl(n,e,i){return`hsl(${n}, ${e*100}%, ${i*100}%)`}addHue(n,e){let i=n+e;return i-Math.floor(i/360)*360}hslToRgb(n,e,i){n/=360;let r,o,s;if(e===0)r=o=s=i;else{let a=(u,f,h)=>(h<0&&(h+=1),h>1&&(h-=1),h<.16666666666666666?u+(f-u)*6*h:h<.5?f:h<.6666666666666666?u+(f-u)*(.6666666666666666-h)*6:u),l=i<.5?i*(1+e):i+e-i*e,c=2*i-l;r=a(c,l,n+1/3),o=a(c,l,n),s=a(c,l,n-1/3)}return[Math.round(r*255),Math.round(o*255),Math.round(s*255)]}rgbToHex(n,e,i){return"#"+[n,e,i].map(r=>r.toString(16).padStart(2,"0")).join("")}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})};var cn=class t{utilsService=d(Gn);inc=z(0);state=z({name:"",colors:[]});stateForm=hl(this.state,n=>{Pu(n.colors,e=>{Pu(e.shades,i=>{pE(i,0),mE(i,1),fE(i,{when:({valueOf:r})=>r(e.lockShade)})})})});_=et(()=>{this._updateColors(this.state()),localStorage.setItem("currentColor",JSON.stringify(this.state()))});constructor(){let n=localStorage.getItem("currentColor")||"";if(!n)this.reset();else{let e=JSON.parse(n);this.inc.set(0),this.setColor(e.name||"unknown",(e.colors||[]).map((i,r)=>this._createColor(r===0,i)))}}setColor=(n,e)=>{this.state.set({name:n,colors:[...e]})};reset=()=>{this.inc.set(0),this.setColor("New Palette",[this._createColor(!0)])};addShade=n=>{let e=this.state().colors.find((i,r)=>r===n);e&&this.state.set(N(p({},this.state()),{colors:[...this.state().colors.map((i,r)=>r!=n?i:N(p({},e),{shades:[...e?.shades,.5]}))]}))};addColor=()=>{this.state.set(N(p({},this.state()),{colors:[...this.state().colors,this._createColor(!1)]}))};removeShade=(n,e)=>{let i=this.state().colors.find((r,o)=>o===n);i&&this.state.set(N(p({},this.state()),{colors:[...this.state().colors.map((r,o)=>o!=n?r:N(p({},i),{shades:[...i?.shades.filter((s,a)=>a!=e)]}))]}))};removeColor=n=>{this.state().colors.find((i,r)=>r===n)&&this.state.set(N(p({},this.state()),{colors:[...this.state().colors.filter((i,r)=>r!=n)]}))};_updateColors(n){let e=n.colors,i=e[0];if(e.length>1)for(let r=1;r<e.length;r++){let o=e[r];o.lockHue?o.hue=this.utilsService.addHue(i.hue,o.hueDiff):o.hueDiff=o.hue-i.hue,o.lockSaturation&&(o.saturation=i.saturation),o.lockShade&&(o.shades=[...i.shades])}}_createColor(n=!1,e={}){let r=(()=>{let o=this.inc();return this.inc.set(o+1),o})();return{name:e.name||`Color ${r}`,hue:e.hue||15,hueDiff:e.hueDiff||0,lockHue:e.lockHue||!n,saturation:e.saturation||.52,lockSaturation:e.lockSaturation||!n,shades:e.shades||[.23,.43,.63,.83],lockShade:e.lockShade||!n}}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})};var mv=class{_document;_textarea;constructor(n,e){this._document=e;let i=this._textarea=this._document.createElement("textarea"),r=i.style;r.position="fixed",r.top=r.opacity="0",r.left="-999em",i.setAttribute("aria-hidden","true"),i.value=n,i.readOnly=!0,(this._document.fullscreenElement||this._document.body).appendChild(i)}copy(){let n=this._textarea,e=!1;try{if(n){let i=this._document.activeElement;n.select(),n.setSelectionRange(0,n.value.length),e=this._document.execCommand("copy"),i&&i.focus()}}catch{}return e}destroy(){let n=this._textarea;n&&(n.remove(),this._textarea=void 0)}},UF=(()=>{class t{_document=d(R);copy(e){let i=this.beginCopy(e),r=i.copy();return i.destroy(),r}beginCopy(e){return new mv(e,this._document)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),HF=new g("CDK_COPY_TO_CLIPBOARD_CONFIG"),bE=(()=>{class t{_clipboard=d(UF);_ngZone=d(I);text="";attempts=1;copied=new U;_pending=new Set;_destroyed=!1;_currentTimeout;constructor(){let e=d(HF,{optional:!0});e&&e.attempts!=null&&(this.attempts=e.attempts)}copy(e=this.attempts){if(e=Math.min(e,50),e>1){let i=e,r=this._clipboard.beginCopy(this.text);this._pending.add(r);let o=()=>{let s=r.copy();!s&&--i&&!this._destroyed?this._currentTimeout=this._ngZone.runOutsideAngular(()=>setTimeout(o,1)):(this._currentTimeout=null,this._pending.delete(r),r.destroy(),this.copied.emit(s))};o()}else this.copied.emit(this._clipboard.copy(this.text))}ngOnDestroy(){this._currentTimeout&&clearTimeout(this._currentTimeout),this._pending.forEach(e=>e.destroy()),this._pending.clear(),this._destroyed=!0}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdkCopyToClipboard",""]],hostBindings:function(i,r){i&1&&G("click",function(){return r.copy()})},inputs:{text:[0,"cdkCopyToClipboard","text"],attempts:[0,"cdkCopyToClipboardAttempts","attempts"]},outputs:{copied:"cdkCopyToClipboardCopied"}})}return t})(),yE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({})}return t})();function pl(t){return t.buttons===0||t.detail===0}function gl(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var pv;function CE(){if(pv==null){let t=typeof document<"u"?document.head:null;pv=!!(t&&(t.createShadowRoot||t.attachShadow))}return pv}function gv(t){if(CE()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function vl(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function pt(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var vv;try{vv=typeof Intl<"u"&&Intl.v8BreakIterator}catch{vv=!1}var me=(()=>{class t{_platformId=d(Nr);isBrowser=this._platformId?JS(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||vv)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var _l;function SE(){if(_l==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>_l=!0}))}finally{_l=_l||!1}return _l}function ls(t){return SE()?t:!!t.capture}function _i(t,n=0){return DE(t)?Number(t):arguments.length===2?n:0}function DE(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function Gt(t){return t instanceof k?t.nativeElement:t}var wE=new g("cdk-input-modality-detector-options"),EE={ignoreKeys:[18,17,224,91,16]},xE=650,_v={passive:!0,capture:!0},IE=(()=>{class t{_platform=d(me);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Xe(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=pt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<xE||(this._modality.next(pl(e)?"keyboard":"mouse"),this._mostRecentTarget=pt(e))};_onTouchstart=e=>{if(gl(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=pt(e)};constructor(){let e=d(I),i=d(R),r=d(wE,{optional:!0});if(this._options=p(p({},EE),r),this.modalityDetected=this._modality.pipe(Hs(1)),this.modalityChanged=this.modalityDetected.pipe(vc()),this._platform.isBrowser){let o=d(Ze).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,_v),o.listen(i,"mousedown",this._onMousedown,_v),o.listen(i,"touchstart",this._onTouchstart,_v)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),bl=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(bl||{}),TE=new g("cdk-focus-monitor-default-options"),ju=ls({passive:!0,capture:!0}),Zr=(()=>{class t{_ngZone=d(I);_platform=d(me);_inputModalityDetector=d(IE);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(R);_stopInputModalityDetector=new S;constructor(){let e=d(TE,{optional:!0});this._detectionMode=e?.detectionMode||bl.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=pt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=Gt(e);if(!this._platform.isBrowser||r.nodeType!==1)return H();let o=gv(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new S,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=Gt(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=Gt(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,l])=>this._originChanged(a,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===bl.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===bl.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?xE:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=pt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,ju),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,ju)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(be(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,ju),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,ju),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var Uu=new WeakMap,Ke=(()=>{class t{_appRef;_injector=d(P);_environmentInjector=d(we);load(e){let i=this._appRef=this._appRef||this._injector.get(Ht),r=Uu.get(i);r||(r={loaders:new Set,refs:[]},Uu.set(i,r),i.onDestroy(()=>{Uu.get(i)?.refs.forEach(o=>o.destroy()),Uu.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(zd(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var cs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})(),Hu;function $F(){if(Hu===void 0&&(Hu=null,typeof window<"u")){let t=window;if(t.trustedTypes!==void 0)try{Hu=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n})}catch(n){console.error(n)}}return Hu}function Kr(t){return $F()?.createHTML(t)||t}function ME(t,n,e){let i=e.sanitize(nt.HTML,n);t.innerHTML=Kr(i||"")}function ds(t){return Array.isArray(t)?t:[t]}var NE=new Set,Xr,us=(()=>{class t{_platform=d(me);_nonce=d(si,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):GF}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&WF(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function WF(t,n){if(!NE.has(t))try{Xr||(Xr=document.createElement("style"),n&&Xr.setAttribute("nonce",n),Xr.setAttribute("type","text/css"),document.head.appendChild(Xr)),Xr.sheet&&(Xr.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),NE.add(t))}catch(e){console.error(e)}}function GF(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var bv=(()=>{class t{_mediaMatcher=d(us);_zone=d(I);_queries=new Map;_destroySubject=new S;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return kE(ds(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=kE(ds(e)).map(s=>this._registerQuery(s).observable),o=Vs(r);return o=xi(o.pipe(ze(1)),o.pipe(Hs(1),js(0))),o.pipe(ie(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:l,query:c})=>{a.matches=a.matches||l,a.breakpoints[c]=l}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new J(s=>{let a=l=>this._zone.run(()=>s.next(l));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(It(i),ie(({matches:s})=>({query:e,matches:s})),be(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function kE(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var qF=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var AE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({providers:[qF]})}return t})();var Cv=(()=>{class t{_platform=d(me);isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return ZF(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=YF(i1(e));if(i&&(RE(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=RE(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!t1(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return n1(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function YF(t){try{return t.frameElement}catch{return null}}function ZF(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function KF(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function XF(t){return JF(t)&&t.type=="hidden"}function QF(t){return e1(t)&&t.hasAttribute("href")}function JF(t){return t.nodeName.toLowerCase()=="input"}function e1(t){return t.nodeName.toLowerCase()=="a"}function OE(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function RE(t){if(!OE(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function t1(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function n1(t){return XF(t)?!1:KF(t)||QF(t)||t.hasAttribute("contenteditable")||OE(t)}function i1(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var yv=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,s){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){rt(n,{injector:this._injector})}},Sv=(()=>{class t{_checker=d(Cv);_ngZone=d(I);_document=d(R);_injector=d(P);constructor(){d(Ke).load(cs)}create(e,i=!1){return new yv(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var PE=new g("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),FE=new g("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),r1=0,yl=(()=>{class t{_ngZone=d(I);_defaultOptions=d(FE,{optional:!0});_liveElement;_document=d(R);_sanitizer=d(Va);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(PE,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:ME(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${r1++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var o1=200,zu=class{_letterKeyStream=new S;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new S;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:o1;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(tt(e=>this._pressedLetters.push(e)),js(n),_e(()=>this._pressedLetters.length>0),ie(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function Dt(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var $u=class{_items;_activeItemIndex=z(-1);_activeItem=z(null);_wrap=!1;_typeaheadSubscription=pe.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Or?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Ut(n)&&(this._effectRef=et(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new S;change=new S;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new zu(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||Dt(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Ut(this._items)?this._items():this._items instanceof Or?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Cl=class extends $u{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var jE=new Map,je=class t{_appId=d(mn);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=jE.get(n);return i===void 0?i=0:i++,jE.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})};var HE=" ";function a1(t,n,e){let i=Gu(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(HE)))}function l1(t,n,e){let i=Gu(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(HE)):t.removeAttribute(n)}function Gu(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var zE="cdk-describedby-message",Wu="cdk-describedby-host",xv=0,$E=(()=>{class t{_platform=d(me);_document=d(R);_messageRegistry=new Map;_messagesContainer=null;_id=`${xv++}`;constructor(){d(Ke).load(cs),this._id=d(mn)+"-"+xv++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=Ev(i,r);typeof i!="string"?(UE(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=Ev(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let s=this._messageRegistry.get(o);s&&s.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${Wu}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(Wu);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");UE(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(Ev(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=Gu(e,"aria-describedby").filter(r=>r.indexOf(zE)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);a1(e,"aria-describedby",r.messageElement.id),e.setAttribute(Wu,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,l1(e,"aria-describedby",r.messageElement.id),e.removeAttribute(Wu)}_isElementDescribedByMessage(e,i){let r=Gu(e,"aria-describedby"),o=this._messageRegistry.get(i),s=o&&o.messageElement.id;return!!s&&r.indexOf(s)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function Ev(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function UE(t,n){t.id||(t.id=`${zE}-${n}-${xv++}`)}var WE={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};function Iv(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}function qe(t){return t==null?"":typeof t=="string"?t:`${t}px`}var c1=new g("cdk-dir-doc",{providedIn:"root",factory:()=>d(R)}),d1=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function GE(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?d1.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var lt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=z("ltr");change=new U;constructor(){let e=d(c1,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(GE(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var En=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(En||{}),qu,Qr;function Yu(){if(Qr==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Qr=!1,Qr;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Qr=!0;else{let t=Element.prototype.scrollTo;t?Qr=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Qr=!1}}return Qr}function fs(){if(typeof document!="object"||!document)return En.NORMAL;if(qu==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),qu=En.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,qu=t.scrollLeft===0?En.NEGATED:En.INVERTED),t.remove()}return qu}var Ie=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({})}return t})();var u1=20,Jr=(()=>{class t{_ngZone=d(I);_platform=d(me);_renderer=d(Ze).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new S;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=u1){return this._platform.isBrowser?new J(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(gc(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):H()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(_e(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=Gt(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Tv=(()=>{class t{elementRef=d(k);scrollDispatcher=d(Jr);ngZone=d(I);dir=d(lt,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new S;_renderer=d(De);_cleanupScroll;_elementScrolled=new S;ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&fs()!=En.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),fs()==En.INVERTED?e.left=e.right:fs()==En.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;Yu()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let s=this.dir&&this.dir.value=="rtl";return e=="start"?e=s?r:i:e=="end"&&(e=s?i:r),s&&fs()==En.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:s&&fs()==En.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),f1=20,qi=(()=>{class t{_platform=d(me);_listeners;_viewportSize=null;_change=new S;_document=d(R);constructor(){let e=d(I),i=d(Ze).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=f1){return e>0?this._change.pipe(gc(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var Sl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({})}return t})(),Mv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[Ie,Sl,Ie,Sl]})}return t})();var Dl=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},xn=class extends Dl{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,s){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},bi=class extends Dl{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Nv=class extends Dl{element;constructor(n){super(),this.element=n instanceof k?n.nativeElement:n}},Yi=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof xn)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof bi)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Nv)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Zu=class extends Yi{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(Un,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||P.NULL,o=r.get(we,i.injector);e=zd(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var Zi=(()=>{class t extends Yi{_moduleRef=d(Un,{optional:!0});_document=d(R);_viewContainerRef=d(Mt);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new U;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=(()=>{let e;return function(r){return(e||(e=bt(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[Ne]})}return t})(),wl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({})}return t})();var qE=Yu();function ps(t){return new Ku(t.get(qi),t.get(R))}var Ku=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=qe(-this._previousScrollPosition.left),n.style.top=qe(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),qE&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),qE&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function e0(t,n){return new Xu(t.get(Jr),t.get(I),t.get(qi),n)}var Xu=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(_e(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var El=class{enable(){}disable(){}attach(){}};function kv(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function YE(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function no(t,n){return new Qu(t.get(Jr),t.get(qi),t.get(I),n)}var Qu=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();kv(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},t0=(()=>{class t{_injector=d(P);noop=()=>new El;close=e=>e0(this._injector,e);block=()=>ps(this._injector);reposition=e=>no(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),yi=class{positionStrategy;scrollStrategy=new El;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Ju=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var n0=(()=>{class t{_attachedOverlays=[];_document=d(R);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),i0=(()=>{class t extends n0{_ngZone=d(I);_renderer=d(Ze).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),r0=(()=>{class t extends n0{_platform=d(me);_ngZone=d(I);_renderer=d(Ze).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=pt(e)};_clickListener=e=>{let i=pt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],l=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,l))){if(ZE(a.overlayElement,i)||ZE(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function ZE(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var o0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),nf=(()=>{class t{_platform=d(me);_containerElement;_document=d(R);_styleLoader=d(Ke);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Iv()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Iv()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(o0)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Av=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Rv(t){return t&&t.nodeType===1}var hs=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new S;_attachments=new S;_detachments=new S;_positionStrategy;_scrollStrategy;_locationChanges=pe.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new S;_outsidePointerEvents=new S;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,l,c,u=!1,f,h){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=f,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=rt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=p(p({},this._config),n),this._updateElementSize()}setDirection(n){this._config=N(p({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=qe(this._config.width),n.height=qe(this._config.height),n.minWidth=qe(this._config.minWidth),n.minHeight=qe(this._config.minHeight),n.maxWidth=qe(this._config.maxWidth),n.maxHeight=qe(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Rv(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Av(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=ds(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=rt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},KE="cdk-overlay-connected-position-bounding-box",m1=/([A-Za-z%]+)$/;function xl(t,n){return new ef(n,t.get(qi),t.get(R),t.get(me),t.get(nf))}var ef=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new S;_resizeSubscription=pe.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(KE),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let l=this._getOriginPoint(n,r,a),c=this._getOverlayPoint(l,e,a),u=this._getOverlayFit(c,e,i,a);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:a,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,a)});continue}(!s||s.overlayFit.visibleArea<u.visibleArea)&&(s={overlayFit:u,overlayPoint:c,originPoint:l,position:a,overlayRect:e})}if(o.length){let a=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,a=c)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&eo(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(KE),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof k?this._origin.nativeElement:Rv(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=QE(e),{x:s,y:a}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(s+=l),c&&(a+=c);let u=0-s,f=s+o.width-i.width,h=0-a,m=a+o.height-i.height,v=this._subtractOverflows(o.width,u,f),D=this._subtractOverflows(o.height,h,m),A=v*D;return{visibleArea:A,isCompletelyWithinViewport:o.width*o.height===A,fitsInViewportVertically:D===o.height,fitsInViewportHorizontally:v==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=XE(this._overlayRef.getConfig().minHeight),a=XE(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||s!=null&&s<=r,c=n.fitsInViewportHorizontally||a!=null&&a<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=QE(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,f=0;return r.width<=o.width?u=c||-s:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=l||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:f},{x:n.x+u,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!p1(this._lastScrollVisibility,i)){let r=new Ju(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let m=Math.min(i.bottom-n.y+i.top,n.y),v=this._lastBoundingBoxSize.height;o=m*2,s=n.y-m,o>v&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-v/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,f,h;if(c)h=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)f=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let m=Math.min(i.right-n.x+i.left,n.x),v=this._lastBoundingBoxSize.width;u=m*2,f=n.x-m,u>v&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-v/2)}return{top:s,left:f,bottom:a,right:h,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=qe(i.width),r.height=qe(i.height),r.top=qe(i.top)||"auto",r.bottom=qe(i.bottom)||"auto",r.left=qe(i.left)||"auto",r.right=qe(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=qe(o)),s&&(r.maxWidth=qe(s))}this._lastBoundingBoxSize=i,eo(this._boundingBox.style,r)}_resetBoundingBoxStyles(){eo(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){eo(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();eo(i,this._getExactOverlayY(e,n,u)),eo(i,this._getExactOverlayX(e,n,u))}else i.position="static";let a="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(a+=`translateX(${l}px) `),c&&(a+=`translateY(${c}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=qe(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=qe(s.maxWidth):o&&(i.maxWidth="")),eo(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=qe(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=qe(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:YE(n,i),isOriginOutsideView:kv(n,i),isOverlayClipped:YE(e,i),isOverlayOutsideView:kv(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&ds(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof k)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function eo(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function XE(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(m1);return!e||e==="px"?parseFloat(n):null}return t||null}function QE(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function p1(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var JE="cdk-global-overlay-wrapper";function Ki(t){return new tf}var tf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(JE),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,l=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),c=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),u=this._xPosition,f=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",m="",v="",D="";l?D="flex-start":u==="center"?(D="center",h?v=f:m=f):h?u==="left"||u==="end"?(D="flex-end",m=f):(u==="right"||u==="start")&&(D="flex-start",v=f):u==="left"||u==="start"?(D="flex-start",m=f):(u==="right"||u==="end")&&(D="flex-end",v=f),n.position=this._cssPosition,n.marginLeft=l?"0":m,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":v,e.justifyContent=D,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(JE),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},s0=(()=>{class t{_injector=d(P);global(){return Ki()}flexibleConnectedTo(e){return xl(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),Il=new g("OVERLAY_DEFAULT_CONFIG");function Ci(t,n){t.get(Ke).load(o0);let e=t.get(nf),i=t.get(R),r=t.get(je),o=t.get(Ht),s=t.get(lt),a=t.get(De,null,{optional:!0})||t.get(Ze).createRenderer(null,null),l=new yi(n),c=t.get(Il,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||s.value,!i.body||!("showPopover"in i.body)?l.usePopover=!1:l.usePopover=n?.usePopover??c;let u=i.createElement("div"),f=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),f.appendChild(u),l.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let h=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return Rv(h)?h.after(f):h?.type==="parent"?h.element.appendChild(f):e.getContainerElement().appendChild(f),new hs(new Zu(u,o,t),f,u,l,t.get(I),t.get(i0),i,t.get(Vi),t.get(r0),n?.disableAnimations??t.get(aa,null,{optional:!0})==="NoopAnimations",t.get(we),a)}var a0=(()=>{class t{scrollStrategies=d(t0);_positionBuilder=d(s0);_injector=d(P);create(e){return Ci(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),g1=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],v1=new g("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(P);return()=>no(t)}}),ms=(()=>{class t{elementRef=d(k);static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),l0=new g("cdk-connected-overlay-default-config"),rf=(()=>{class t{_dir=d(lt,{optional:!0});_injector=d(P);_overlayRef;_templatePortal;_backdropSubscription=pe.EMPTY;_attachSubscription=pe.EMPTY;_detachSubscription=pe.EMPTY;_positionSubscription=pe.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(v1);_ngZone=d(I);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new U;positionChange=new U;attach=new U;detach=new U;overlayKeydown=new U;overlayOutsideClick=new U;constructor(){let e=d(Pt),i=d(Mt),r=d(l0,{optional:!0}),o=d(Il,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new bi(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=g1);let e=this._overlayRef=Ci(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!Dt(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=pt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new yi({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=xl(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof ms?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof ms?this.origin.elementRef.nativeElement:this.origin instanceof k?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Zf(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",Q],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",Q],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",Q],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",Q],push:[2,"cdkConnectedOverlayPush","push",Q],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",Q],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",Q],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Ge]})}return t})(),gs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({providers:[a0],imports:[Ie,wl,Mv,Mv]})}return t})();var vs,c0=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Ov(){if(vs)return vs;if(typeof document!="object"||!document)return vs=new Set(c0),vs;let t=document.createElement("input");return vs=new Set(c0.filter(n=>(t.setAttribute("type",n),t.type===n))),vs}var _1=new g("MATERIAL_ANIMATIONS"),d0=null;function b1(){return d(_1,{optional:!0})?.animationsDisabled||d(aa,{optional:!0})==="NoopAnimations"?"di-disabled":(d0??=d(us).matchMedia("(prefers-reduced-motion)").matches,d0?"reduced-motion":"enabled")}function Re(){return b1()!=="enabled"}function Si(t){return t!=null&&`${t}`!="false"}var At=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(At||{}),Pv=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=At.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},u0=ls({passive:!0,capture:!0}),Fv=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,u0)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,u0)))}_delegateEventHandler=n=>{let e=pt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Tl={enterDuration:225,exitDuration:150},y1=800,f0=ls({passive:!0,capture:!0}),h0=["mousedown","touchstart"],m0=["mouseup","mouseleave","touchend","touchcancel"],C1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--%NS%mat-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),Ml=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Fv;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=Gt(i)),o&&o.get(Ke).load(C1)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=p(p({},Tl),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||S1(n,e,r),a=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${a-s}px`,u.style.top=`${l-s}px`,u.style.height=`${s*2}px`,u.style.width=`${s*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let f=window.getComputedStyle(u),h=f.transitionProperty,m=f.transitionDuration,v=h==="none"||m==="0s"||m==="0s, 0s"||r.width===0&&r.height===0,D=new Pv(this,u,i,v);u.style.transform="scale3d(1, 1, 1)",D.state=At.FADING_IN,i.persistent||(this._mostRecentTransientRipple=D);let A=null;return!v&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let ne=()=>{A&&(A.fallbackTimer=null),clearTimeout(Ye),this._finishRippleTransition(D)},ke=()=>this._destroyRipple(D),Ye=setTimeout(ke,c+100);u.addEventListener("transitionend",ne),u.addEventListener("transitioncancel",ke),A={onTransitionEnd:ne,onTransitionCancel:ke,fallbackTimer:Ye}}),this._activeRipples.set(D,A),(v||!c)&&this._finishRippleTransition(D),D}fadeOutRipple(n){if(n.state===At.FADING_OUT||n.state===At.HIDDEN)return;let e=n.element,i=p(p({},Tl),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=At.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=Gt(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,h0.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{m0.forEach(e=>{this._triggerElement.addEventListener(e,this,f0)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===At.FADING_IN?this._startFadeOutTransition(n):n.state===At.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=At.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=At.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=pl(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+y1;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!gl(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===At.VISIBLE||n.config.terminateOnPointerUp&&n.state===At.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(h0.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(m0.forEach(e=>n.removeEventListener(e,this,f0)),this._pointerUpEventsRegistered=!1))}};function S1(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Nl=new g("mat-ripple-global-options"),io=(()=>{class t{_elementRef=d(k);_animationsDisabled=Re();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(I),i=d(me),r=d(Nl,{optional:!0}),o=d(P);this._globalOptions=r||{},this._rippleRenderer=new Ml(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:p(p(p({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,p(p({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,p(p({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var D1={capture:!0},w1=["focus","mousedown","mouseenter","touchstart"],Lv="mat-ripple-loader-uninitialized",Vv="mat-ripple-loader-class-name",p0="mat-ripple-loader-centered",of="mat-ripple-loader-disabled",g0=(()=>{class t{_document=d(R);_animationsDisabled=Re();_globalRippleOptions=d(Nl,{optional:!0});_platform=d(me);_ngZone=d(I);_injector=d(P);_eventCleanups;_hosts=new Map;constructor(){let e=d(Ze).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>w1.map(i=>e.listen(this._document,i,this._onInteraction,D1)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Lv,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Vv))&&e.setAttribute(Vv,i.className||""),i.centered&&e.setAttribute(p0,""),i.disabled&&e.setAttribute(of,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(of,""):e.removeAttribute(of)}_onInteraction=e=>{let i=pt(e);if(i instanceof HTMLElement){let r=i.closest(`[${Lv}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Vv)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Tl.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Tl.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(of),rippleConfig:{centered:e.hasAttribute(p0),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},l=new Ml(a,this._ngZone,i,this._platform,this._injector),c=!a.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:l,hasSetUpEvents:c}),e.removeAttribute(Lv)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var Xi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--%NS%mat-focus-indicator-display, none);
  border-width: var(--%NS%mat-focus-indicator-border-width, 3px);
  border-style: var(--%NS%mat-focus-indicator-border-style, solid);
  border-color: var(--%NS%mat-focus-indicator-border-color, transparent);
  border-radius: var(--%NS%mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --%NS%mat-focus-indicator-display: block;
    --%NS%mat-focus-indicator-fallback-border-style: none;
  }
}
`],encapsulation:2})}return t})();var E1=["*",[["","progressIndicator",""]]],x1=["*","[progressIndicator]"];function I1(t,n){t&1&&(ut(0,"div",1),le(1,1),yt())}var T1=new g("MAT_BUTTON_CONFIG");function v0(t){return t==null?void 0:St(t)}var Bv=(()=>{class t{_elementRef=d(k);_ngZone=d(I);_animationsDisabled=Re();_config=d(T1,{optional:!0});_focusMonitor=d(Zr);_cleanupClick;_renderer=d(De);_rippleLoader=d(g0);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=ht(!1,{transform:Q});constructor(){d(Ke).load(Xi);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(ve("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),ft(r.color?"mat-"+r.color:""),Z("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",Q],disabled:[2,"disabled","disabled",Q],ariaDisabled:[2,"aria-disabled","ariaDisabled",Q],disabledInteractive:[2,"disabledInteractive","disabledInteractive",Q],tabIndex:[2,"tabIndex","tabIndex",v0],_tabindex:[2,"tabindex","_tabindex",v0],showProgress:[1,"showProgress"]}})}return t})(),jv=(()=>{class t extends Bv{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[Ne],ngContentSelectors:x1,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(xe(E1),nn(0,"span",0),le(1),ee(2,I1,2,0,"div",1),nn(3,"span",2)(4,"span",3)),i&2&&(C(2),te(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--%NS%mat-icon-button-state-layer-size, 40px);
  height: var(--%NS%mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--%NS%mat-icon-button-state-layer-size, 40px) - var(--%NS%mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--%NS%mat-icon-button-icon-size, 24px);
  color: var(--%NS%mat-icon-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--%NS%mat-icon-button-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface-variant) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-icon-button-touch-target-size, 48px);
  display: var(--%NS%mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--%NS%mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--%NS%mat-icon-button-icon-size, 24px);
  height: var(--%NS%mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();var _s=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[Ie]})}return t})();var M1=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],N1=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function k1(t,n){t&1&&(ut(0,"div",2),le(1,3),yt())}var _0=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Qi=(()=>{class t extends Bv{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=A1(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?_0.get(this._appearance):null,o=_0.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Ne],ngContentSelectors:N1,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(xe(M1),nn(0,"span",0),le(1),ut(2,"span",1),le(3,1),yt(),le(4,2),ee(5,k1,2,0,"div",2),nn(6,"span",3)(7,"span",4)),i&2&&(Z("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),C(5),te(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--%NS%mat-button-text-horizontal-padding, 12px);
  height: var(--%NS%mat-button-text-container-height, 40px);
  font-family: var(--%NS%mat-button-text-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-text-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-text-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-text-label-text-transform);
  font-weight: var(--%NS%mat-button-text-label-text-weight, var(--%NS%mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-text-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--%NS%mat-button-text-label-text-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--%NS%mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-text-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-text-touch-target-size, 48px);
  display: var(--%NS%mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-filled-container-height, 40px);
  font-family: var(--%NS%mat-button-filled-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-filled-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-filled-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-filled-label-text-transform);
  font-weight: var(--%NS%mat-button-filled-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-filled-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-state-layer-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-filled-touch-target-size, 48px);
  display: var(--%NS%mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--%NS%mat-button-filled-label-text-color, var(--%NS%mat-sys-on-primary));
  background-color: var(--%NS%mat-button-filled-container-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-filled-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --%NS%mat-progress-spinner-active-indicator-color: var(--%NS%mat-button-filled-progress-active-indicator-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--%NS%mat-button-protected-container-elevation-shadow, var(--%NS%mat-sys-level1));
  height: var(--%NS%mat-button-protected-container-height, 40px);
  font-family: var(--%NS%mat-button-protected-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-protected-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-protected-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-protected-label-text-transform);
  font-weight: var(--%NS%mat-button-protected-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-protected-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-protected-touch-target-size, 48px);
  display: var(--%NS%mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--%NS%mat-button-protected-label-text-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-button-protected-container-color, var(--%NS%mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-protected-container-shape, var(--%NS%mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--%NS%mat-button-protected-hover-container-elevation-shadow, var(--%NS%mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--%NS%mat-button-protected-focus-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--%NS%mat-button-protected-pressed-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-protected-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--%NS%mat-button-protected-disabled-container-elevation-shadow, var(--%NS%mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-outlined-container-height, 40px);
  font-family: var(--%NS%mat-button-outlined-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-outlined-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-outlined-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-outlined-label-text-transform);
  font-weight: var(--%NS%mat-button-outlined-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  border-radius: var(--%NS%mat-button-outlined-container-shape, var(--%NS%mat-sys-corner-full));
  border-width: var(--%NS%mat-button-outlined-outline-width, 1px);
  padding: 0 var(--%NS%mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-outlined-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-outlined-touch-target-size, 48px);
  display: var(--%NS%mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--%NS%mat-button-outlined-label-text-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-button-outlined-outline-color, var(--%NS%mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: var(--%NS%mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-tonal-container-height, 40px);
  font-family: var(--%NS%mat-button-tonal-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-tonal-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-tonal-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-tonal-label-text-transform);
  font-weight: var(--%NS%mat-button-tonal-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--%NS%mat-button-tonal-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-tonal-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-tonal-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-tonal-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-secondary-container) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-tonal-touch-target-size, 48px);
  display: var(--%NS%mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();function A1(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Uv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[_s,Ie]})}return t})();function O1(t,n){if(t&1){let e=rn();b(0,"div",1)(1,"button",2),G("click",function(){Bt(e);let r=re();return jt(r.action())}),X(2),_()()}if(t&2){let e=re();C(2),yn(" ",e.data.action," ")}}var P1=["label"];function F1(t,n){}var L1=Math.pow(2,31)-1,kl=class{_overlayRef;instance;containerInstance;_afterDismissed=new S;_afterOpened=new S;_onAction=new S;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,L1))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},b0=new g("MatSnackBarData"),bs=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},V1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),B1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),j1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),U1=(()=>{class t{snackBarRef=d(kl);data=d(b0);action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(b(0,"div",0),X(1),_(),ee(2,O1,3,1,"div",1)),i&2&&(C(),yn(" ",r.data.message,`
`),C(),te(r.hasAction?2:-1))},dependencies:[Qi,V1,B1,j1],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2})}return t})(),Hv="_mat-snack-bar-enter",zv="_mat-snack-bar-exit",H1=(()=>{class t extends Yi{_ngZone=d(I);_elementRef=d(k);_changeDetectorRef=d(Fe);_platform=d(me);_animationsDisabled=Re();snackBarConfig=d(bs);_document=d(R);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(P);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new S;_onExit=new S;_onEnter=new S;_animationState="void";_live;_label;_role;_liveElementId=d(je).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===zv?this._completeExit():e===Hv&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?rt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Hv)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(Hv)},200)))}exit(){return this._destroyed?H(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?rt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(zv)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(zv),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&Pe(Zi,7)(P1,7),i&2){let o;q(o=Y())&&(r._portalOutlet=o.first),q(o=Y())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&G("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&Z("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[Ne],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(b(0,"div",1)(1,"div",2,0)(3,"div",3),Nt(4,F1,0,0,"ng-template",4),_(),O(5,"div"),_()()),i&2&&(C(5),ve("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[Zi],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--%NS%mat-snack-bar-supporting-text-color, var(--%NS%mat-sys-inverse-on-surface));
  border-radius: var(--%NS%mat-snack-bar-container-shape, var(--%NS%mat-sys-corner-extra-small));
  background-color: var(--%NS%mat-snack-bar-container-color, var(--%NS%mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--%NS%mat-snack-bar-supporting-text-font, var(--%NS%mat-sys-body-medium-font));
  font-size: var(--%NS%mat-snack-bar-supporting-text-size, var(--%NS%mat-sys-body-medium-size));
  font-weight: var(--%NS%mat-snack-bar-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight));
  line-height: var(--%NS%mat-snack-bar-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--%NS%mat-snack-bar-button-color, var(--%NS%mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --%NS%mat-button-text-state-layer-color: currentColor;
  --%NS%mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2,changeDetection:1})}return t})(),z1=new g("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new bs}),ys=(()=>{class t{_live=d(yl);_injector=d(P);_breakpointObserver=d(bv);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(z1);_animationsDisabled=Re();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=U1;snackBarContainerComponent=H1;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=p(p({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=P.create({parent:r||this._injector,providers:[{provide:bs,useValue:i}]}),s=new xn(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=p(p(p({},new bs),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new kl(s,o);if(e instanceof Pt){let l=new bi(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(l)}else{let l=this._createInjector(r,a),c=new xn(e,void 0,l),u=s.attachComponentPortal(c);a.instance=u.instance}return this._breakpointObserver.observe(WE.HandsetPortrait).pipe(be(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new yi;i.direction=e.direction;let r=Ki(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Ci(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return P.create({parent:r||this._injector,providers:[{provide:kl,useValue:i},{provide:b0,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var $1=["tooltip"],W1=20;var G1=new g("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(P);return()=>no(t,{scrollThrottle:W1})}}),q1=new g("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var y0="tooltip-panel",Y1={passive:!0},Z1=8,K1=8,X1=24,Q1=200,$v=(()=>{class t{_elementRef=d(k);_ngZone=d(I);_platform=d(me);_ariaDescriber=d($E);_focusMonitor=d(Zr);_dir=d(lt);_injector=d(P);_viewContainerRef=d(Mt);_mediaMatcher=d(us);_document=d(R);_renderer=d(De);_animationsDisabled=Re();_defaultOptions=d(q1,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=C0;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=Si(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=Si(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=_i(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=_i(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new S;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=Z1}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(be(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new xn(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(be(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let s=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&s._origin instanceof k)return this._overlayRef;this._detach()}let i=this._injector.get(Jr).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${y0}`,o=xl(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(be(this._destroyed)).subscribe(s=>{this._updateCurrentPositionClass(s.connectionPair),this._tooltipInstance&&s.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=Ci(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(G1)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(be(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(be(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(be(this._destroyed)).subscribe(s=>{s.preventDefault(),s.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(be(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(p(p({},r.main),o.main)),this._addOffset(p(p({},r.fallback),o.fallback))])}_addOffset(e){let i=K1,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:s}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:s}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:s}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:s}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),rt(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,s;if(i==="center"?this._dir&&this._dir.value==="rtl"?s=r==="end"?"left":"right":s=r==="start"?"left":"right":s=i==="bottom"&&o==="top"?"above":"below",s!==this._currentPosition){let a=this._overlayRef;if(a){let l=`${this._cssClassPrefix}-${y0}-`;a.removePanelClass(l+this._currentPosition),a.addPanelClass(l+s)}this._currentPosition=s}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,Y1))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||rt({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!Dt(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),C0=(()=>{class t{_changeDetectorRef=d(Fe);_elementRef=d(k);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Re();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new S;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>X1&&e.width>=Q1}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let s=getComputedStyle(i);(s.getPropertyValue("animation-duration")==="0s"||s.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&Pe($1,7),i&2){let o;q(o=Y())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&G("mouseleave",function(s){return r._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(ut(0,"div",1,0),Rd("animationend",function(s){return r._handleAnimationEnd(s)}),ut(2,"div",2),X(3),yt()()),i&2&&(ft(r.tooltipClass),Z("mdc-tooltip--multiline",r._isMultiline),C(3),Ct(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--%NS%mat-tooltip-container-color, var(--%NS%mat-sys-inverse-surface));
  color: var(--%NS%mat-tooltip-supporting-text-color, var(--%NS%mat-sys-inverse-on-surface));
  border-radius: var(--%NS%mat-tooltip-container-shape, var(--%NS%mat-sys-corner-extra-small));
  font-family: var(--%NS%mat-tooltip-supporting-text-font, var(--%NS%mat-sys-body-small-font));
  font-size: var(--%NS%mat-tooltip-supporting-text-size, var(--%NS%mat-sys-body-small-size));
  font-weight: var(--%NS%mat-tooltip-supporting-text-weight, var(--%NS%mat-sys-body-small-weight));
  line-height: var(--%NS%mat-tooltip-supporting-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  letter-spacing: var(--%NS%mat-tooltip-supporting-text-tracking, var(--%NS%mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2})}return t})();var Cs=class t{utilsService=d(Gn);snackBar=d(ys);hue=ht.required();saturation=ht.required();light=ht.required();color=E(()=>this.utilsService.formatHsl(this.hue(),this.saturation(),this.light()));hexColor=E(()=>{let n=this.utilsService.hslToRgb(this.hue(),this.saturation(),this.light());return this.utilsService.rgbToHex(n[0],n[1],n[2])});onCopied(){this.snackBar.open("Color copied to clipboard","Close",{duration:5e3})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-copy"]],hostVars:2,hostBindings:function(e,i){e&2&&on("background-color",i.color())},inputs:{hue:[1,"hue"],saturation:[1,"saturation"],light:[1,"light"]},decls:1,vars:2,consts:[[3,"cdkCopyToClipboardCopied","cdkCopyToClipboard","matTooltip"]],template:function(e,i){e&1&&(b(0,"button",0),G("cdkCopyToClipboardCopied",function(){return i.onCopied()}),_()),e&2&&j("cdkCopyToClipboard",i.hexColor())("matTooltip",i.hexColor())},dependencies:[yE,bE,$v],styles:["[_nghost-%COMP%]{height:5vw;width:5vw;display:block}button[_ngcontent-%COMP%]{height:100%;width:100%;background:none;border:0;cursor:pointer}button[_ngcontent-%COMP%]:hover + .tooltip[_ngcontent-%COMP%]{opacity:1}.tooltip[_ngcontent-%COMP%]{pointer-events:none;opacity:0;transition:opacity .4s}"]})};var sf=class t{utilsService=d(Gn);hue=ht.required();saturation=ht.required();light=ht.required();position=E(()=>{let n=Math.PI/180*this.hue(),e=Math.cos(n),i=Math.sin(n),r=this.light(),o=0;return[r*e-o*i,r*i+o*e]});left=E(()=>{var n=(this.position()[0]+1)*50;return n+"%"});top=E(()=>{var n=(this.position()[1]+1)*50;return n+"%"});static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-pointer"]],hostVars:4,hostBindings:function(e,i){e&2&&on("left",i.left())("top",i.top())},inputs:{hue:[1,"hue"],saturation:[1,"saturation"],light:[1,"light"]},decls:1,vars:3,consts:[[3,"hue","saturation","light"]],template:function(e,i){e&1&&O(0,"pbu-color-copy",0),e&2&&j("hue",i.hue())("saturation",i.saturation())("light",i.light())},dependencies:[Cs],styles:["[_nghost-%COMP%]{border-radius:50%;width:30px;height:30px;display:block;border:2px solid #fff;outline:1px solid #cacaca;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%)}pbu-color-copy[_ngcontent-%COMP%]{height:100%;width:100%;border-radius:50%}"]})};var J1=["colorWheel"];function eL(t,n){if(t&1&&O(0,"pbu-color-pointer",1),t&2){let e=n.$implicit,i=re().$implicit;j("hue",i.hue)("saturation",i.saturation)("light",e)}}function tL(t,n){if(t&1&&st(0,eL,1,3,"pbu-color-pointer",1,_n),t&2){let e=n.$implicit;at(e.shades)}}var S0=.6,af=class t{canvasBinding;ctx;colorState=d(cn);hostElement=d(k);stateForm=this.colorState.stateForm;colors=E(()=>this.colorState.state().colors);radius=z(0);cx=E(()=>this.radius());cy=E(()=>this.radius());constructor(){et(()=>{this.draw(S0)})}onWindowResize(){this.resizeCanvas()}ngAfterViewInit(){let n=this.canvasBinding.nativeElement;this.ctx=n.getContext("2d"),this.resizeCanvas()}resizeCanvas(){let n=this.canvasBinding.nativeElement,e=this.hostElement.nativeElement;n.width=e.clientWidth,n.height=e.clientHeight,this.radius.set(n.width/2),this.draw(S0)}draw(n){if(this.ctx!=null)for(let e=0;e<360;e++){let i=(e-1)*Math.PI/180,r=(e+1)*Math.PI/180;this.ctx.beginPath(),this.ctx.moveTo(this.cx(),this.cy()),this.ctx.arc(this.cx(),this.cy(),this.radius(),i,r),this.ctx.closePath();let o=this.ctx.createRadialGradient(this.cx(),this.cy(),0,this.cx(),this.cy(),this.radius());o.addColorStop(1,`hsl(${e}, ${Math.round(n*100)}%, 50%)`),o.addColorStop(0,`hsl(${e}, ${Math.round(n*100)}%, 50%)`),this.ctx.fillStyle=o,this.ctx.fill()}}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-wheel"]],viewQuery:function(e,i){if(e&1&&Pe(J1,5),e&2){let r;q(r=Y())&&(i.canvasBinding=r.first)}},hostBindings:function(e,i){e&1&&G("resize",function(){return i.onWindowResize()},Qm)},decls:4,vars:0,consts:[["colorWheel",""],[3,"hue","saturation","light"]],template:function(e,i){e&1&&(O(0,"canvas",null,0),st(2,tL,2,0,null,null,_n)),e&2&&(C(2),at(i.colors()))},dependencies:[sf],styles:["[_nghost-%COMP%]{position:relative;width:100%;max-width:70vh;aspect-ratio:1}canvas[_ngcontent-%COMP%]{width:100%;height:100%}"]})};var nL=["knob"],iL=["valueIndicatorContainer"];function rL(t,n){if(t&1&&(b(0,"div",2,1)(2,"div",5)(3,"span",6),X(4),_()()()),t&2){let e=re();C(4),Ct(e.valueIndicatorText)}}var oL=["trackActive"],sL=["*"];function aL(t,n){if(t&1&&O(0,"div"),t&2){let e=n.$implicit,i=n.$index,r=re(3);ft(e===0?"mdc-slider__tick-mark--active":"mdc-slider__tick-mark--inactive"),on("transform",r._calcTickMarkTransform(i))}}function lL(t,n){if(t&1&&st(0,aL,1,4,"div",8,_n),t&2){let e=re(2);at(e._tickMarks)}}function cL(t,n){if(t&1&&(b(0,"div",6,1),ee(2,lL,2,0),_()),t&2){let e=re();C(2),te(e._cachedWidth?2:-1)}}function dL(t,n){if(t&1&&O(0,"mat-slider-visual-thumb",7),t&2){let e=re();j("discrete",e.discrete)("thumbPosition",1)("valueIndicatorText",e.startValueIndicatorText)}}var oe=(function(t){return t[t.START=1]="START",t[t.END=2]="END",t})(oe||{}),Ss=(function(t){return t[t.ACTIVE=0]="ACTIVE",t[t.INACTIVE=1]="INACTIVE",t})(Ss||{}),Wv=new g("_MatSlider"),D0=new g("_MatSliderThumb"),uL=new g("_MatSliderRangeThumb"),w0=new g("_MatSliderVisualThumb");var fL=(()=>{class t{_cdr=d(Fe);_ngZone=d(I);_slider=d(Wv);_renderer=d(De);_listenerCleanups;discrete=!1;thumbPosition;valueIndicatorText;_ripple;_knob;_valueIndicatorContainer;_sliderInput;_sliderInputEl;_hoverRippleRef;_focusRippleRef;_activeRippleRef;_isHovered=!1;_isActive=!1;_isValueIndicatorVisible=!1;_hostElement=d(k).nativeElement;_platform=d(me);ngAfterViewInit(){let e=this._slider._getInput(this.thumbPosition);e&&(this._ripple.radius=24,this._sliderInput=e,this._sliderInputEl=this._sliderInput._hostElement,this._ngZone.runOutsideAngular(()=>{let i=this._sliderInputEl,r=this._renderer;this._listenerCleanups=[r.listen(i,"pointermove",this._onPointerMove),r.listen(i,"pointerdown",this._onDragStart),r.listen(i,"pointerup",this._onDragEnd),r.listen(i,"pointerleave",this._onMouseLeave),r.listen(i,"focus",this._onFocus),r.listen(i,"blur",this._onBlur)]}))}ngOnDestroy(){this._listenerCleanups?.forEach(e=>e())}_onPointerMove=e=>{if(this._sliderInput._isFocused)return;let i=this._hostElement.getBoundingClientRect(),r=this._slider._isCursorOnSliderThumb(e,i);this._isHovered=r,r?this._showHoverRipple():this._hideRipple(this._hoverRippleRef)};_onMouseLeave=()=>{this._isHovered=!1,this._hideRipple(this._hoverRippleRef)};_onFocus=()=>{this._hideRipple(this._hoverRippleRef),this._showFocusRipple(),this._hostElement.classList.add("mdc-slider__thumb--focused")};_onBlur=()=>{this._isActive||this._hideRipple(this._focusRippleRef),this._isHovered&&this._showHoverRipple(),this._hostElement.classList.remove("mdc-slider__thumb--focused")};_onDragStart=e=>{e.button===0&&(this._isActive=!0,this._showActiveRipple())};_onDragEnd=()=>{this._isActive=!1,this._hideRipple(this._activeRippleRef),this._sliderInput._isFocused||this._hideRipple(this._focusRippleRef),this._platform.SAFARI&&this._showHoverRipple()};_showHoverRipple(){this._isShowingRipple(this._hoverRippleRef)||(this._hoverRippleRef=this._showRipple({enterDuration:0,exitDuration:0}),this._hoverRippleRef?.element.classList.add("mat-mdc-slider-hover-ripple"))}_showFocusRipple(){this._isShowingRipple(this._focusRippleRef)||(this._focusRippleRef=this._showRipple({enterDuration:0,exitDuration:0},!0),this._focusRippleRef?.element.classList.add("mat-mdc-slider-focus-ripple"))}_showActiveRipple(){this._isShowingRipple(this._activeRippleRef)||(this._activeRippleRef=this._showRipple({enterDuration:225,exitDuration:400}),this._activeRippleRef?.element.classList.add("mat-mdc-slider-active-ripple"))}_isShowingRipple(e){return e?.state===At.FADING_IN||e?.state===At.VISIBLE}_showRipple(e,i){if(!this._slider.disabled&&(this._showValueIndicator(),this._slider._isRange&&this._slider._getThumb(this.thumbPosition===oe.START?oe.END:oe.START)._showValueIndicator(),!(this._slider._globalRippleOptions?.disabled&&!i)))return this._ripple.launch({animation:this._slider._noopAnimations?{enterDuration:0,exitDuration:0}:e,centered:!0,persistent:!0})}_hideRipple(e){if(e?.fadeOut(),this._isShowingAnyRipple())return;this._slider._isRange||this._hideValueIndicator();let i=this._getSibling();i._isShowingAnyRipple()||(this._hideValueIndicator(),i._hideValueIndicator())}_showValueIndicator(){this._hostElement.classList.add("mdc-slider__thumb--with-indicator")}_hideValueIndicator(){this._hostElement.classList.remove("mdc-slider__thumb--with-indicator")}_getSibling(){return this._slider._getThumb(this.thumbPosition===oe.START?oe.END:oe.START)}_getValueIndicatorContainer(){return this._valueIndicatorContainer?.nativeElement}_getKnob(){return this._knob.nativeElement}_isShowingAnyRipple(){return this._isShowingRipple(this._hoverRippleRef)||this._isShowingRipple(this._focusRippleRef)||this._isShowingRipple(this._activeRippleRef)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-slider-visual-thumb"]],viewQuery:function(i,r){if(i&1&&Pe(io,5)(nL,5)(iL,5),i&2){let o;q(o=Y())&&(r._ripple=o.first),q(o=Y())&&(r._knob=o.first),q(o=Y())&&(r._valueIndicatorContainer=o.first)}},hostAttrs:[1,"mdc-slider__thumb","mat-mdc-slider-visual-thumb"],inputs:{discrete:"discrete",thumbPosition:"thumbPosition",valueIndicatorText:"valueIndicatorText"},features:[Ae([{provide:w0,useExisting:t}])],decls:4,vars:2,consts:[["knob",""],["valueIndicatorContainer",""],[1,"mdc-slider__value-indicator-container"],[1,"mdc-slider__thumb-knob"],["matRipple","",1,"mat-focus-indicator",3,"matRippleDisabled"],[1,"mdc-slider__value-indicator"],[1,"mdc-slider__value-indicator-text"]],template:function(i,r){i&1&&(ee(0,rL,5,1,"div",2),O(1,"div",3,0)(3,"div",4)),i&2&&(te(r.discrete?0:-1),C(3),j("matRippleDisabled",!0))},dependencies:[io],styles:[`.mat-mdc-slider-visual-thumb .mat-ripple {
  height: 100%;
  width: 100%;
}

.mat-mdc-slider .mdc-slider__tick-marks {
  justify-content: start;
}
.mat-mdc-slider .mdc-slider__tick-marks .mdc-slider__tick-mark--active,
.mat-mdc-slider .mdc-slider__tick-marks .mdc-slider__tick-mark--inactive {
  position: absolute;
  left: 2px;
}
`],encapsulation:2})}return t})(),E0=(()=>{class t{_ngZone=d(I);_cdr=d(Fe);_elementRef=d(k);_dir=d(lt,{optional:!0});_globalRippleOptions=d(Nl,{optional:!0});_trackActive;_thumbs;_input;_inputs;get disabled(){return this._disabled}set disabled(e){this._disabled=e;let i=this._getInput(oe.END),r=this._getInput(oe.START);i&&(i.disabled=this._disabled),r&&(r.disabled=this._disabled)}_disabled=!1;get discrete(){return this._discrete}set discrete(e){this._discrete=e,this._updateValueIndicatorUIs()}_discrete=!1;get showTickMarks(){return this._showTickMarks}set showTickMarks(e){this._showTickMarks=e,this._hasViewInitialized&&(this._updateTickMarkUI(),this._updateTickMarkTrackUI())}_showTickMarks=!1;get min(){return this._min}set min(e){let i=e==null||isNaN(e)?this._min:e;this._min!==i&&this._updateMin(i)}_min=0;color;disableRipple=!1;_updateMin(e){let i=this._min;this._min=e,this._isRange?this._updateMinRange({old:i,new:e}):this._updateMinNonRange(e),this._onMinMaxOrStepChange()}_updateMinRange(e){let i=this._getInput(oe.END),r=this._getInput(oe.START),o=i.value,s=r.value;r.min=e.new,i.min=Math.max(e.new,r.value),r.max=Math.min(i.max,i.value),r._updateWidthInactive(),i._updateWidthInactive(),e.new<e.old?this._onTranslateXChangeBySideEffect(i,r):this._onTranslateXChangeBySideEffect(r,i),o!==i.value&&this._onValueChange(i),s!==r.value&&this._onValueChange(r)}_updateMinNonRange(e){let i=this._getInput(oe.END);if(i){let r=i.value;i.min=e,i._updateThumbUIByValue(),this._updateTrackUI(i),r!==i.value&&this._onValueChange(i)}}get max(){return this._max}set max(e){let i=e==null||isNaN(e)?this._max:e;this._max!==i&&this._updateMax(i)}_max=100;_updateMax(e){let i=this._max;this._max=e,this._isRange?this._updateMaxRange({old:i,new:e}):this._updateMaxNonRange(e),this._onMinMaxOrStepChange()}_updateMaxRange(e){let i=this._getInput(oe.END),r=this._getInput(oe.START),o=i.value,s=r.value;i.max=e.new,r.max=Math.min(e.new,i.value),i.min=r.value,i._updateWidthInactive(),r._updateWidthInactive(),e.new>e.old?this._onTranslateXChangeBySideEffect(r,i):this._onTranslateXChangeBySideEffect(i,r),o!==i.value&&this._onValueChange(i),s!==r.value&&this._onValueChange(r)}_updateMaxNonRange(e){let i=this._getInput(oe.END);if(i){let r=i.value;i.max=e,i._updateThumbUIByValue(),this._updateTrackUI(i),r!==i.value&&this._onValueChange(i)}}get step(){return this._step}set step(e){let i=isNaN(e)?this._step:e;this._step!==i&&this._updateStep(i)}_step=1;_updateStep(e){this._step=e,this._isRange?this._updateStepRange():this._updateStepNonRange(),this._onMinMaxOrStepChange()}_updateStepRange(){let e=this._getInput(oe.END),i=this._getInput(oe.START),r=e.value,o=i.value,s=i.value;e.min=this._min,i.max=this._max,e.step=this._step,i.step=this._step,this._platform.SAFARI&&(e.value=e.value,i.value=i.value),e.min=Math.max(this._min,i.value),i.max=Math.min(this._max,e.value),i._updateWidthInactive(),e._updateWidthInactive(),e.value<s?this._onTranslateXChangeBySideEffect(i,e):this._onTranslateXChangeBySideEffect(e,i),r!==e.value&&this._onValueChange(e),o!==i.value&&this._onValueChange(i)}_updateStepNonRange(){let e=this._getInput(oe.END);if(e){let i=e.value;e.step=this._step,this._platform.SAFARI&&(e.value=e.value),e._updateThumbUIByValue(),i!==e.value&&this._onValueChange(e)}}displayWith=e=>`${e}`;_tickMarks;_noopAnimations=Re();_resizeObserver=null;_cachedWidth;_cachedLeft;_rippleRadius=24;startValueIndicatorText="";endValueIndicatorText="";_endThumbTransform;_startThumbTransform;_isRange=!1;_isRtl=E(()=>this._dir?.valueSignal()==="rtl");_hasViewInitialized=!1;_tickMarkTrackWidth=0;_hasAnimation=!1;_resizeTimer=null;_platform=d(me);constructor(){d(Ke).load(Xi);let e=this._isRtl();Fo(()=>{let i=this._isRtl();i!==e&&(e=i,this._isRange?this._onDirChangeRange():this._onDirChangeNonRange(),this._updateTickMarkUI())})}_knobRadius=8;_inputPadding;ngAfterViewInit(){this._platform.isBrowser&&this._updateDimensions();let e=this._getInput(oe.END),i=this._getInput(oe.START);this._isRange=!!e&&!!i,this._cdr.detectChanges();let r=this._getThumb(oe.END);this._rippleRadius=r._ripple.radius,this._inputPadding=this._rippleRadius-this._knobRadius,this._isRange?this._initUIRange(e,i):this._initUINonRange(e),this._updateTrackUI(e),this._updateTickMarkUI(),this._updateTickMarkTrackUI(),this._observeHostResize(),this._cdr.detectChanges()}_initUINonRange(e){e.initProps(),e.initUI(),this._updateValueIndicatorUI(e),this._hasViewInitialized=!0,e._updateThumbUIByValue()}_initUIRange(e,i){e.initProps(),e.initUI(),i.initProps(),i.initUI(),e._updateMinMax(),i._updateMinMax(),e._updateStaticStyles(),i._updateStaticStyles(),this._updateValueIndicatorUIs(),this._hasViewInitialized=!0,e._updateThumbUIByValue(),i._updateThumbUIByValue()}ngOnDestroy(){this._resizeObserver?.disconnect(),this._resizeObserver=null}_onDirChangeRange(){let e=this._getInput(oe.END),i=this._getInput(oe.START);e._setIsLeftThumb(),i._setIsLeftThumb(),e.translateX=e._calcTranslateXByValue(),i.translateX=i._calcTranslateXByValue(),e._updateStaticStyles(),i._updateStaticStyles(),e._updateWidthInactive(),i._updateWidthInactive(),e._updateThumbUIByValue(),i._updateThumbUIByValue()}_onDirChangeNonRange(){this._getInput(oe.END)._updateThumbUIByValue()}_observeHostResize(){typeof ResizeObserver>"u"||!ResizeObserver||this._ngZone.runOutsideAngular(()=>{this._resizeObserver=new ResizeObserver(()=>{this._isActive()||(this._resizeTimer&&clearTimeout(this._resizeTimer),this._onResize())}),this._resizeObserver.observe(this._elementRef.nativeElement)})}_isActive(){return this._getThumb(oe.START)._isActive||this._getThumb(oe.END)._isActive}_getValue(e=oe.END){let i=this._getInput(e);return i?i.value:this.min}_skipUpdate(){return!!(this._getInput(oe.START)?._skipUIUpdate||this._getInput(oe.END)?._skipUIUpdate)}_updateDimensions(){this._cachedWidth=this._elementRef.nativeElement.offsetWidth,this._cachedLeft=this._elementRef.nativeElement.getBoundingClientRect().left}_setTrackActiveStyles(e){let i=this._trackActive.nativeElement.style;i.left=e.left,i.right=e.right,i.transformOrigin=e.transformOrigin,i.transform=e.transform}_calcTickMarkTransform(e){let i=e*(this._tickMarkTrackWidth/(this._tickMarks.length-1));return`translateX(${this._isRtl()?this._cachedWidth-6-i:i}px)`}_onTranslateXChange(e){this._hasViewInitialized&&(this._updateThumbUI(e),this._updateTrackUI(e),this._updateOverlappingThumbUI(e))}_onTranslateXChangeBySideEffect(e,i){this._hasViewInitialized&&(e._updateThumbUIByValue(),i._updateThumbUIByValue())}_onValueChange(e){this._hasViewInitialized&&(this._updateValueIndicatorUI(e),this._updateTickMarkUI(),this._cdr.detectChanges())}_onMinMaxOrStepChange(){this._hasViewInitialized&&(this._updateTickMarkUI(),this._updateTickMarkTrackUI(),this._cdr.markForCheck())}_onResize(){if(this._hasViewInitialized){if(this._updateDimensions(),this._isRange){let e=this._getInput(oe.END),i=this._getInput(oe.START);e._updateThumbUIByValue(),i._updateThumbUIByValue(),e._updateStaticStyles(),i._updateStaticStyles(),e._updateMinMax(),i._updateMinMax(),e._updateWidthInactive(),i._updateWidthInactive()}else{let e=this._getInput(oe.END);e&&e._updateThumbUIByValue()}this._updateTickMarkUI(),this._updateTickMarkTrackUI(),this._cdr.detectChanges()}}_thumbsOverlap=!1;_areThumbsOverlapping(){let e=this._getInput(oe.START),i=this._getInput(oe.END);return!e||!i?!1:i.translateX-e.translateX<20}_updateOverlappingThumbClassNames(e){let i=e.getSibling(),r=this._getThumb(e.thumbPosition);this._getThumb(i.thumbPosition)._hostElement.classList.remove("mdc-slider__thumb--top"),r._hostElement.classList.toggle("mdc-slider__thumb--top",this._thumbsOverlap)}_updateOverlappingThumbUI(e){!this._isRange||this._skipUpdate()||this._thumbsOverlap!==this._areThumbsOverlapping()&&(this._thumbsOverlap=!this._thumbsOverlap,this._updateOverlappingThumbClassNames(e))}_updateThumbUI(e){if(this._skipUpdate())return;let i=this._getThumb(e.thumbPosition===oe.END?oe.END:oe.START);i._hostElement.style.transform=`translateX(${e.translateX}px)`}_updateValueIndicatorUI(e){if(this._skipUpdate())return;let i=this.displayWith(e.value);if(this._hasViewInitialized?e._valuetext.set(i):e._hostElement.setAttribute("aria-valuetext",i),this.discrete){e.thumbPosition===oe.START?this.startValueIndicatorText=i:this.endValueIndicatorText=i;let r=this._getThumb(e.thumbPosition);i.length<3?r._hostElement.classList.add("mdc-slider__thumb--short-value"):r._hostElement.classList.remove("mdc-slider__thumb--short-value")}}_updateValueIndicatorUIs(){let e=this._getInput(oe.END),i=this._getInput(oe.START);e&&this._updateValueIndicatorUI(e),i&&this._updateValueIndicatorUI(i)}_updateTickMarkTrackUI(){if(!this.showTickMarks||this._skipUpdate())return;let e=this._step&&this._step>0?this._step:1,r=(Math.floor(this.max/e)*e-this.min)/(this.max-this.min);this._tickMarkTrackWidth=(this._cachedWidth-6)*r}_updateTrackUI(e){this._skipUpdate()||(this._isRange?this._updateTrackUIRange(e):this._updateTrackUINonRange(e))}_updateTrackUIRange(e){let i=e.getSibling();if(!i||!this._cachedWidth)return;let r=Math.abs(i.translateX-e.translateX)/this._cachedWidth;e._isLeftThumb&&this._cachedWidth?this._setTrackActiveStyles({left:"auto",right:`${this._cachedWidth-i.translateX}px`,transformOrigin:"right",transform:`scaleX(${r})`}):this._setTrackActiveStyles({left:`${i.translateX}px`,right:"auto",transformOrigin:"left",transform:`scaleX(${r})`})}_updateTrackUINonRange(e){this._isRtl()?this._setTrackActiveStyles({left:"auto",right:"0px",transformOrigin:"right",transform:`scaleX(${1-e.fillPercentage})`}):this._setTrackActiveStyles({left:"0px",right:"auto",transformOrigin:"left",transform:`scaleX(${e.fillPercentage})`})}_updateTickMarkUI(){if(!this.showTickMarks||this.step===void 0||this.min===void 0||this.max===void 0)return;let e=this.step>0?this.step:1;this._isRange?this._updateTickMarkUIRange(e):this._updateTickMarkUINonRange(e)}_updateTickMarkUINonRange(e){let i=this._getValue(),r=Math.max(Math.round((i-this.min)/e),0)+1,o=Math.max(Math.round((this.max-i)/e),0)-1;this._isRtl()?r++:o++,this._tickMarks=Array(r).fill(Ss.ACTIVE).concat(Array(o).fill(Ss.INACTIVE))}_updateTickMarkUIRange(e){let i=this._getValue(),r=this._getValue(oe.START),o=Math.max(Math.round((r-this.min)/e),0),s=Math.max(Math.round((i-r)/e)+1,0),a=Math.max(Math.round((this.max-i)/e),0);this._tickMarks=Array(o).fill(Ss.INACTIVE).concat(Array(s).fill(Ss.ACTIVE),Array(a).fill(Ss.INACTIVE))}_getInput(e){if(e===oe.END&&this._input)return this._input;if(this._inputs?.length)return e===oe.START?this._inputs.first:this._inputs.last}_getThumb(e){return e===oe.END?this._thumbs?.last:this._thumbs?.first}_setTransition(e){this._hasAnimation=!this._platform.IOS&&e&&!this._noopAnimations,this._elementRef.nativeElement.classList.toggle("mat-mdc-slider-with-animation",this._hasAnimation)}_isCursorOnSliderThumb(e,i){let r=i.width/2,o=i.x+r,s=i.y+r,a=e.clientX-o,l=e.clientY-s;return Math.pow(a,2)+Math.pow(l,2)<Math.pow(r,2)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-slider"]],contentQueries:function(i,r,o){if(i&1&&bn(o,D0,5)(o,uL,4),i&2){let s;q(s=Y())&&(r._input=s.first),q(s=Y())&&(r._inputs=s)}},viewQuery:function(i,r){if(i&1&&Pe(oL,5)(w0,5),i&2){let o;q(o=Y())&&(r._trackActive=o.first),q(o=Y())&&(r._thumbs=o)}},hostAttrs:[1,"mat-mdc-slider","mdc-slider"],hostVars:12,hostBindings:function(i,r){i&2&&(ft("mat-"+(r.color||"primary")),Z("mdc-slider--range",r._isRange)("mdc-slider--disabled",r.disabled)("mdc-slider--discrete",r.discrete)("mdc-slider--tick-marks",r.showTickMarks)("_mat-animation-noopable",r._noopAnimations))},inputs:{disabled:[2,"disabled","disabled",Q],discrete:[2,"discrete","discrete",Q],showTickMarks:[2,"showTickMarks","showTickMarks",Q],min:[2,"min","min",St],color:"color",disableRipple:[2,"disableRipple","disableRipple",Q],max:[2,"max","max",St],step:[2,"step","step",St],displayWith:"displayWith"},exportAs:["matSlider"],features:[Ae([{provide:Wv,useExisting:t}])],ngContentSelectors:sL,decls:9,vars:5,consts:[["trackActive",""],["tickMarkContainer",""],[1,"mdc-slider__track"],[1,"mdc-slider__track--inactive"],[1,"mdc-slider__track--active"],[1,"mdc-slider__track--active_fill"],[1,"mdc-slider__tick-marks"],[3,"discrete","thumbPosition","valueIndicatorText"],[3,"class","transform"]],template:function(i,r){i&1&&(xe(),le(0),b(1,"div",2),O(2,"div",3),b(3,"div",4),O(4,"div",5,0),_(),ee(6,cL,3,1,"div",6),_(),ee(7,dL,1,3,"mat-slider-visual-thumb",7),O(8,"mat-slider-visual-thumb",7)),i&2&&(C(6),te(r.showTickMarks?6:-1),C(),te(r._isRange?7:-1),C(),j("discrete",r.discrete)("thumbPosition",2)("valueIndicatorText",r.endValueIndicatorText))},dependencies:[fL],styles:[`.mdc-slider__track {
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  width: 100%;
  pointer-events: none;
  height: var(--%NS%mat-slider-inactive-track-height, 4px);
}

.mdc-slider__track--active,
.mdc-slider__track--inactive {
  display: flex;
  height: 100%;
  position: absolute;
  width: 100%;
}

.mdc-slider__track--active {
  overflow: hidden;
  border-radius: var(--%NS%mat-slider-active-track-shape, var(--%NS%mat-sys-corner-full));
  height: var(--%NS%mat-slider-active-track-height, 4px);
  top: calc((var(--%NS%mat-slider-inactive-track-height, 4px) - var(--%NS%mat-slider-active-track-height, 4px)) / 2);
}

.mdc-slider__track--active_fill {
  border-top-style: solid;
  box-sizing: border-box;
  height: 100%;
  width: 100%;
  position: relative;
  transform-origin: left;
  transition: transform 80ms ease;
  border-color: var(--%NS%mat-slider-active-track-color, var(--%NS%mat-sys-primary));
  border-top-width: var(--%NS%mat-slider-active-track-height, 4px);
}
.mdc-slider--disabled .mdc-slider__track--active_fill {
  border-color: var(--%NS%mat-slider-disabled-active-track-color, var(--%NS%mat-sys-on-surface));
}
[dir=rtl] .mdc-slider__track--active_fill {
  -webkit-transform-origin: right;
  transform-origin: right;
}

.mdc-slider__track--inactive {
  left: 0;
  top: 0;
  opacity: 0.24;
  background-color: var(--%NS%mat-slider-inactive-track-color, var(--%NS%mat-sys-surface-variant));
  height: var(--%NS%mat-slider-inactive-track-height, 4px);
  border-radius: var(--%NS%mat-slider-inactive-track-shape, var(--%NS%mat-sys-corner-full));
}
.mdc-slider--disabled .mdc-slider__track--inactive {
  background-color: var(--%NS%mat-slider-disabled-inactive-track-color, var(--%NS%mat-sys-on-surface));
  opacity: 0.24;
}
.mdc-slider__track--%NS%inactive::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-slider__track--%NS%inactive::before {
    border-color: CanvasText;
  }
}

.mdc-slider__value-indicator-container {
  bottom: 44px;
  left: 50%;
  pointer-events: none;
  position: absolute;
  transform: var(--%NS%mat-slider-value-indicator-container-transform, translateX(-50%) rotate(-45deg));
}
.mdc-slider__thumb--with-indicator .mdc-slider__value-indicator-container {
  pointer-events: auto;
}

.mdc-slider__value-indicator {
  display: flex;
  align-items: center;
  transform: scale(0);
  transform-origin: var(--%NS%mat-slider-value-indicator-transform-origin, 0 28px);
  transition: transform 100ms cubic-bezier(0.4, 0, 1, 1);
  word-break: normal;
  background-color: var(--%NS%mat-slider-label-container-color, var(--%NS%mat-sys-primary));
  color: var(--%NS%mat-slider-label-label-text-color, var(--%NS%mat-sys-on-primary));
  width: var(--%NS%mat-slider-value-indicator-width, 28px);
  height: var(--%NS%mat-slider-value-indicator-height, 28px);
  padding: var(--%NS%mat-slider-value-indicator-padding, 0);
  opacity: var(--%NS%mat-slider-value-indicator-opacity, 1);
  border-radius: var(--%NS%mat-slider-value-indicator-border-radius, 50% 50% 50% 0);
}
.mdc-slider__thumb--with-indicator .mdc-slider__value-indicator {
  transition: transform 100ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale(1);
}
.mdc-slider__value-indicator::before {
  border-left: 6px solid transparent;
  border-right: 6px solid transparent;
  border-top: 6px solid;
  bottom: -5px;
  content: "";
  height: 0;
  left: 50%;
  position: absolute;
  transform: translateX(-50%);
  width: 0;
  display: var(--%NS%mat-slider-value-indicator-caret-display, none);
  border-top-color: var(--%NS%mat-slider-label-container-color, var(--%NS%mat-sys-primary));
}
.mdc-slider__value-indicator::after {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-slider__value-indicator::after {
    border-color: CanvasText;
  }
}

.mdc-slider__value-indicator-text {
  text-align: center;
  width: var(--%NS%mat-slider-value-indicator-width, 28px);
  transform: var(--%NS%mat-slider-value-indicator-text-transform, rotate(45deg));
  font-family: var(--%NS%mat-slider-label-label-text-font, var(--%NS%mat-sys-label-medium-font));
  font-size: var(--%NS%mat-slider-label-label-text-size, var(--%NS%mat-sys-label-medium-size));
  font-weight: var(--%NS%mat-slider-label-label-text-weight, var(--%NS%mat-sys-label-medium-weight));
  line-height: var(--%NS%mat-slider-label-label-text-line-height, var(--%NS%mat-sys-label-medium-line-height));
  letter-spacing: var(--%NS%mat-slider-label-label-text-tracking, var(--%NS%mat-sys-label-medium-tracking));
}

.mdc-slider__thumb {
  -webkit-user-select: none;
  user-select: none;
  display: flex;
  left: -24px;
  outline: none;
  position: absolute;
  height: 48px;
  width: 48px;
  pointer-events: none;
}
.mdc-slider--discrete .mdc-slider__thumb {
  transition: transform 80ms ease;
}
.mdc-slider--disabled .mdc-slider__thumb {
  pointer-events: none;
}

.mdc-slider__thumb--top {
  z-index: 1;
}

.mdc-slider__thumb-knob {
  position: absolute;
  box-sizing: border-box;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%);
  border-style: solid;
  width: var(--%NS%mat-slider-handle-width, 20px);
  height: var(--%NS%mat-slider-handle-height, 20px);
  border-width: calc(var(--%NS%mat-slider-handle-height, 20px) / 2) calc(var(--%NS%mat-slider-handle-width, 20px) / 2);
  box-shadow: var(--%NS%mat-slider-handle-elevation, var(--%NS%mat-sys-level1));
  background-color: var(--%NS%mat-slider-handle-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-slider-handle-color, var(--%NS%mat-sys-primary));
  border-radius: var(--%NS%mat-slider-handle-shape, var(--%NS%mat-sys-corner-full));
}
.mdc-slider__thumb:hover .mdc-slider__thumb-knob {
  background-color: var(--%NS%mat-slider-hover-handle-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-slider-hover-handle-color, var(--%NS%mat-sys-primary));
}
.mdc-slider__thumb--focused .mdc-slider__thumb-knob {
  background-color: var(--%NS%mat-slider-focus-handle-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-slider-focus-handle-color, var(--%NS%mat-sys-primary));
}
.mdc-slider--disabled .mdc-slider__thumb-knob {
  background-color: var(--%NS%mat-slider-disabled-handle-color, var(--%NS%mat-sys-on-surface));
  border-color: var(--%NS%mat-slider-disabled-handle-color, var(--%NS%mat-sys-on-surface));
}
.mdc-slider__thumb--top .mdc-slider__thumb-knob, .mdc-slider__thumb--top.mdc-slider__thumb:hover .mdc-slider__thumb-knob, .mdc-slider__thumb--top.mdc-slider__thumb--focused .mdc-slider__thumb-knob {
  border: solid 1px #fff;
  box-sizing: content-box;
  border-color: var(--%NS%mat-slider-with-overlap-handle-outline-color, var(--%NS%mat-sys-on-primary));
  border-width: var(--%NS%mat-slider-with-overlap-handle-outline-width, 1px);
}

.mdc-slider__tick-marks {
  align-items: center;
  box-sizing: border-box;
  display: flex;
  height: 100%;
  justify-content: space-between;
  padding: 0 1px;
  position: absolute;
  width: 100%;
}

.mdc-slider__tick-mark--active,
.mdc-slider__tick-mark--inactive {
  width: var(--%NS%mat-slider-with-tick-marks-container-size, 2px);
  height: var(--%NS%mat-slider-with-tick-marks-container-size, 2px);
  border-radius: var(--%NS%mat-slider-with-tick-marks-container-shape, var(--%NS%mat-sys-corner-full));
}

.mdc-slider__tick-mark--inactive {
  opacity: var(--%NS%mat-slider-with-tick-marks-inactive-container-opacity, 0.38);
  background-color: var(--%NS%mat-slider-with-tick-marks-inactive-container-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-slider--disabled .mdc-slider__tick-mark--inactive {
  opacity: var(--%NS%mat-slider-with-tick-marks-inactive-container-opacity, 0.38);
  background-color: var(--%NS%mat-slider-with-tick-marks-disabled-container-color, var(--%NS%mat-sys-on-surface));
}

.mdc-slider__tick-mark--active {
  opacity: var(--%NS%mat-slider-with-tick-marks-active-container-opacity, 0.38);
  background-color: var(--%NS%mat-slider-with-tick-marks-active-container-color, var(--%NS%mat-sys-on-primary));
}

.mdc-slider__input {
  cursor: pointer;
  left: 2px;
  margin: 0;
  height: 44px;
  opacity: 0;
  position: absolute;
  top: 2px;
  width: 44px;
  box-sizing: content-box;
}
.mdc-slider__input.mat-mdc-slider-input-no-pointer-events {
  pointer-events: none;
}
.mdc-slider__input.mat-slider__right-input {
  left: auto;
  right: 0;
}

.mat-mdc-slider {
  display: inline-block;
  box-sizing: border-box;
  outline: none;
  vertical-align: middle;
  cursor: pointer;
  height: 48px;
  margin: 0 8px;
  position: relative;
  touch-action: pan-y;
  width: auto;
  min-width: 112px;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-slider.mdc-slider--disabled {
  cursor: auto;
  opacity: 0.38;
}
.mat-mdc-slider.mdc-slider--disabled .mdc-slider__input {
  cursor: auto;
}
.mat-mdc-slider .mdc-slider__thumb,
.mat-mdc-slider .mdc-slider__track--active_fill {
  transition-duration: 0ms;
}
.mat-mdc-slider.mat-mdc-slider-with-animation .mdc-slider__thumb,
.mat-mdc-slider.mat-mdc-slider-with-animation .mdc-slider__track--active_fill {
  transition-duration: 80ms;
}
.mat-mdc-slider.mdc-slider--discrete .mdc-slider__thumb,
.mat-mdc-slider.mdc-slider--discrete .mdc-slider__track--active_fill {
  transition-duration: 0ms;
}
.mat-mdc-slider.mat-mdc-slider-with-animation .mdc-slider__thumb,
.mat-mdc-slider.mat-mdc-slider-with-animation .mdc-slider__track--active_fill {
  transition-duration: 80ms;
}
.mat-mdc-slider .mat-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-slider-ripple-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-slider .mat-ripple .mat-mdc-slider-hover-ripple {
  background-color: var(--%NS%mat-slider-hover-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-primary) 5%, transparent));
}
.mat-mdc-slider .mat-ripple .mat-mdc-slider-focus-ripple,
.mat-mdc-slider .mat-ripple .mat-mdc-slider-active-ripple {
  background-color: var(--%NS%mat-slider-focus-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-primary) 20%, transparent));
}
.mat-mdc-slider._mat-animation-noopable.mdc-slider--discrete .mdc-slider__thumb, .mat-mdc-slider._mat-animation-noopable.mdc-slider--discrete .mdc-slider__track--active_fill,
.mat-mdc-slider._mat-animation-noopable .mdc-slider__value-indicator {
  transition: none;
}
.mat-mdc-slider .mat-focus-indicator::before {
  border-radius: 50%;
}

.mdc-slider__thumb--focused .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})();var hL={provide:Wr,useExisting:Ot(()=>Gv),multi:!0};var Gv=(()=>{class t{_ngZone=d(I);_elementRef=d(k);_cdr=d(Fe);_slider=d(Wv);_platform=d(me);_listenerCleanups;get value(){return St(this._hostElement.value,0)}set value(e){e===null&&(e=this._getDefaultValue()),e=isNaN(e)?0:e;let i=e+"";if(!this._hasSetInitialValue){this._initialValue=i;return}this._isActive||this._setValue(i)}_setValue(e){this._hostElement.value=e,this._updateThumbUIByValue(),this._slider._onValueChange(this),this._cdr.detectChanges(),this._slider._cdr.markForCheck()}valueChange=new U;dragStart=new U;dragEnd=new U;get translateX(){return this._slider.min>=this._slider.max?(this._translateX=this._tickMarkOffset,this._translateX):(this._translateX===void 0&&(this._translateX=this._calcTranslateXByValue()),this._translateX)}set translateX(e){this._translateX=e}_translateX;thumbPosition=oe.END;get min(){return St(this._hostElement.min,0)}set min(e){this._hostElement.min=e+"",this._cdr.detectChanges()}get max(){return St(this._hostElement.max,0)}set max(e){this._hostElement.max=e+"",this._cdr.detectChanges()}get step(){return St(this._hostElement.step,0)}set step(e){this._hostElement.step=e+"",this._cdr.detectChanges()}get disabled(){return Q(this._hostElement.disabled)}set disabled(e){this._hostElement.disabled=e,this._cdr.detectChanges(),this._slider.disabled!==this.disabled&&(this._slider.disabled=this.disabled)}get percentage(){return this._slider.min>=this._slider.max?this._slider._isRtl()?1:0:(this.value-this._slider.min)/(this._slider.max-this._slider.min)}get fillPercentage(){return this._slider._cachedWidth?this._translateX===0?0:this.translateX/this._slider._cachedWidth:this._slider._isRtl()?1:0}_hostElement=this._elementRef.nativeElement;_valuetext=z("");_knobRadius=8;_tickMarkOffset=3;_isActive=!1;_isFocused=!1;_setIsFocused(e){this._isFocused=e}_hasSetInitialValue=!1;_initialValue;_formControl;_destroyed=new S;_skipUIUpdate=!1;_onChangeFn;_onTouchedFn=()=>{};_isControlInitialized=!1;constructor(){let e=d(De);this._ngZone.runOutsideAngular(()=>{this._listenerCleanups=[e.listen(this._hostElement,"pointerdown",this._onPointerDown.bind(this)),e.listen(this._hostElement,"pointermove",this._onPointerMove.bind(this)),e.listen(this._hostElement,"pointerup",this._onPointerUp.bind(this))]})}ngOnDestroy(){this._listenerCleanups.forEach(e=>e()),this._destroyed.next(),this._destroyed.complete(),this.dragStart.complete(),this.dragEnd.complete()}initProps(){this._updateWidthInactive(),this.disabled!==this._slider.disabled&&(this._slider.disabled=!0),this.step=this._slider.step,this.min=this._slider.min,this.max=this._slider.max,this._initValue()}initUI(){this._updateThumbUIByValue()}_initValue(){this._hasSetInitialValue=!0,this._initialValue===void 0?this.value=this._getDefaultValue():(this._hostElement.value=this._initialValue,this._updateThumbUIByValue(),this._slider._onValueChange(this),this._cdr.detectChanges())}_getDefaultValue(){return this.min}_onBlur(){this._setIsFocused(!1),this._onTouchedFn()}_onFocus(){this._slider._setTransition(!1),this._slider._updateTrackUI(this),this._setIsFocused(!0)}_onChange(){this.valueChange.emit(this.value),this._isActive&&this._updateThumbUIByValue({withAnimation:!0})}_onInput(){this._onChangeFn?.(this.value),(this._slider.step||!this._isActive)&&this._updateThumbUIByValue({withAnimation:!0}),this._slider._onValueChange(this)}_onNgControlValueChange(){(!this._isActive||!this._isFocused)&&(this._slider._onValueChange(this),this._updateThumbUIByValue()),this._slider.disabled=this._formControl.disabled}_onPointerDown(e){if(!(this.disabled||e.button!==0)){if(this._platform.IOS){let i=this._slider._isCursorOnSliderThumb(e,this._slider._getThumb(this.thumbPosition)._hostElement.getBoundingClientRect());this._isActive=i,this._updateWidthActive(),this._slider._updateDimensions();return}this._isActive=!0,this._setIsFocused(!0),this._updateWidthActive(),this._slider._updateDimensions(),this._slider.step||this._updateThumbUIByPointerEvent(e,{withAnimation:!0}),this.disabled||(this._handleValueCorrection(e),this.dragStart.emit({source:this,parent:this._slider,value:this.value}))}}_handleValueCorrection(e){this._skipUIUpdate=!0,setTimeout(()=>{this._skipUIUpdate=!1,this._fixValue(e)},0)}_fixValue(e){let i=e.clientX-this._slider._cachedLeft,r=this._slider._cachedWidth,o=this._slider.step===0?1:this._slider.step,s=Math.floor((this._slider.max-this._slider.min)/o),a=this._slider._isRtl()?1-i/r:i/r,c=Math.round(a*s)/s*(this._slider.max-this._slider.min)+this._slider.min,u=Math.round(c/o)*o,f=this.value;if(u===f){this._slider._onValueChange(this),this._slider.step>0?this._updateThumbUIByValue():this._updateThumbUIByPointerEvent(e,{withAnimation:this._slider._hasAnimation});return}this.value=u,this.valueChange.emit(this.value),this._onChangeFn?.(this.value),this._slider._onValueChange(this),this._slider.step>0?this._updateThumbUIByValue():this._updateThumbUIByPointerEvent(e,{withAnimation:this._slider._hasAnimation})}_onPointerMove(e){!this._slider.step&&this._isActive&&this._updateThumbUIByPointerEvent(e)}_onPointerUp(){this._isActive&&(this._isActive=!1,this._platform.SAFARI&&this._setIsFocused(!1),this.dragEnd.emit({source:this,parent:this._slider,value:this.value}),setTimeout(()=>this._updateWidthInactive(),this._platform.IOS?10:0))}_clamp(e){let i=this._tickMarkOffset,r=this._slider._cachedWidth-this._tickMarkOffset;return Math.max(Math.min(e,r),i)}_calcTranslateXByValue(){return this._slider._isRtl()?(1-this.percentage)*(this._slider._cachedWidth-this._tickMarkOffset*2)+this._tickMarkOffset:this.percentage*(this._slider._cachedWidth-this._tickMarkOffset*2)+this._tickMarkOffset}_calcTranslateXByPointerEvent(e){return e.clientX-this._slider._cachedLeft}_updateWidthActive(){}_updateWidthInactive(){this._hostElement.style.padding=`0 ${this._slider._inputPadding}px`,this._hostElement.style.width=`calc(100% + ${this._slider._inputPadding-this._tickMarkOffset*2}px)`,this._hostElement.style.left=`-${this._slider._rippleRadius-this._tickMarkOffset}px`}_updateThumbUIByValue(e){this.translateX=this._clamp(this._calcTranslateXByValue()),this._updateThumbUI(e)}_updateThumbUIByPointerEvent(e,i){this.translateX=this._clamp(this._calcTranslateXByPointerEvent(e)),this._updateThumbUI(i)}_updateThumbUI(e){this._slider._setTransition(!!e?.withAnimation),this._slider._onTranslateXChange(this)}writeValue(e){(this._isControlInitialized||e!==null)&&(this.value=e)}registerOnChange(e){this._onChangeFn=e,this._isControlInitialized=!0}registerOnTouched(e){this._onTouchedFn=e}setDisabledState(e){this.disabled=e}focus(){this._hostElement.focus()}blur(){this._hostElement.blur()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["input","matSliderThumb",""]],hostAttrs:["type","range",1,"mdc-slider__input"],hostVars:1,hostBindings:function(i,r){i&1&&G("change",function(){return r._onChange()})("input",function(){return r._onInput()})("blur",function(){return r._onBlur()})("focus",function(){return r._onFocus()}),i&2&&ve("aria-valuetext",r._valuetext())},inputs:{value:[2,"value","value",St]},outputs:{valueChange:"valueChange",dragStart:"dragStart",dragEnd:"dragEnd"},exportAs:["matSliderThumb"],features:[Ae([hL,{provide:D0,useExisting:t}])]})}return t})();var x0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[_s,Ie]})}return t})();var pL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),gL={passive:!0},I0=(()=>{class t{_platform=d(me);_ngZone=d(I);_renderer=d(Ze).createRenderer(null,null);_styleLoader=d(Ke);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return He;this._styleLoader.load(pL);let i=Gt(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new S,s="cdk-text-field-autofilled",a=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,gL)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=Gt(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var T0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({})}return t})();var M0=new g("MAT_INPUT_VALUE_ACCESSOR");var qv=class{_box;_destroyed=new S;_resizeSubject=new S;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new J(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(_e(e=>e.some(i=>i.target===n)),yc({bufferSize:1,refCount:!0}),be(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},N0=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(I);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new qv(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var vL=["notch"],_L=["*"],k0=["iconPrefixContainer"],A0=["textPrefixContainer"],R0=["iconSuffixContainer"],O0=["textSuffixContainer"],bL=["textField"],yL=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],CL=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function SL(t,n){t&1&&O(0,"span",21)}function DL(t,n){if(t&1&&(b(0,"label",20),le(1,1),ee(2,SL,1,0,"span",21),_()),t&2){let e=re(2);j("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),ve("for",e._control.disableAutomaticLabeling?null:e._control.id),C(2),te(!e.hideRequiredMarker&&e._control.required?2:-1)}}function wL(t,n){if(t&1&&ee(0,DL,3,5,"label",20),t&2){let e=re();te(e._hasFloatingLabel()?0:-1)}}function EL(t,n){t&1&&O(0,"div",7)}function xL(t,n){}function IL(t,n){if(t&1&&Nt(0,xL,0,0,"ng-template",13),t&2){re(2);let e=fi(1);j("ngTemplateOutlet",e)}}function TL(t,n){if(t&1&&(b(0,"div",9),ee(1,IL,1,1,null,13),_()),t&2){let e=re();j("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),C(),te(e._forceDisplayInfixLabel()?-1:1)}}function ML(t,n){t&1&&(b(0,"div",10,2),le(2,2),_())}function NL(t,n){t&1&&(b(0,"div",11,3),le(2,3),_())}function kL(t,n){}function AL(t,n){if(t&1&&Nt(0,kL,0,0,"ng-template",13),t&2){re();let e=fi(1);j("ngTemplateOutlet",e)}}function RL(t,n){t&1&&(b(0,"div",14,4),le(2,4),_())}function OL(t,n){t&1&&(b(0,"div",15,5),le(2,5),_())}function PL(t,n){t&1&&O(0,"div",16)}function FL(t,n){t&1&&(b(0,"div",18),le(1,6),_())}function LL(t,n){if(t&1&&(b(0,"mat-hint",22),X(1),_()),t&2){let e=re(2);j("id",e._hintLabelId),C(),Ct(e.hintLabel)}}function VL(t,n){if(t&1&&(b(0,"div",19),ee(1,LL,2,2,"mat-hint",22),le(2,7),O(3,"div",23),le(4,8),_()),t&2){let e=re();C(),te(e.hintLabel?1:-1)}}var Ds=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-label"]]})}return t})(),BL=new g("MatError");var cf=(()=>{class t{align="start";id=d(je).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(zt("id",r.id),ve("align",null),Z("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),jL=new g("MatPrefix");var U0=new g("MatSuffix"),df=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[Ae([{provide:U0,useExisting:t}])]})}return t})(),H0=new g("FloatingLabelParent"),P0=(()=>{class t{_elementRef=d(k);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(N0);_ngZone=d(I);_parent=d(H0);_resizeSubscription=new pe;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return UL(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function UL(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var F0="mdc-line-ripple--active",lf="mdc-line-ripple--deactivating",L0=(()=>{class t{_elementRef=d(k);_cleanupTransitionEnd;constructor(){let e=d(I),i=d(De);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(lf),e.add(F0)}deactivate(){this._elementRef.nativeElement.classList.add(lf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(lf);e.propertyName==="opacity"&&r&&i.remove(F0,lf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),V0=(()=>{class t{_elementRef=d(k);_ngZone=d(I);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Pe(vL,5),i&2){let o;q(o=Y())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:_L,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(xe(),nn(0,"div",1),ut(1,"div",2,0),le(3),yt(),nn(4,"div",3))},encapsulation:2})}return t})(),Al=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t})}return t})();var Rl=new g("MatFormField"),HL=new g("MAT_FORM_FIELD_DEFAULT_OPTIONS"),B0="fill",zL="auto",j0="fixed",$L="translateY(-50%)",Ol=(()=>{class t{_elementRef=d(k);_changeDetectorRef=d(Fe);_platform=d(me);_idGenerator=d(je);_ngZone=d(I);_defaults=d(HL,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Ta("iconPrefixContainer");_textPrefixContainerSignal=Ta("textPrefixContainer");_iconSuffixContainerSignal=Ta("iconSuffixContainer");_textSuffixContainerSignal=Ta("textSuffixContainer");_prefixSuffixContainers=E(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=WS(Ds);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Si(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||zL}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||B0;this._appearanceSignal.set(i)}_appearanceSignal=z(B0);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||j0}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||j0}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new S;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Re();constructor(){let e=this._defaults,i=d(lt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),et(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=E(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(It([void 0,void 0]),ie(()=>[i.errorState,i.userAriaDescribedBy]),bc(),_e(([[o,s],[a,l]])=>o!==a||s!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(be(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Qn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){Fo({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=E(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,m=`calc(${u} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,v=`var(--mat-mdc-form-field-label-transform, ${$L} translateX(${m}))`,D=s+a+l+c;return[v,D]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Od(o,r._labelChild,Ds,5),bn(o,Al,5)(o,jL,5)(o,U0,5)(o,BL,5)(o,cf,5)),i&2){Fd();let s;q(s=Y())&&(r._formFieldControl=s.first),q(s=Y())&&(r._prefixChildren=s),q(s=Y())&&(r._suffixChildren=s),q(s=Y())&&(r._errorChildren=s),q(s=Y())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Pd(r._iconPrefixContainerSignal,k0,5)(r._textPrefixContainerSignal,A0,5)(r._iconSuffixContainerSignal,R0,5)(r._textSuffixContainerSignal,O0,5),Pe(bL,5)(k0,5)(A0,5)(R0,5)(O0,5)(P0,5)(V0,5)(L0,5)),i&2){Fd(4);let o;q(o=Y())&&(r._textField=o.first),q(o=Y())&&(r._iconPrefixContainer=o.first),q(o=Y())&&(r._textPrefixContainer=o.first),q(o=Y())&&(r._iconSuffixContainer=o.first),q(o=Y())&&(r._textSuffixContainer=o.first),q(o=Y())&&(r._floatingLabel=o.first),q(o=Y())&&(r._notchedOutline=o.first),q(o=Y())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&Z("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Ae([{provide:Rl,useExisting:t},{provide:H0,useExisting:t}])],ngContentSelectors:CL,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(xe(yL),Nt(0,wL,1,1,"ng-template",null,0,wp),b(2,"div",6,1),G("click",function(s){return r._control.onContainerClick(s)}),ee(4,EL,1,0,"div",7),b(5,"div",8),ee(6,TL,2,2,"div",9),ee(7,ML,3,0,"div",10),ee(8,NL,3,0,"div",11),b(9,"div",12),ee(10,AL,1,1,null,13),le(11),_(),ee(12,RL,3,0,"div",14),ee(13,OL,3,0,"div",15),_(),ee(14,PL,1,0,"div",16),_(),b(15,"div",17),ee(16,FL,2,0,"div",18)(17,VL,5,1,"div",19),_()),i&2){let o;C(2),Z("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),C(2),te(!r._hasOutline()&&!r._control.disabled?4:-1),C(2),te(r._hasOutline()?6:-1),C(),te(r._hasIconPrefix?7:-1),C(),te(r._hasTextPrefix?8:-1),C(2),te(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),C(2),te(r._hasTextSuffix?12:-1),C(),te(r._hasIconSuffix?13:-1),C(),te(r._hasOutline()?-1:14),C(),Z("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();C(),te((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[P0,V0,Ap,L0,cf],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-filled-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-outlined-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-filled-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-outlined-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-top-right-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) {
  background-color: var(--%NS%mat-form-field-filled-container-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--%NS%mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-hover-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-filled-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-filled-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-filled-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-filled-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-outlined-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-outlined-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-outlined-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-outlined-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-outline-color, var(--%NS%mat-sys-outline));
  border-width: var(--%NS%mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-hover-outline-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-focus-outline-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-hover-outline-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-focus-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--%NS%mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--%NS%mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--%NS%mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-active-indicator-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-hover-active-indicator-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-active-indicator-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-hover-active-indicator-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--%NS%mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-focus-active-indicator-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-focus-active-indicator-color, var(--%NS%mat-sys-error));
}

.mdc-line-ripple--%NS%active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--%NS%deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--%NS%no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --%NS%mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--%NS%mat-form-field-container-height, 56px);
  padding-top: var(--%NS%mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--%NS%mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--%NS%mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--%NS%mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--%NS%mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--%NS%mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --%NS%mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--%NS%mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--%NS%mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--%NS%mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--%NS%mat-form-field-error-text-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-subscript-text-font, var(--%NS%mat-sys-body-small-font));
  line-height: var(--%NS%mat-form-field-subscript-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  font-size: var(--%NS%mat-form-field-subscript-text-size, var(--%NS%mat-sys-body-small-size));
  letter-spacing: var(--%NS%mat-form-field-subscript-text-tracking, var(--%NS%mat-sys-body-small-tracking));
  font-weight: var(--%NS%mat-form-field-subscript-text-weight, var(--%NS%mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--%NS%mat-form-field-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--%NS%mat-form-field-select-option-text-color, var(--%NS%mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--%NS%mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--%NS%mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--%NS%mat-form-field-enabled-select-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-focus-select-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --%NS%mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-container-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-form-field-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-form-field-container-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-form-field-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
  font-weight: var(--%NS%mat-form-field-container-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--%NS%mat-form-field-outlined-label-text-populated-size) * var(--%NS%mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--%NS%mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-trailing-icon-color, var(--%NS%mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-hover-trailing-icon-color, var(--%NS%mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-focus-trailing-icon-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--%NS%filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return t})();var uf=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}isSignalErrorState(e){if(!e)return!1;let i=e().invalid(),r=e().touched();return i&&r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();var ws=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,i,r,o){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o,e?Ut(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,e)??!1}};var Pl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[AE,Ol,Ie]})}return t})();var qL=["button","checkbox","file","hidden","image","radio","range","reset","submit"],YL=new g("MAT_INPUT_CONFIG"),z0=(()=>{class t{_elementRef=d(k);_platform=d(me);ngControl=d(Hi,{optional:!0,self:!0});_autofillMonitor=d(I0);_ngZone=d(I);_formField=d(Rl,{optional:!0});_renderer=d(De);_uid=d(je).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(YL,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new S;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=Si(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Wn.required)??!1}set required(e){this._required=Si(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Ov().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=Si(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Ov().has(e));constructor(){let e=d(sl,{optional:!0}),i=d(al,{optional:!0}),r=d(uf),o=d(M0,{optional:!0,self:!0}),s=d(as,{optional:!0,self:!0}),a=this._elementRef.nativeElement,l=a.nodeName.toLowerCase();o?Ut(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new ws(r,s||this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=l==="select",this._isTextarea=l==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&et(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){qL.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&G("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(zt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),ve("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),Z("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",Q]},exportAs:["matInput"],features:[Ae([{provide:Al,useExisting:t}]),Ge]})}return t})(),$0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[Pl,Pl,T0,Ie]})}return t})();function W0(t){return Error(`Unable to find icon with the name "${t}"`)}function KL(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function G0(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function q0(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Di=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},Z0=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Di(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(nt.HTML,r);if(!s)throw q0(r);let a=Kr(s);return this._addSvgIconConfig(e,i,new Di("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Di(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(nt.HTML,i);if(!o)throw q0(i);let s=Kr(o);return this._addSvgIconSetConfig(e,new Di("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(nt.RESOURCE_URL,e);if(!i)throw G0(e);let r=this._cachedIconsByUrl.get(i);return r?H(ff(r)):this._loadSvgIconFromConfig(new Di(e,null)).pipe(tt(o=>this._cachedIconsByUrl.set(i,o)),ie(o=>ff(o)))}getNamedSvgIcon(e,i=""){let r=Y0(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):Fs(W0(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?H(ff(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ie(i=>ff(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return H(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(hr(a=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(nt.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(c)),H(null)})));return Bs(o).pipe(ie(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw W0(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(tt(i=>e.svgText=i),ie(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?H(null):this._fetchIcon(e).pipe(tt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(Kr("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(Kr("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw KL();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(nt.RESOURCE_URL,i);if(!s)throw G0(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let l=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(ie(c=>Kr(c)),mr(()=>this._inProgressUrlFetches.delete(s)),Us());return this._inProgressUrlFetches.set(s,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(Y0(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return XL(o)?new Di(o.url,null,o.options):new Di(o,null)}}static \u0275fac=function(i){return new(i||t)(L(La,8),L(Va),L(R,8),L(vt))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ff(t){return t.cloneNode(!0)}function Y0(t,n){return t+":"+n}function XL(t){return!!(t.url&&t.options)}var QL=["*"],JL=new g("MAT_ICON_DEFAULT_OPTIONS"),eV=new g("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(R),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),K0=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],tV=K0.map(t=>`[${t}]`).join(", "),nV=/^url\(['"]?#(.*?)['"]?\)$/,X0=(()=>{class t{_elementRef=d(k);_iconRegistry=d(Z0);_location=d(eV);_errorHandler=d(vt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=pe.EMPTY;constructor(){let e=d(new mi("aria-hidden"),{optional:!0}),i=d(JL,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(tV),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)K0.forEach(s=>{let a=i[o],l=a.getAttribute(s),c=l?l.match(nV):null;if(c){let u=r.get(a);u||(u=[],r.set(a,u)),u.push({name:s,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(ze(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(ve("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),ft(r.color?"mat-"+r.color:""),Z("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",Q],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:QL,decls:1,vars:0,template:function(i,r){i&1&&(xe(),le(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--%NS%mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return t})();var iV=["*",[["mat-toolbar-row"]]],rV=["*","mat-toolbar-row"],oV=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),hf=(()=>{class t{_elementRef=d(k);_platform=d(me);_document=d(R);color;_toolbarRows;ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&bn(o,oV,5),i&2){let s;q(s=Y())&&(r._toolbarRows=s)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(ft(r.color?"mat-"+r.color:""),Z("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:rV,decls:2,vars:0,template:function(i,r){i&1&&(xe(iV),le(0),le(1,1))},styles:[`.mat-toolbar {
  background: var(--%NS%mat-toolbar-container-background-color, var(--%NS%mat-sys-surface));
  color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--%NS%mat-toolbar-title-text-font, var(--%NS%mat-sys-title-large-font));
  font-size: var(--%NS%mat-toolbar-title-text-size, var(--%NS%mat-sys-title-large-size));
  line-height: var(--%NS%mat-toolbar-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-weight: var(--%NS%mat-toolbar-title-text-weight, var(--%NS%mat-sys-title-large-weight));
  letter-spacing: var(--%NS%mat-toolbar-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --%NS%mat-button-text-label-text-color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
  --%NS%mat-button-outlined-label-text-color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--%NS%mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--%NS%mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--%NS%mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--%NS%mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2})}return t})();var sV=["*"],Q0=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:sV,decls:1,vars:0,template:function(i,r){i&1&&(xe(),le(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label, .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label, [dir=rtl] .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label, .mdc-form-field--align-end > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label, [dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2})}return t})();var aV=["input"],lV=["label"],cV=["*"],Yv={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},dV=new g("mat-checkbox-default-options",{providedIn:"root",factory:()=>Yv}),wt=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(wt||{}),Zv=class{source;checked},Kv=(()=>{class t{_elementRef=d(k);_changeDetectorRef=d(Fe);_ngZone=d(I);_animationsDisabled=Re();_options=d(dV,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new Zv;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new U;indeterminateChange=new U;value;disableRipple=!1;_inputElement;_labelElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=wt.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){d(Ke).load(Xi);let e=d(new mi("tabindex"),{optional:!0});this._options=this._options||Yv,this.color=this._options.color||Yv.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=d(je).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(wt.Indeterminate):this._transitionCheckState(this.checked?wt.Checked:wt.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=z(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?wt.Checked:wt.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case wt.Init:if(i===wt.Checked)return this._animationClasses.uncheckedToChecked;if(i==wt.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case wt.Unchecked:return i===wt.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case wt.Checked:return i===wt.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case wt.Indeterminate:return i===wt.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_onTouchTargetClick(){this._handleInputClick(),this.disabled||this._inputElement.nativeElement.focus()}_preventBubblingFromLabel(e){e.target&&this._labelElement.nativeElement.contains(e.target)&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&Pe(aV,5)(lV,5),i&2){let o;q(o=Y())&&(r._inputElement=o.first),q(o=Y())&&(r._labelElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(zt("id",r.id),ve("tabindex",null)("aria-label",null)("aria-labelledby",null),ft(r.color?"mat-"+r.color:"mat-accent"),Z("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",Q],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",Q],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",Q],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:St(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",Q],checked:[2,"checked","checked",Q],disabled:[2,"disabled","disabled",Q],indeterminate:[2,"indeterminate","indeterminate",Q]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Ae([{provide:Wr,useExisting:Ot(()=>t),multi:!0},{provide:Gr,useExisting:t,multi:!0}]),Ge],ngContentSelectors:cV,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target",3,"click"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-label",3,"for"]],template:function(i,r){if(i&1&&(xe(),b(0,"div",3),G("click",function(s){return r._preventBubblingFromLabel(s)}),b(1,"div",4,0)(3,"div",5),G("click",function(){return r._onTouchTargetClick()}),_(),b(4,"input",6,1),G("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),_(),O(6,"div",7),b(7,"div",8),wo(),b(8,"svg",9),O(9,"path",10),_(),Wc(),O(10,"div",11),_(),O(11,"div",12),_(),b(12,"label",13,2),le(14),_()()),i&2){let o=fi(2);j("labelPosition",r.labelPosition),C(4),Z("mdc-checkbox--selected",r.checked),j("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),ve("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),C(7),j("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),C(),j("for",r.inputId)}},dependencies:[io,Q0],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--%NS%mat-checkbox-state-layer-size, 40px);
  height: var(--%NS%mat-checkbox-state-layer-size, 40px);
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--%NS%mat-checkbox-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-hover-icon-color, var(--%NS%mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-focus-icon-color, var(--%NS%mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--%NS%mat-checkbox-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-checkbox-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-checkbox-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-checkbox-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-checkbox-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-checkbox-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
  color: var(--%NS%mat-checkbox-disabled-label-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
    color: GrayText;
  }
}
.mat-mdc-checkbox label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-checkbox-touch-target-size, 48px);
  width: var(--%NS%mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})(),J0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[Kv,Ie]})}return t})();var Ji=class t{http=d(La);utils=d(Gn);state=z({default:[],custom:[]});constructor(){this.http.get("/data/presets.json").subscribe(n=>{let e=n,i=JSON.parse(localStorage.getItem("customPresets")||"[]");this.state.set({default:e,custom:i})})}checkIfExists(n){return!![...this.state().custom,...this.state().default].find(e=>e.palette.name===n.name)}savePreset(n){this.state.set(N(p({},this.state()),{custom:[...this.state().custom,{type:"custom",codes:n.colors.flatMap(e=>this.colorToCode(e)),palette:n}]})),this._save()}updatePreset(n){this.state.set(N(p({},this.state()),{custom:[{type:"custom",codes:n.colors.flatMap(e=>this.colorToCode(e)),palette:n},...this.state().custom.filter(e=>e.palette.name!==n.name)]})),this._save()}deletePreset(n){this.state.set(N(p({},this.state()),{custom:[...this.state().custom.filter(e=>e!==n)]})),this._save()}colorToCode(n){let e=[];for(let i of n.shades){let r=[n.hue,n.saturation,i],o=this.utils.hslToRgb(r[0],r[1],r[2]);e.push(this.utils.rgbToHex(o[0],o[1],o[2]))}return e}_save(){localStorage.setItem("customPresets",JSON.stringify(this.state().custom))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})};function fV(t,n){}var er=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext;bindings};var Qv=(()=>{class t extends Yi{_elementRef=d(k);_focusTrapFactory=d(Sv);_config;_interactivityChecker=d(Cv);_ngZone=d(I);_focusMonitor=d(Zr);_renderer=d(De);_changeDetectorRef=d(Fe);_injector=d(P);_platform=d(me);_document=d(R);_portalOutlet;_focusTrapped=new S;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(er,{optional:!0})||new er,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||rt(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=vl(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=vl();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=vl()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&Pe(Zi,7),i&2){let o;q(o=Y())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&ve("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[Ne],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&Nt(0,fV,0,0,"ng-template",0)},dependencies:[Zi],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2,changeDetection:1})}return t})(),Fl=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new S;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!Dt(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},hV=new g("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(P);return()=>ps(t)}}),mV=new g("DialogData"),pV=new g("DefaultDialogConfig");function gV(t){let n=z(t),e=new U;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var ex=(()=>{class t{_injector=d(P);_defaultOptions=d(pV,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(nf);_idGenerator=d(je);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new S;_afterOpenedAtThisLevel=new S;_ariaHiddenElements=new Map;_scrollStrategy=d(hV);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=un(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(It(void 0)));open(e,i){let r=this._defaultOptions||new er;i=p(p({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=Ci(this._injector,o),a=new Fl(s,i),l=this._attachContainer(s,a,i);if(a.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(ze(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,a,l,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){Xv(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){Xv(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),Xv(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new yi({positionStrategy:e.positionStrategy||Ki().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:er,useValue:r},{provide:Fl,useValue:i},{provide:hs,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=Qv;let l=new xn(a,r.viewContainerRef,P.create({parent:o||this._injector,providers:s}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof Pt){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=p(p({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new bi(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new xn(e,o.viewContainerRef,s,null,o.bindings));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:mV,useValue:e.data},{provide:Fl,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(lt,null,{optional:!0}))&&a.push({provide:lt,useValue:gV(e.direction)}),P.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})();function Xv(t,n){let e=t.length;for(;e--;)n(t[e])}function vV(t,n){}var pf=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration;bindings},Jv="mdc-dialog--open",tx="mdc-dialog--opening",nx="mdc-dialog--closing",_V=150,bV=75,yV=(()=>{class t extends Qv{_animationStateChanged=new U;_animationsEnabled=!Re();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?rx(this._config.enterAnimationDuration)??_V:0;_exitAnimationDuration=this._animationsEnabled?rx(this._config.exitAnimationDuration)??bV:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(ix,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(tx,Jv)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Jv),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Jv),this._animationsEnabled?(this._hostElement.style.setProperty(ix,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(nx)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(tx,nx)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=bt(t)))(r||t)}})();static \u0275cmp=x({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(zt("id",r._config.id),ve("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),Z("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[Ne],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(b(0,"div",0)(1,"div",1),Nt(2,vV,0,0,"ng-template",2),_()())},dependencies:[Zi],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--%NS%mat-dialog-container-max-width, 560px);
  min-width: var(--%NS%mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--%NS%mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--%NS%mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--%NS%mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--%NS%mat-dialog-container-elevation-shadow, none);
  border-radius: var(--%NS%mat-dialog-container-shape, var(--%NS%mat-sys-corner-extra-large, 4px));
  background-color: var(--%NS%mat-dialog-container-color, var(--%NS%mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--%NS%mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--%NS%mat-dialog-subhead-color, var(--%NS%mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--%NS%mat-dialog-subhead-font, var(--%NS%mat-sys-headline-small-font, inherit));
  line-height: var(--%NS%mat-dialog-subhead-line-height, var(--%NS%mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--%NS%mat-dialog-subhead-size, var(--%NS%mat-sys-headline-small-size, 1rem));
  font-weight: var(--%NS%mat-dialog-subhead-weight, var(--%NS%mat-sys-headline-small-weight, 400));
  letter-spacing: var(--%NS%mat-dialog-subhead-tracking, var(--%NS%mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--%NS%mat-dialog-supporting-text-color, var(--%NS%mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--%NS%mat-dialog-supporting-text-font, var(--%NS%mat-sys-body-medium-font, inherit));
  line-height: var(--%NS%mat-dialog-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--%NS%mat-dialog-supporting-text-size, var(--%NS%mat-sys-body-medium-size, 1rem));
  font-weight: var(--%NS%mat-dialog-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight, 400));
  letter-spacing: var(--%NS%mat-dialog-supporting-text-tracking, var(--%NS%mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--%NS%mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--%NS%mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--%NS%mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--%NS%mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2,changeDetection:1})}return t})(),ix="--mat-dialog-transition-duration";function rx(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?_i(t.substring(0,t.length-2)):t.endsWith("s")?_i(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var mf=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(mf||{}),Ll=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new Ei(1);_beforeClosed=new Ei(1);_result;_closeFallbackTimeout;_state=mf.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(_e(r=>r.state==="opened"),ze(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(_e(r=>r.state==="closed"),ze(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),Qn(this.backdropClick(),this.keydownEvents().pipe(_e(r=>r.keyCode===27&&!this.disableClose&&!Dt(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),ox(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(_e(i=>i.state==="closing"),ze(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=mf.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=mf.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function ox(t,n,e){return t._closeInteractionType=n,t.close(e)}var e_=new g("MatMdcDialogData"),CV=new g("mat-mdc-dialog-default-options"),SV=new g("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(P);return()=>ps(t)}}),gf=(()=>{class t{_defaultOptions=d(CV,{optional:!0});_scrollStrategy=d(SV);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(je);_injector=d(P);_dialog=d(ex);_animationsDisabled=Re();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new S;_afterOpenedAtThisLevel=new S;dialogConfigClass=pf;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=un(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(It(void 0)));constructor(){this._dialogRefConstructor=Ll,this._dialogContainerType=yV,this._dialogDataToken=e_}open(e,i){let r;i=p(p({},this._defaultOptions||new pf),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,N(p({},i),{positionStrategy:Ki(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:er,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,l)=>(r=new this._dialogRefConstructor(s,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})}return t})(),sx=(()=>{class t{dialogRef=d(Ll,{optional:!0});_elementRef=d(k);_dialog=d(gf);ariaLabel;type="button";dialogResult;_matDialogClose;ngOnInit(){this.dialogRef||(this.dialogRef=ux(this._elementRef,this._dialog.openDialogs))}ngOnChanges(e){let i=e._matDialogClose;i&&(this.dialogResult=i.currentValue)}_onButtonClick(e){this._elementRef.nativeElement.getAttribute("aria-disabled")!=="true"&&ox(this.dialogRef,e.screenX===0&&e.screenY===0?"keyboard":"mouse",this.dialogResult)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","mat-dialog-close",""],["","matDialogClose",""]],hostVars:2,hostBindings:function(i,r){i&1&&G("click",function(s){return r._onButtonClick(s)}),i&2&&ve("aria-label",r.ariaLabel||null)("type",r.type)},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],type:"type",dialogResult:[0,"mat-dialog-close","dialogResult"],_matDialogClose:[0,"matDialogClose","_matDialogClose"]},exportAs:["matDialogClose"],features:[Ge]})}return t})(),ax=(()=>{class t{_dialogRef=d(Ll,{optional:!0});_elementRef=d(k);_dialog=d(gf);ngOnInit(){this._dialogRef||(this._dialogRef=ux(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t})}return t})(),lx=(()=>{class t extends ax{id=d(je).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=bt(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&zt("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[Ne]})}return t})(),cx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[Cp([Tv])]})}return t})(),dx=(()=>{class t extends ax{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=bt(t)))(r||t)}})();static \u0275dir=M({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&Z("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[Ne]})}return t})();function ux(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var DV=()=>({success:!1}),wV=()=>({success:!0}),vf=class t{data=d(e_);palette=$S(this.data.palette);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-palette-save-dialog"]],inputs:{palette:[1,"palette"]},outputs:{palette:"paletteChange"},decls:10,vars:6,consts:[["mat-dialog-title",""],["matButton","",3,"mat-dialog-close"],["matButton","","cdkFocusInitial","",3,"mat-dialog-close"]],template:function(e,i){e&1&&(b(0,"h2",0),X(1),_(),b(2,"mat-dialog-content")(3,"p"),X(4),_()(),b(5,"mat-dialog-actions")(6,"button",1),X(7,"No"),_(),b(8,"button",2),X(9,"Yes"),_()()),e&2&&(C(),yn("A preset for ",i.palette().name," already exists"),C(3),yn("Do you want to overwrite ",i.palette().name,"?"),C(2),j("mat-dialog-close",Ld(4,DV)),C(2),j("mat-dialog-close",Ld(5,wV)))},dependencies:[Uv,Qi,lx,cx,dx,sx],encapsulation:2})};function EV(t,n){if(t&1){let e=rn();b(0,"button",18),G("click",function(){Bt(e);let r=re().$index,o=re();return jt(o.removeColor(r))}),b(1,"mat-icon"),X(2,"remove"),_()()}}function xV(t,n){if(t&1&&(O(0,"mat-checkbox",13),en()),t&2){let e=re().$implicit;j("formField",e.lockHue),tn()}}function IV(t,n){if(t&1&&(O(0,"mat-checkbox",13),en()),t&2){let e=re().$implicit;j("formField",e.lockSaturation),tn()}}function TV(t,n){if(t&1){let e=rn();b(0,"mat-form-field",16)(1,"button",19),G("click",function(){let r=Bt(e).$index,o=re().$index,s=re();return jt(s.removeShade(o,r))}),b(2,"mat-icon"),X(3,"remove"),_()(),O(4,"input",20),en(),_()}if(t&2){let e=n.$implicit,i=re().$implicit;C(),j("disabled",i.lockShade().value()),C(3),j("formField",e),tn()}}function MV(t,n){if(t&1&&(O(0,"mat-checkbox",13),en()),t&2){let e=re().$implicit;j("formField",e.lockShade),tn()}}function NV(t,n){if(t&1){let e=rn();b(0,"div",6)(1,"mat-toolbar")(2,"h2"),X(3),_(),ee(4,EV,3,0,"button",9),_(),b(5,"div",10)(6,"mat-label"),X(7,"Hue"),_(),b(8,"mat-slider",11),O(9,"input",12),en(),_(),ee(10,xV,1,1,"mat-checkbox",13),_(),b(11,"div",10)(12,"mat-label"),X(13,"Saturation"),_(),b(14,"mat-slider",14),O(15,"input",12),en(),_(),ee(16,IV,1,1,"mat-checkbox",13),_(),b(17,"div",15),st(18,TV,5,2,"mat-form-field",16,_n),b(20,"button",17),G("click",function(){let r=Bt(e).$index,o=re();return jt(o.addShade(r))}),X(21," Add shade "),b(22,"mat-icon",8),X(23,"add"),_()(),ee(24,MV,1,1,"mat-checkbox",13),_()()}if(t&2){let e=n.$implicit,i=n.$index;C(3),Ct(e.name().value()),C(),te(i>0?4:-1),C(4),j("disabled",e.lockHue().value()),C(),j("formField",e.hue),tn(),C(),te(i>0?10:-1),C(4),j("disabled",e.lockSaturation().value()),C(),j("formField",e.saturation),tn(),C(),te(i>0?16:-1),C(2),at(e.shades),C(2),j("disabled",e.lockShade().value()),C(4),te(i>0?24:-1)}}var _f=class t{colorState=d(cn);presetState=d(Ji);snackBar=d(ys);dialog=d(gf);stateForm=this.colorState.stateForm;addShade=this.colorState.addShade;addColor=this.colorState.addColor;removeShade=this.colorState.removeShade;removeColor=this.colorState.removeColor;savePreset(){let n=this.colorState.state();this.presetState.checkIfExists(n)?this.dialog.open(vf,{data:{palette:n}}).afterClosed().subscribe(({success:r})=>{r&&(this.presetState.updatePreset(n),this.snackBar.open(`Preset "${n.name}" updated`,"Close",{duration:5e3}))}):(this.presetState.savePreset(n),this.snackBar.open(`Preset "${n.name}" saved`,"Close",{duration:5e3}))}resetForm(){this.colorState.reset()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-form"]],decls:17,vars:1,consts:[[1,"header"],["matNativeControl","","input","text",3,"formField"],[1,"spacer"],["matButton","",3,"click"],["matButton","filled",3,"click"],[1,"color-list"],[1,"color"],["matButton","","aria-label","Add color",3,"click"],["matSuffix",""],["matIconButton","","aria-label","Remove color"],[1,"control"],["min","1","max","360","step","1","discrete","",3,"disabled"],["matSliderThumb","",3,"formField"],[3,"formField"],["min","0","max","1","step","0.01","discrete","",3,"disabled"],[1,"shades"],["appearance","fill"],["matButton","","aria-label","Add shade",3,"click","disabled"],["matIconButton","","aria-label","Remove color",3,"click"],["matSuffix","","matIconButton","","aria-label","Remove shade",1,"top-right-icon",3,"click","disabled"],["matInput","","type","number","step","0.01",3,"formField"]],template:function(e,i){e&1&&(b(0,"div",0)(1,"mat-form-field")(2,"mat-label"),X(3,"Name"),_(),O(4,"input",1),en(),_(),O(5,"div",2),b(6,"button",3),G("click",function(){return i.resetForm()}),X(7,"New"),_(),b(8,"button",4),G("click",function(){return i.savePreset()}),X(9,"Save Preset"),_()(),b(10,"div",5),st(11,NV,25,10,"div",6,Po),_(),b(13,"button",7),G("click",function(){return i.addColor()}),X(14," Add color "),b(15,"mat-icon",8),X(16,"add"),_()()),e&2&&(C(4),j("formField",i.stateForm.name),tn(),C(7),at(i.stateForm.colors))},dependencies:[x0,E0,Gv,$0,z0,Ol,Ds,df,Bu,X0,jv,Qi,hf,J0,Kv],styles:[".header[_ngcontent-%COMP%]{display:flex;gap:1em}.header[_ngcontent-%COMP%]   .spacer[_ngcontent-%COMP%]{flex:1 0 auto}.color-list[_ngcontent-%COMP%]{display:grid;gap:1em;grid-template-columns:1fr;grid-template-rows:min-content;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));margin-bottom:1em;--%NS%mat-toolbar-container-background-color: transparent;--%NS%mat-toolbar-standard-height: 34px}.color[_ngcontent-%COMP%]{background:#efefef;padding:1em 2em;border-radius:1em}.color[_ngcontent-%COMP%]   mat-toolbar[_ngcontent-%COMP%]{justify-content:space-between;padding:0}.color[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{font-weight:700;font-size:16px}.color[_ngcontent-%COMP%]   mat-form-field[_ngcontent-%COMP%]{padding-right:1em;width:9em;--%NS%mat-form-field-container-text-size: 12px;--%NS%mat-icon-button-state-layer-size: 28px;--%NS%mat-form-field-container-vertical-padding: 3px;--%NS%mat-form-field-container-height: 28px;--%NS%mat-icon-button-touch-target-size: 28px}.color[_ngcontent-%COMP%]   mat-form-field[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{width:16px;height:16px;font-size:16px;margin-left:6px}"]})};var fx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[Ie]})}return t})();var Vl=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new S;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var hx=(()=>{class t{_animationsDisabled=Re();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&Z("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--%NS%mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--%NS%mat-pseudo-checkbox-full-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--%NS%mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--%NS%mat-pseudo-checkbox-full-selected-icon-color, var(--%NS%mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-full-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return t})();var kV=["text"],AV=[[["mat-icon"]],"*"],RV=["mat-icon","*"];function OV(t,n){if(t&1&&O(0,"mat-pseudo-checkbox",1),t&2){let e=re();j("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function PV(t,n){if(t&1&&O(0,"mat-pseudo-checkbox",3),t&2){let e=re();j("disabled",e.disabled)}}function FV(t,n){if(t&1&&(b(0,"span",4),X(1),_()),t&2){let e=re();C(),yn("(",e.group.label,")")}}var n_=new g("MAT_OPTION_PARENT_COMPONENT"),i_=new g("MatOptgroup");var t_=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},Es=(()=>{class t{_element=d(k);_changeDetectorRef=d(Fe);_parent=d(n_,{optional:!0});group=d(i_,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(je).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=z(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new U;_text;_stateChanges=new S;constructor(){let e=d(Ke);e.load(Xi),e.load(cs),this._signalDisableRipple=!!this._parent&&Ut(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!Dt(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new t_(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Pe(kV,7),i&2){let o;q(o=Y())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&G("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(zt("id",r.id),ve("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),Z("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",Q]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:RV,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(xe(AV),ee(0,OV,1,2,"mat-pseudo-checkbox",1),le(1),b(2,"span",2,0),le(4,1),_(),ee(5,PV,1,1,"mat-pseudo-checkbox",3),ee(6,FV,2,1,"span",4),O(7,"div",5)),i&2&&(te(r.multiple?0:-1),C(5),te(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),C(),te(r.group&&r.group._inert?6:-1),C(),j("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[hx,io],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--%NS%mat-option-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-option-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-option-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-option-label-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-option-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  font-weight: var(--%NS%mat-option-label-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--%NS%mat-option-hover-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--%NS%mat-option-focus-state-layer-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--%NS%selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--%NS%mat-option-selected-state-layer-color, var(--%NS%mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--%NS%selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--%NS%mat-option-selected-state-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-option-selected-state-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--%NS%selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--%NS%selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --%NS%mat-list-list-item-selected-container-color: var(--%NS%mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})();function mx(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function px(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var gx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[Ie]})}return t})();var r_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[_s,gx,Es,Ie]})}return t})();var LV=["trigger"],VV=["panel"],BV=[[["mat-select-trigger"]],"*"],jV=["mat-select-trigger","*"];function UV(t,n){if(t&1&&(b(0,"span",4),X(1),_()),t&2){let e=re();C(),Ct(e.placeholder)}}function HV(t,n){t&1&&le(0)}function zV(t,n){if(t&1&&(b(0,"span",11),X(1),_()),t&2){let e=re(2);C(),Ct(e.triggerValue)}}function $V(t,n){if(t&1&&(b(0,"span",5),ee(1,HV,1,0)(2,zV,2,1,"span",11),_()),t&2){let e=re();C(),te(e.customTrigger?1:2)}}function WV(t,n){if(t&1){let e=rn();b(0,"div",12,1),G("keydown",function(r){Bt(e);let o=re();return jt(o._handleKeydown(r))}),le(2,1),_()}if(t&2){let e=re();ft(e.panelClass),Z("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),ve("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var GV=new g("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(P);return()=>no(t)}}),qV=new g("MAT_SELECT_CONFIG"),YV=new g("MatSelectTrigger"),o_=class{source;value;constructor(n,e){this.source=n,this.value=e}},vx=(()=>{class t{_viewportRuler=d(qi);_changeDetectorRef=d(Fe);_elementRef=d(k);_dir=d(lt,{optional:!0});_idGenerator=d(je);_renderer=d(De);_parentFormField=d(Rl,{optional:!0});ngControl=d(Hi,{self:!0,optional:!0});_liveAnnouncer=d(yl);_defaultOptions=d(qV,{optional:!0});_animationsDisabled=Re();_popoverLocation;_initialized=new S;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=mx(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=px(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new o_(this,e)}_scrollStrategyFactory=d(GV);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new S;_errorStateTracker;stateChanges=new S;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=z(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Wn.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=un(()=>{let e=this.options;return e?e.changes.pipe(It(e),ct(()=>Qn(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(ct(()=>this.optionSelectionChanges))});openedChange=new U;_openedStream=this.openedChange.pipe(_e(e=>e),ie(()=>{}));_closedStream=this.openedChange.pipe(_e(e=>!e),ie(()=>{}));selectionChange=new U;valueChange=new U;constructor(){let e=d(uf),i=d(sl,{optional:!0}),r=d(al,{optional:!0}),o=d(new mi("tabindex"),{optional:!0}),s=d(Il,{optional:!0}),a=d(as,{optional:!0,self:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new ws(e,a||this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Vl(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(be(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(be(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(It(null),be(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe(ze(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!Dt(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let l=this.selected;l&&a!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!Dt(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(a?l.select():l.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!Dt(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof ms?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Cl(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Qn(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(be(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Qn(...this.options.map(i=>i._stateChanges)).pipe(be(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=pt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&bn(o,YV,5)(o,Es,5)(o,i_,5),i&2){let s;q(s=Y())&&(r.customTrigger=s.first),q(s=Y())&&(r.options=s),q(s=Y())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&Pe(LV,5)(VV,5)(rf,5),i&2){let o;q(o=Y())&&(r.trigger=o.first),q(o=Y())&&(r.panel=o.first),q(o=Y())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&G("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(ve("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),Z("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",Q],disableRipple:[2,"disableRipple","disableRipple",Q],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:St(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",Q],placeholder:"placeholder",required:[2,"required","required",Q],multiple:[2,"multiple","multiple",Q],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",Q],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",St],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",Q]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Ae([{provide:Al,useExisting:t},{provide:n_,useExisting:t}]),Ge],ngContentSelectors:jV,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(xe(BV),b(0,"div",2,0),G("click",function(){return r.open()}),b(3,"div",3),ee(4,UV,2,1,"span",4)(5,$V,3,1,"span",5),_(),b(6,"div",6)(7,"div",7),wo(),b(8,"svg",8),O(9,"path",9),_()()()(),Nt(10,WV,3,16,"ng-template",10),G("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=fi(1);C(3),ve("id",r._valueId),C(),te(r.empty?4:5),C(6),j("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[ms,rf],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--%NS%mat-select-enabled-trigger-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-select-trigger-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-select-trigger-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-select-trigger-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-select-trigger-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-select-trigger-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--%NS%mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--%NS%mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--%NS%mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-select-invalid-arrow-color, var(--%NS%mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--%NS%mat-select-enabled-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--%NS%mat-select-focused-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--%NS%mat-select-disabled-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--%NS%mat-select-panel-background-color, var(--%NS%mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--%NS%mat-select-placeholder-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--%NS%mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2})}return t})();var _x=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[gs,r_,Ie,Sl,Pl,r_]})}return t})();function KV(t,n){if(t&1&&O(0,"pbu-color-copy",1),t&2){let e=n.$implicit,i=re().$implicit;j("hue",i.hue)("saturation",i.saturation)("light",e)}}function XV(t,n){if(t&1&&(b(0,"div",0),st(1,KV,1,3,"pbu-color-copy",1,_n),_()),t&2){let e=n.$implicit;C(),at(e.shades)}}var Cf=class t{colorState=d(cn);colors=E(()=>this.colorState.state().colors);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-strips"]],decls:2,vars:0,consts:[[1,"shade-list"],[3,"hue","saturation","light"]],template:function(e,i){e&1&&st(0,XV,3,0,"div",0,_n),e&2&&at(i.colors())},dependencies:[Cs],styles:["[_nghost-%COMP%]{display:flex;flex-flow:column nowrap;width:100%}.shade-list[_ngcontent-%COMP%]{display:flex;flex:1 0 auto}.shade-list[_ngcontent-%COMP%]   *[_ngcontent-%COMP%]{width:auto;height:auto;flex:1 0 auto}"]})};var Sf=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-header"]],decls:4,vars:0,consts:[[1,"spacer"]],template:function(e,i){e&1&&(b(0,"mat-toolbar")(1,"h1"),X(2,"Palette Builder"),_(),O(3,"span",0),_())},dependencies:[hf],styles:[".spacer[_ngcontent-%COMP%]{flex:1 0 auto}"]})};var Df=class t{state=z({display:{color:"wheel",form:"edit"}});stateForm=hl(this.state);static \u0275fac=function(e){return new(e||t)};static \u0275prov=w({token:t,factory:t.\u0275fac})};var QV=["*"];var JV=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],eB=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],tB=new g("MAT_CARD_CONFIG"),bx=(()=>{class t{appearance;constructor(){let e=d(tB,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&Z("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:QV,decls:1,vars:0,template:function(i,r){i&1&&(xe(),le(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-elevated-container-elevation, var(--%NS%mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--%NS%mat-card-outlined-container-color, var(--%NS%mat-sys-surface));
  border-radius: var(--%NS%mat-card-outlined-container-shape, var(--%NS%mat-sys-corner-medium));
  border-width: var(--%NS%mat-card-outlined-outline-width, 1px);
  border-color: var(--%NS%mat-card-outlined-outline-color, var(--%NS%mat-sys-outline-variant));
  box-shadow: var(--%NS%mat-card-outlined-container-elevation, var(--%NS%mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--%NS%mat-card-filled-container-color, var(--%NS%mat-sys-surface-container-highest));
  border-radius: var(--%NS%mat-card-filled-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-filled-container-elevation, var(--%NS%mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--%NS%mat-card-title-text-font, var(--%NS%mat-sys-title-large-font));
  line-height: var(--%NS%mat-card-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-size: var(--%NS%mat-card-title-text-size, var(--%NS%mat-sys-title-large-size));
  letter-spacing: var(--%NS%mat-card-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  font-weight: var(--%NS%mat-card-title-text-weight, var(--%NS%mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--%NS%mat-card-subtitle-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-card-subtitle-text-font, var(--%NS%mat-sys-title-medium-font));
  line-height: var(--%NS%mat-card-subtitle-text-line-height, var(--%NS%mat-sys-title-medium-line-height));
  font-size: var(--%NS%mat-card-subtitle-text-size, var(--%NS%mat-sys-title-medium-size));
  letter-spacing: var(--%NS%mat-card-subtitle-text-tracking, var(--%NS%mat-sys-title-medium-tracking));
  font-weight: var(--%NS%mat-card-subtitle-text-weight, var(--%NS%mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})}return t})(),yx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var Cx=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=M({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})(),Sx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:eB,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(xe(JV),le(0),ut(1,"div",0),le(2,1),yt(),le(3,2))},encapsulation:2})}return t})();var Dx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=K({type:t});static \u0275inj=W({imports:[Ie]})}return t})();function iB(t,n){if(t&1&&O(0,"div",5),t&2){let e=n.$implicit;on("background-color",e)}}function rB(t,n){if(t&1){let e=rn();b(0,"button",4),G("click",function(){Bt(e);let r=re();return jt(r.deletePreset(r.preset()))}),X(1,"Delete"),_()}}var wf=class t{colorState=d(cn);presetState=d(Ji);snackBar=d(ys);preset=ht.required();loadPreset(n){let{name:e,colors:i}=n.palette;this.colorState.setColor(e,i),this.snackBar.open(`Preset "${e}" loaded`,"Close",{duration:5e3})}deletePreset(n){this.presetState.deletePreset(n),this.snackBar.open(`Preset "${n.palette.name}" deleted`,"Close",{duration:5e3})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-preset-card"]],inputs:{preset:[1,"preset"]},decls:11,vars:2,consts:[["appearance","outlined",1,"preset-card"],[1,"colors"],[1,"color",3,"backgroundColor"],["matButton",""],["matButton","",3,"click"],[1,"color"]],template:function(e,i){e&1&&(b(0,"mat-card",0)(1,"mat-card-header")(2,"mat-card-title"),X(3),_()(),b(4,"div",1),st(5,iB,1,2,"div",2,Po),_(),b(7,"mat-card-actions"),ee(8,rB,2,0,"button",3),b(9,"button",4),G("click",function(){return i.loadPreset(i.preset())}),X(10,"Load Preset"),_()()()),e&2&&(C(3),Ct(i.preset().palette.name),C(2),at(i.preset().codes),C(3),te(i.preset().type==="custom"?8:-1))},dependencies:[Dx,bx,Cx,Sx,yx,Qi],styles:[".colors[_ngcontent-%COMP%]{background:pink;aspect-ratio:16/9;display:flex;align-items:stretch}.colors[_ngcontent-%COMP%]   *[_ngcontent-%COMP%]{flex:1}"]})};var wx=(t,n)=>n.palette.name;function oB(t,n){t&1&&(b(0,"h2"),X(1,"Custom Presets"),_())}function sB(t,n){if(t&1&&O(0,"pbu-preset-card",1),t&2){let e=n.$implicit;j("preset",e)}}function aB(t,n){if(t&1&&O(0,"pbu-preset-card",1),t&2){let e=n.$implicit;j("preset",e)}}var Ef=class t{colorState=d(cn);presetState=d(Ji);state=this.presetState.state;customPresets=E(()=>this.state().custom);defaultPresets=E(()=>this.state().default);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-preset-list"]],decls:9,vars:1,consts:[[1,"list"],[3,"preset"]],template:function(e,i){e&1&&(ee(0,oB,2,0,"h2"),b(1,"div",0),st(2,sB,1,1,"pbu-preset-card",1,wx),_(),b(4,"h2"),X(5,"Default Presets"),_(),b(6,"div",0),st(7,aB,1,1,"pbu-preset-card",1,wx),_()),e&2&&(te(i.customPresets().length?0:-1),C(2),at(i.customPresets()),C(5),at(i.defaultPresets()))},dependencies:[wf],styles:[".list[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,1fr);gap:1em;margin-bottom:2em}"]})};function lB(t,n){t&1&&O(0,"pbu-color-wheel")}function cB(t,n){t&1&&O(0,"pbu-color-strips")}function dB(t,n){t&1&&O(0,"pbu-color-form")}function uB(t,n){t&1&&O(0,"pbu-preset-list")}var xf=class t{globalState=d(Df);display=this.globalState.stateForm.display;static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["app-root"]],decls:17,vars:4,consts:[[1,"col-palette"],[3,"formField"],["value","wheel"],["value","strips"],[1,"col-colors"],["value","edit"],["value","presets"]],template:function(e,i){if(e&1&&(O(0,"pbu-header"),b(1,"div",0)(2,"mat-select",1)(3,"mat-option",2),X(4,"Color Wheel"),_(),b(5,"mat-option",3),X(6,"Strips"),_()(),en(),ee(7,lB,1,0,"pbu-color-wheel")(8,cB,1,0,"pbu-color-strips"),_(),b(9,"div",4)(10,"mat-select",1)(11,"mat-option",5),X(12,"Build Palette"),_(),b(13,"mat-option",6),X(14,"Preset List"),_()(),en(),ee(15,dB,1,0,"pbu-color-form")(16,uB,1,0,"pbu-preset-list"),_()),e&2){let r,o;C(2),j("formField",i.display.color),tn(),C(5),te((r=i.display.color().value())==="wheel"?7:r==="strips"?8:-1),C(3),j("formField",i.display.form),tn(),C(5),te((o=i.display.form().value())==="edit"?15:o==="presets"?16:-1)}},dependencies:[af,_f,Cf,fx,_x,vx,Es,Bu,Sf,Ef],styles:['pbu-header[_ngcontent-%COMP%]{grid-area:title}[_nghost-%COMP%]{display:grid;grid-template-rows:min-content 1fr;grid-template-columns:1fr 2fr;grid-template-areas:"title title" "left right";gap:2em;padding:1em;height:100%}.col-palette[_ngcontent-%COMP%]{border:1px solid #dfdfdf;border-radius:10px;display:grid;grid-template-rows:min-content 1fr;gap:1em;padding:1em 2em;overflow-x:hidden;overflow-y:auto}.col-colors[_ngcontent-%COMP%]{border:1px solid #dfdfdf;border-radius:10px;padding:1em 2em;display:grid;grid-template-rows:min-content 1fr;gap:1em;overflow-x:hidden;overflow-y:auto}pbu-color-wheel[_ngcontent-%COMP%]{margin:2em 0}']})};Up(xf,mw).catch(t=>console.error(t));
