var Rx=Object.defineProperty,Ox=Object.defineProperties;var Px=Object.getOwnPropertyDescriptors;var y_=Object.getOwnPropertySymbols;var Fx=Object.prototype.hasOwnProperty,Lx=Object.prototype.propertyIsEnumerable;var C_=(t,n,e)=>n in t?Rx(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,p=(t,n)=>{for(var e in n||={})Fx.call(n,e)&&C_(t,e,n[e]);if(y_)for(var e of y_(n))Lx.call(n,e)&&C_(t,e,n[e]);return t},P=(t,n)=>Ox(t,Px(n));var Mt=null,Kl=!1,ur=1,Vx=null,Ne=Symbol("SIGNAL");function B(t){let n=Mt;return Mt=t,n}function Xl(){return Mt}var Qn={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Jn(t){if(Kl)throw new Error("");if(Mt===null)return;Mt.consumerOnSignalRead(t);let n=Mt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=Mt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:Mt.producers,e!==void 0&&e.producer===t)){Mt.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=ur;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===Mt&&(!i||r.knownValidAtEpoch===ur))return;let o=go(Mt),s={producer:t,consumer:Mt,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:ur,lastReadVersion:t.version,nextConsumer:void 0};Mt.producersTail=s,n!==void 0?n.nextProducer=s:Mt.producers=s,o&&x_(t,s)}function S_(){ur++}function mr(t){if(!(go(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===ur)){if(!t.producerMustRecompute(t)&&!pr(t)){po(t);return}t.producerRecomputeValue(t),po(t)}}function Wf(t){if(t.consumers===void 0)return;let n=Kl;Kl=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||D_(i)}}finally{Kl=n}}function Gf(){return Mt?.consumerAllowSignalWrites!==!1}function D_(t){t.dirty=!0,Wf(t),t.consumerMarkedDirty?.(t)}function po(t){t.dirty=!1,t.lastCleanEpoch=ur}function On(t){return t&&E_(t),B(t)}function E_(t){if(t.producersTail?.knownValidAtEpoch===ur){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function ei(t,n){B(n),t&&w_(t)}function w_(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(go(t))do e=qf(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function pr(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(mr(e),i!==e.version))return!0}return!1}function ti(t){if(go(t)){let n=t.producers;for(;n!==void 0;)n=qf(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function x_(t,n){let e=t.consumersTail,i=go(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)x_(r.producer,r)}function qf(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!go(n)){let o=n.producers;for(;o!==void 0;)o=qf(o)}return e}function go(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Ps(t){Vx?.(t)}function Fs(t,n){return Object.is(t,n)}function Ls(t,n){let e=Object.create(Bx);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(mr(e),Jn(e),e.value===Rn)throw e.error;return e.value};return i[Ne]=e,Ps(e),i}var fr=Symbol("UNSET"),hr=Symbol("COMPUTING"),Rn=Symbol("ERRORED"),Bx=P(p({},Qn),{value:fr,dirty:!0,error:null,equal:Fs,kind:"computed",producerMustRecompute(t){return t.value===fr||t.value===hr},producerRecomputeValue(t){if(t.value===hr)throw new Error("");let n=t.value;t.value=hr;let e=On(t),i,r=!1;try{i=t.computation(),B(null),r=n!==fr&&n!==Rn&&i!==Rn&&t.equal(n,i)}catch(o){i=Rn,t.error=o}finally{ei(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function jx(){throw new Error}var I_=jx;function T_(t){I_(t)}function Yf(t){I_=t}var Ux=null;function Zf(t,n){let e=Object.create(Vs);e.value=t,n!==void 0&&(e.equal=n);let i=()=>M_(e);return i[Ne]=e,Ps(e),[i,s=>ki(e,s),s=>Ql(e,s)]}function M_(t){return Jn(t),t.value}function ki(t,n){Gf()||T_(t),t.equal(t.value,n)||(t.value=n,Hx(t))}function Ql(t,n){Gf()||T_(t),ki(t,n(t.value))}var Vs=P(p({},Qn),{equal:Fs,value:void 0,kind:"signal"});function Hx(t){t.version++,S_(),Wf(t),Ux?.(t)}var Kf=P(p({},Qn),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Xf(t){if(t.dirty=!1,t.version>0&&!pr(t))return;t.version++;let n=On(t);try{t.cleanup(),t.fn()}finally{ei(t,n)}}var Qf;function Jl(){return Qf}function Pn(t){let n=Qf;return Qf=t,n}var N_=Symbol("NotFound");function vo(t){return t===N_||t?.name==="\u0275NotFound"}function Jf(t,n,e){let i=Object.create(zx);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(mr(i),Jn(i),i.value===Rn)throw i.error;return i.value};return o[Ne]=i,Ps(i),o}function eh(t,n){mr(t),ki(t,n),po(t)}function k_(t,n){if(mr(t),t.value===Rn)throw t.error;Ql(t,n),po(t)}var zx=P(p({},Qn),{value:fr,dirty:!0,error:null,equal:Fs,kind:"linkedSignal",producerMustRecompute(t){return t.value===fr||t.value===hr},producerRecomputeValue(t){if(t.value===hr)throw new Error("");let n=t.value;t.value=hr;let e=On(t),i,r=!1;try{let o=t.source(),s=n!==fr&&n!==Rn,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,B(null),r=s&&i!==Rn&&t.equal(n,i)}catch(o){i=Rn,t.error=o}finally{ei(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function A_(t){let n=B(null);try{return t()}finally{B(n)}}function ce(t){return typeof t=="function"}function _o(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var ec=_o(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function gr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var ge=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(ce(i))try{i()}catch(o){n=o instanceof ec?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{R_(o)}catch(s){n=n??[],s instanceof ec?n=[...n,...s.errors]:n.push(s)}}if(n)throw new ec(n)}}add(n){var e;if(n&&n!==this)if(this.closed)R_(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&gr(e,n)}remove(n){let{_finalizers:e}=this;e&&gr(e,n),n instanceof t&&n._removeParent(this)}};ge.EMPTY=(()=>{let t=new ge;return t.closed=!0,t})();var th=ge.EMPTY;function tc(t){return t instanceof ge||t&&"closed"in t&&ce(t.remove)&&ce(t.add)&&ce(t.unsubscribe)}function R_(t){ce(t)?t():t.unsubscribe()}var mn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var bo={setTimeout(t,n,...e){let{delegate:i}=bo;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=bo;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function nc(t){bo.setTimeout(()=>{let{onUnhandledError:n}=mn;if(n)n(t);else throw t})}function Bs(){}var O_=nh("C",void 0,void 0);function P_(t){return nh("E",void 0,t)}function F_(t){return nh("N",t,void 0)}function nh(t,n,e){return{kind:t,value:n,error:e}}var vr=null;function yo(t){if(mn.useDeprecatedSynchronousErrorHandling){let n=!vr;if(n&&(vr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=vr;if(vr=null,e)throw i}}else t()}function L_(t){mn.useDeprecatedSynchronousErrorHandling&&vr&&(vr.errorThrown=!0,vr.error=t)}var _r=class extends ge{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,tc(n)&&n.add(this)):this.destination=Gx}static create(n,e,i){return new ni(n,e,i)}next(n){this.isStopped?rh(F_(n),this):this._next(n)}error(n){this.isStopped?rh(P_(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?rh(O_,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},$x=Function.prototype.bind;function ih(t,n){return $x.call(t,n)}var oh=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){ic(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){ic(i)}else ic(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){ic(e)}}},ni=class extends _r{constructor(n,e,i){super();let r;if(ce(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&mn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&ih(n.next,o),error:n.error&&ih(n.error,o),complete:n.complete&&ih(n.complete,o)}):r=n}this.destination=new oh(r)}};function ic(t){mn.useDeprecatedSynchronousErrorHandling?L_(t):nc(t)}function Wx(t){throw t}function rh(t,n){let{onStoppedNotification:e}=mn;e&&bo.setTimeout(()=>e(t,n))}var Gx={closed:!0,next:Bs,error:Wx,complete:Bs};var Co=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Ht(t){return t}function sh(...t){return ah(t)}function ah(t){return t.length===0?Ht:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ie=class t{constructor(n){n&&(this._subscribe=n)}lift(n){let e=new t;return e.source=this,e.operator=n,e}subscribe(n,e,i){let r=Yx(n)?n:new ni(n,e,i);return yo(()=>{let{operator:o,source:s}=this;r.add(o?o.call(r,s):s?this._subscribe(r):this._trySubscribe(r))}),r}_trySubscribe(n){try{return this._subscribe(n)}catch(e){n.error(e)}}forEach(n,e){return e=V_(e),new e((i,r)=>{let o=new ni({next:s=>{try{n(s)}catch(a){r(a),o.unsubscribe()}},error:r,complete:i});this.subscribe(o)})}_subscribe(n){var e;return(e=this.source)===null||e===void 0?void 0:e.subscribe(n)}[Co](){return this}pipe(...n){return ah(n)(this)}toPromise(n){return n=V_(n),new n((e,i)=>{let r;this.subscribe(o=>r=o,o=>i(o),()=>e(r))})}};ie.create=t=>new ie(t);function V_(t){var n;return(n=t??mn.Promise)!==null&&n!==void 0?n:Promise}function qx(t){return t&&ce(t.next)&&ce(t.error)&&ce(t.complete)}function Yx(t){return t&&t instanceof _r||qx(t)&&tc(t)}function Zx(t){return ce(t?.lift)}function ue(t){return n=>{if(Zx(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function fe(t,n,e,i,r){return new lh(t,n,e,i,r)}var lh=class extends _r{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(l){n.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var B_=_o(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var D=class extends ie{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let e=new rc(this,this);return e.operator=n,e}_throwIfClosed(){if(this.closed)throw new B_}next(n){yo(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let e of this.currentObservers)e.next(n)}})}error(n){yo(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:e}=this;for(;e.length;)e.shift().error(n)}})}complete(){yo(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:e,isStopped:i,observers:r}=this;return e||i?th:(this.currentObservers=null,r.push(n),new ge(()=>{this.currentObservers=null,gr(r,n)}))}_checkFinalizedStatuses(n){let{hasError:e,thrownError:i,isStopped:r}=this;e?n.error(i):r&&n.complete()}asObservable(){let n=new ie;return n.source=this,n}};D.create=(t,n)=>new rc(t,n);var rc=class extends D{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:th}};var Je=class extends D{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var js={now(){return(js.delegate||Date).now()},delegate:void 0};var Ai=class extends D{constructor(n=1/0,e=1/0,i=js){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let l=1;l<i.length&&i[l]<=s;l+=2)a=l;a&&i.splice(0,a+1)}}};var oc=class extends ge{constructor(n,e){super()}schedule(n,e=0){return this}};var Us={setInterval(t,n,...e){let{delegate:i}=Us;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Us;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var sc=class extends oc{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Us.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Us.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,gr(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var ch=(()=>{class t{constructor(e,i=t.now){this.schedulerActionCtor=e,this.now=i}schedule(e,i=0,r){return new this.schedulerActionCtor(this,e).schedule(r,i)}}return t.now=js.now,t})();var ac=class extends ch{constructor(n,e=ch.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Hs=new ac(sc),j_=Hs;var ze=new ie(t=>t.complete());function lc(t){return t&&ce(t.schedule)}function dh(t){return t[t.length-1]}function cc(t){return ce(dh(t))?t.pop():void 0}function Fn(t){return lc(dh(t))?t.pop():void 0}function U_(t,n){return typeof dh(t)=="number"?t.pop():n}function z_(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(u){try{c(i.next(u))}catch(f){s(f)}}function l(u){try{c(i.throw(u))}catch(f){s(f)}}function c(u){u.done?o(u.value):r(u.value).then(a,l)}c((i=i.apply(t,n||[])).next())})}function H_(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function br(t){return this instanceof br?(this.v=t,this):new br(t)}function $_(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(m){return function(v){return Promise.resolve(v).then(m,f)}}function a(m,v){i[m]&&(r[m]=function(S){return new Promise(function(M,O){o.push([m,S,M,O])>1||l(m,S)})},v&&(r[m]=v(r[m])))}function l(m,v){try{c(i[m](v))}catch(S){h(o[0][3],S)}}function c(m){m.value instanceof br?Promise.resolve(m.value.v).then(u,f):h(o[0][2],m)}function u(m){l("next",m)}function f(m){l("throw",m)}function h(m,v){m(v),o.shift(),o.length&&l(o[0][0],o[0][1])}}function W_(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof H_=="function"?H_(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,l){s=t[o](s),r(a,l,s.done,s.value)})}}function r(o,s,a,l){Promise.resolve(l).then(function(c){o({value:c,done:a})},s)}}var dc=(t=>t&&typeof t.length=="number"&&typeof t!="function");function uc(t){return ce(t?.then)}function fc(t){return ce(t[Co])}function hc(t){return Symbol.asyncIterator&&ce(t?.[Symbol.asyncIterator])}function mc(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function Kx(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var pc=Kx();function gc(t){return ce(t?.[pc])}function vc(t){return $_(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield br(e.read());if(r)return yield br(void 0);yield yield br(i)}}finally{e.releaseLock()}})}function _c(t){return ce(t?.getReader)}function Ie(t){if(t instanceof ie)return t;if(t!=null){if(fc(t))return Xx(t);if(dc(t))return Qx(t);if(uc(t))return Jx(t);if(hc(t))return G_(t);if(gc(t))return eI(t);if(_c(t))return tI(t)}throw mc(t)}function Xx(t){return new ie(n=>{let e=t[Co]();if(ce(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function Qx(t){return new ie(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function Jx(t){return new ie(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,nc)})}function eI(t){return new ie(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function G_(t){return new ie(n=>{nI(t,n).catch(e=>n.error(e))})}function tI(t){return G_(vc(t))}function nI(t,n){var e,i,r,o;return z_(this,void 0,void 0,function*(){try{for(e=W_(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Vt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function bc(t,n=0){return ue((e,i)=>{e.subscribe(fe(i,r=>Vt(i,t,()=>i.next(r),n),()=>Vt(i,t,()=>i.complete(),n),r=>Vt(i,t,()=>i.error(r),n)))})}function yc(t,n=0){return ue((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function q_(t,n){return Ie(t).pipe(yc(n),bc(n))}function Y_(t,n){return Ie(t).pipe(yc(n),bc(n))}function Z_(t,n){return new ie(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function K_(t,n){return new ie(e=>{let i;return Vt(e,n,()=>{i=t[pc](),Vt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>ce(i?.return)&&i.return()})}function Cc(t,n){if(!t)throw new Error("Iterable cannot be null");return new ie(e=>{Vt(e,n,()=>{let i=t[Symbol.asyncIterator]();Vt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function X_(t,n){return Cc(vc(t),n)}function Q_(t,n){if(t!=null){if(fc(t))return q_(t,n);if(dc(t))return Z_(t,n);if(uc(t))return Y_(t,n);if(hc(t))return Cc(t,n);if(gc(t))return K_(t,n);if(_c(t))return X_(t,n)}throw mc(t)}function Pe(t,n){return n?Q_(t,n):Ie(t)}function q(...t){let n=Fn(t);return Pe(t,n)}function zs(t,n){let e=ce(t)?t:()=>t,i=r=>r.error(e());return new ie(n?r=>n.schedule(i,0,r):i)}function $s(t){return!!t&&(t instanceof ie||ce(t.lift)&&ce(t.subscribe))}var yr=_o(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function J_(t){return t instanceof Date&&!isNaN(t)}function re(t,n){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:iI}=Array;function rI(t,n){return iI(n)?t(...n):t(n)}function Sc(t){return re(n=>rI(t,n))}var{isArray:oI}=Array,{getPrototypeOf:sI,prototype:aI,keys:lI}=Object;function Dc(t){if(t.length===1){let n=t[0];if(oI(n))return{args:n,keys:null};if(cI(n)){let e=lI(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function cI(t){return t&&typeof t=="object"&&sI(t)===aI}function Ec(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Ws(...t){let n=Fn(t),e=cc(t),{args:i,keys:r}=Dc(t);if(i.length===0)return Pe([],n);let o=new ie(dI(i,n,r?s=>Ec(r,s):Ht));return e?o.pipe(Sc(e)):o}function dI(t,n,e=Ht){return i=>{eb(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let l=0;l<r;l++)eb(n,()=>{let c=Pe(t[l],n),u=!1;c.subscribe(fe(i,f=>{o[l]=f,u||(u=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function eb(t,n,e){t?Vt(e,t,n):n()}function tb(t,n,e,i,r,o,s,a){let l=[],c=0,u=0,f=!1,h=()=>{f&&!l.length&&!c&&n.complete()},m=S=>c<i?v(S):l.push(S),v=S=>{o&&n.next(S),c++;let M=!1;Ie(e(S,u++)).subscribe(fe(n,O=>{r?.(O),o?m(O):n.next(O)},()=>{M=!0},void 0,()=>{if(M)try{for(c--;l.length&&c<i;){let O=l.shift();s?Vt(n,s,()=>v(O)):v(O)}h()}catch(O){n.error(O)}}))};return t.subscribe(fe(n,m,()=>{f=!0,h()})),()=>{a?.()}}function Nt(t,n,e=1/0){return ce(n)?Nt((i,r)=>re((o,s)=>n(i,o,r,s))(Ie(t(i,r))),e):(typeof n=="number"&&(e=n),ue((i,r)=>tb(i,r,t,e)))}function wc(t=1/0){return Nt(Ht,t)}function nb(){return wc(1)}function Ri(...t){return nb()(Pe(t,Fn(t)))}function pn(t){return new ie(n=>{Ie(t()).subscribe(n)})}function Gs(...t){let n=cc(t),{args:e,keys:i}=Dc(t),r=new ie(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),l=s,c=s;for(let u=0;u<s;u++){let f=!1;Ie(e[u]).subscribe(fe(o,h=>{f||(f=!0,c--),a[u]=h},()=>l--,void 0,()=>{(!l||!f)&&(c||o.next(i?Ec(i,a):a),o.complete())}))}});return n?r.pipe(Sc(n)):r}function ib(t=0,n,e=j_){let i=-1;return n!=null&&(lc(n)?e=n:i=n),new ie(r=>{let o=J_(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function ii(...t){let n=Fn(t),e=U_(t,1/0),i=t;return i.length?i.length===1?Ie(i[0]):wc(e)(Pe(i,n)):ze}function be(t,n){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>t.call(n,o,r++)&&i.next(o)))})}function rb(t){return ue((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}s&&e.complete()},l=()=>{o=null,s&&e.complete()};n.subscribe(fe(e,c=>{i=!0,r=c,o||Ie(t(c)).subscribe(o=fe(e,a,l))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function xc(t,n=Hs){return rb(()=>ib(t,n))}function Cr(t){return ue((n,e)=>{let i=null,r=!1,o;i=n.subscribe(fe(e,void 0,void 0,s=>{o=Ie(t(s,Cr(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function So(t,n){return ce(n)?Nt(t,n,1):Nt(t,1)}function qs(t,n=Hs){return ue((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=s+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}a()}e.subscribe(fe(i,c=>{o=c,s=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function ob(t){return ue((n,e)=>{let i=!1;n.subscribe(fe(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function $e(t){return t<=0?()=>ze:ue((n,e)=>{let i=0;n.subscribe(fe(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Ic(t,n=Ht){return t=t??uI,ue((e,i)=>{let r,o=!0;e.subscribe(fe(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function uI(t,n){return t===n}function sb(t=fI){return ue((n,e)=>{let i=!1;n.subscribe(fe(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function fI(){return new yr}function Sr(t){return ue((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function ri(t,n){let e=arguments.length>=2;return i=>i.pipe(t?be((r,o)=>t(r,o,i)):Ht,$e(1),e?ob(n):sb(()=>new yr))}function Tc(t){return t<=0?()=>ze:ue((n,e)=>{let i=[];n.subscribe(fe(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Mc(){return ue((t,n)=>{let e,i=!1;t.subscribe(fe(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function Ys(t={}){let{connector:n=()=>new D,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,l,c=0,u=!1,f=!1,h=()=>{a?.unsubscribe(),a=void 0},m=()=>{h(),s=l=void 0,u=f=!1},v=()=>{let S=s;m(),S?.unsubscribe()};return ue((S,M)=>{c++,!f&&!u&&h();let O=l=l??n();M.add(()=>{c--,c===0&&!f&&!u&&(a=uh(v,r))}),O.subscribe(M),!s&&c>0&&(s=new ni({next:pe=>O.next(pe),error:pe=>{f=!0,h(),a=uh(m,e,pe),O.error(pe)},complete:()=>{u=!0,h(),a=uh(m,i),O.complete()}}),Ie(S).subscribe(s))})(o)}}function uh(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new ni({next:()=>{i.unsubscribe(),t()}});return Ie(n(...e)).subscribe(i)}function Nc(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,Ys({connector:()=>new Ai(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function Zs(t){return be((n,e)=>t<=e)}function kt(...t){let n=Fn(t);return ue((e,i)=>{(n?Ri(t,e,n):Ri(t,e)).subscribe(i)})}function ft(t,n){return ue((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(fe(i,l=>{r?.unsubscribe();let c=0,u=o++;Ie(t(l,u)).subscribe(r=fe(i,f=>i.next(n?n(l,f,u,c++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Ce(t){return ue((n,e)=>{Ie(t).subscribe(fe(e,()=>e.complete(),Bs)),!e.closed&&n.subscribe(e)})}function fh(t,n=!1){return ue((e,i)=>{let r=0;e.subscribe(fe(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function rt(t,n,e){let i=ce(t)||n||e?{next:t,error:n,complete:e}:t;return i?ue((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(fe(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;a=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;a=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;a&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Ht}var Lc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",C=class extends Error{code;constructor(n,e){super(Vn(n,e)),this.code=n}};function hI(t){return`NG0${Math.abs(t)}`}function Vn(t,n){return`${hI(t)}${n?": "+n:""}`}function Se(t){for(let n in t)if(t[n]===Se)return n;throw Error("")}function ub(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function ta(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(ta).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Vc(t,n){return t?n?`${t} ${n}`:t:n||""}var mI=Se({__forward_ref__:Se});function Bt(t){return t.__forward_ref__=Bt,t}function et(t){return wh(t)?t():t}function wh(t){return typeof t=="function"&&t.hasOwnProperty(mI)&&t.__forward_ref__===Bt}function se(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function X(t){return{providers:t.providers||[],imports:t.imports||[]}}function na(t){return pI(t,Bc)}function xh(t){return na(t)!==null}function pI(t,n){return t.hasOwnProperty(n)&&t[n]||null}function gI(t){let n=t?.[Bc]??null;return n||null}function mh(t){return t&&t.hasOwnProperty(Ac)?t[Ac]:null}var Bc=Se({\u0275prov:Se}),Ac=Se({\u0275inj:Se}),g=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=se({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Ih(t){return t&&!!t.\u0275providers}var ia=Se({\u0275cmp:Se}),ra=Se({\u0275dir:Se}),Th=Se({\u0275pipe:Se}),Mh=Se({\u0275mod:Se}),Xs=Se({\u0275fac:Se}),Tr=Se({__NG_ELEMENT_ID__:Se}),ab=Se({__NG_ENV_ID__:Se});function fb(t){return Uc(t,"@NgModule"),t[Mh]||null}function Pi(t){return Uc(t,"@Component"),t[ia]||null}function jc(t){return Uc(t,"@Directive"),t[ra]||null}function hb(t){return Uc(t,"@Pipe"),t[Th]||null}function Uc(t,n){if(t==null)throw new C(-919,!1)}function Nh(t){return typeof t=="string"?t:t==null?"":String(t)}var mb=Se({ngErrorCode:Se}),vI=Se({ngErrorMessage:Se}),_I=Se({ngTokenPath:Se});function kh(t,n){return pb("",-200,n)}function Hc(t,n){throw new C(-201,!1)}function pb(t,n,e){let i=new C(n,t);return i[mb]=n,i[vI]=t,e&&(i[_I]=e),i}function bI(t){return t[mb]}var ph;function gb(){return ph}function zt(t){let n=ph;return ph=t,n}function Ah(t,n,e){let i=na(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Hc(t,"")}var wo=globalThis;var yI={},Dr=yI,CI="__NG_DI_FLAG__",gh=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Er(e)||0;try{return this.injector.get(n,i&8?null:Dr,i)}catch(r){if(vo(r))return r;throw r}}};function SI(t,n=0){let e=Jl();if(e===void 0)throw new C(-203,!1);if(e===null)return Ah(t,void 0,n);{let i=DI(n),r=e.retrieve(t,i);if(vo(r)){if(i.optional)return null;throw r}return r}}function j(t,n=0){return(gb()||SI)(et(t),n)}function d(t,n){return j(t,Er(n))}function Er(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function DI(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function vh(t){let n=[];for(let e=0;e<t.length;e++){let i=et(t[e]);if(Array.isArray(i)){if(i.length===0)throw new C(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],l=EI(a);typeof l=="number"?l===-1?r=a.token:o|=l:r=a}n.push(j(r,o))}else n.push(j(i))}return n}function EI(t){return t[CI]}function wr(t,n){let e=t.hasOwnProperty(Xs);return e?t[Xs]:null}function vb(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function _b(t){return t.flat(Number.POSITIVE_INFINITY)}function zc(t,n){t.forEach(e=>Array.isArray(e)?zc(e,n):n(e))}function Rh(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function oa(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function bb(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function yb(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function $c(t,n,e){let i=xo(t,n);return i>=0?t[i|1]=e:(i=~i,yb(t,i,n,e)),i}function Wc(t,n){let e=xo(t,n);if(e>=0)return t[e|1]}function xo(t,n){return wI(t,n,1)}function wI(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Fi={},_t=[],Mr=new g(""),sa=new g("",-1),Oh=new g(""),Eo=class{get(n,e=Dr){if(e===Dr){let r=pb("",-201);throw r.name="\u0275NotFound",r}return e}};function Bn(t){return{\u0275providers:t}}function Cb(t){return Bn([{provide:Mr,multi:!0,useValue:t}])}function Sb(...t){return{\u0275providers:Ph(!0,t),\u0275fromNgModule:!0}}function Ph(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return zc(n,s=>{let a=s;Rc(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Db(r,o),e}function Db(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Fh(r,o=>{n(o,i)})}}function Rc(t,n,e,i){if(t=et(t),!t)return!1;let r=null,o=mh(t),s=!o&&Pi(t);if(!o&&!s){let l=t.ngModule;if(o=mh(l),o)r=l;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let l=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let c of l)Rc(c,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let c;zc(o.imports,u=>{Rc(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&Db(c,n)}if(!a){let c=wr(r)||(()=>new r);n({provide:r,useFactory:c,deps:_t},r),n({provide:Oh,useValue:r,multi:!0},r),n({provide:Mr,useValue:()=>j(r),multi:!0},r)}let l=o.providers;if(l!=null&&!a){let c=t;Fh(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function Fh(t,n){for(let e of t)Ih(e)&&(e=e.\u0275providers),Array.isArray(e)?Fh(e,n):n(e)}var xI=Se({provide:String,useValue:Se});function Eb(t){return t!==null&&typeof t=="object"&&xI in t}function II(t){return!!(t&&t.useExisting)}function TI(t){return!!(t&&t.useFactory)}function xr(t){return typeof t=="function"}function wb(t){return!!t.useClass}var aa=new g(""),kc={},lb={},hh;function Io(){return hh===void 0&&(hh=new Eo),hh}var xe=class{},Ir=class extends xe{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,bh(n,s=>this.processProvider(s)),this.records.set(sa,Do(void 0,this)),r.has("environment")&&this.records.set(xe,Do(void 0,this));let o=this.records.get(aa);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Oh,_t,{self:!0}))}retrieve(n,e){let i=Er(e)||0;try{return this.get(n,Dr,i)}catch(r){if(vo(r))return r;throw r}}destroy(){Ks(this),this._destroyed=!0;let n=B(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),B(n)}}onDestroy(n){return Ks(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Ks(this);let e=Pn(this),i=zt(void 0),r;try{return n()}finally{Pn(e),zt(i)}}get(n,e=Dr,i){if(Ks(this),n.hasOwnProperty(ab))return n[ab](this);let r=Er(i),o,s=Pn(this),a=zt(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=RI(n)&&na(n);u&&this.injectableDefInScope(u)?c=Do(_h(n),kc):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?Io():this.parent;return e=r&8&&e===Dr?null:e,l.get(n,e)}catch(l){let c=bI(l);throw c===-200||c===-201?new C(c,null):l}finally{zt(a),Pn(s)}}resolveInjectorInitializers(){let n=B(null),e=Pn(this),i=zt(void 0),r;try{let o=this.get(Mr,_t,{self:!0});for(let s of o)s()}finally{Pn(e),zt(i),B(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=et(n);let e=xr(n)?n:et(n&&n.provide),i=NI(n);if(!xr(n)&&n.multi===!0){let r=this.records.get(e);r||(r=Do(void 0,kc,!0),r.factory=()=>vh(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=B(null);try{if(e.value===lb)throw kh("");return e.value===kc&&(e.value=lb,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&AI(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{B(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=et(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function _h(t){let n=na(t),e=n!==null?n.factory:wr(t);if(e!==null)return e;if(t instanceof g)throw new C(-204,!1);if(t instanceof Function)return MI(t);throw new C(-204,!1)}function MI(t){if(t.length>0)throw new C(-204,!1);let e=gI(t);return e!==null?()=>e.factory(t):()=>new t}function NI(t){if(Eb(t))return Do(void 0,t.useValue);{let n=Lh(t);return Do(n,kc)}}function Lh(t,n,e){let i;if(xr(t)){let r=et(t);return wr(r)||_h(r)}else if(Eb(t))i=()=>et(t.useValue);else if(TI(t))i=()=>t.useFactory(...vh(t.deps||[]));else if(II(t))i=(r,o)=>j(et(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=et(t&&(t.useClass||t.provide));if(kI(t))i=()=>new r(...vh(t.deps));else return wr(r)||_h(r)}return i}function Ks(t){if(t.destroyed)throw new C(-205,!1)}function Do(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function kI(t){return!!t.deps}function AI(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function RI(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function bh(t,n){for(let e of t)Array.isArray(e)?bh(e,n):e&&Ih(e)?bh(e.\u0275providers,n):n(e)}function Ve(t,n){let e;t instanceof Ir?(Ks(t),e=t):e=new gh(t);let i,r=Pn(e),o=zt(void 0);try{return n()}finally{Pn(r),zt(o)}}function xb(){return gb()!==void 0||Jl()!=null}var gn=0,U=1,K=2,tt=3,Qt=4,yt=5,Nr=6,To=7,We=8,jn=9,Un=10,ke=11,Mo=12,Vh=13,Li=14,At=15,Vi=16,kr=17,Hn=18,zn=19,Bh=20,oi=21,Gc=22,Oi=23,$t=24,Ar=25,$n=26,Ge=27,Ib=1,jh=6,Rr=7,la=8,Or=9,Be=10;function ai(t){return Array.isArray(t)&&typeof t[Ib]=="object"}function Jt(t){return Array.isArray(t)&&t[Ib]===!0}function Uh(t){return(t.flags&4)!==0}function li(t){return t.componentOffset>-1}function ca(t){return(t.flags&1)===1}function vn(t){return!!t.template}function No(t){return(t[K]&512)!==0}function Pr(t){return(t[K]&256)===256}var ot=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(ot||{});var Hh="svg",Tb="math";function ht(t){for(;Array.isArray(t);)t=t[gn];return t}function zh(t,n){return ht(n[t])}function en(t,n){return ht(n[t.index])}function qc(t,n){return t.data[n]}function Mb(t,n){return t[n]}function tn(t,n){let e=n[t];return ai(e)?e:e[gn]}function Nb(t){return(t[K]&4)===4}function Yc(t){return(t[K]&128)===128}function kb(t){return Jt(t[tt])}function nn(t,n){return n==null?null:t[n]}function $h(t){t[kr]=0}function Wh(t){t[K]&1024||(t[K]|=1024,Yc(t)&&Fr(t))}function Ab(t,n){for(;t>0;)n=n[Li],t--;return n}function da(t){return!!(t[K]&9216||t[$t]?.dirty)}function Zc(t){t[Un].changeDetectionScheduler?.notify(8),t[K]&64&&(t[K]|=1024),da(t)&&Fr(t)}function Fr(t){t[Un].changeDetectionScheduler?.notify(0);let n=si(t);for(;n!==null&&!(n[K]&8192||(n[K]|=8192,!Yc(n)));)n=si(n)}function Kc(t,n){if(Pr(t))throw new C(911,!1);t[oi]===null&&(t[oi]=[]),t[oi].push(n)}function Rb(t,n){if(t[oi]===null)return;let e=t[oi].indexOf(n);e!==-1&&t[oi].splice(e,1)}function si(t){let n=t[tt];return Jt(n)?n[tt]:n}function Gh(t){return t[To]??=[]}function qh(t){return t.cleanup??=[]}function Ob(t,n,e,i){let r=Gh(n);r.push(e),t.firstCreatePass&&qh(t).push(i,r.length-1)}var he={lFrame:Gb(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var yh=!1;function Pb(){return he.lFrame.elementDepthCount}function Fb(){he.lFrame.elementDepthCount++}function Yh(){he.lFrame.elementDepthCount--}function Zh(){return he.bindingsEnabled}function Kh(){return he.skipHydrationRootTNode!==null}function Xh(t){return he.skipHydrationRootTNode===t}function Qh(){he.skipHydrationRootTNode=null}function ae(){return he.lFrame.lView}function je(){return he.lFrame.tView}function Ct(t){return he.lFrame.contextLView=t,t[We]}function St(t){return he.lFrame.contextLView=null,t}function st(){let t=Jh();for(;t!==null&&t.type===64;)t=t.parent;return t}function Jh(){return he.lFrame.currentTNode}function Lb(){let t=he.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function ko(t,n){let e=he.lFrame;e.currentTNode=t,e.isParent=n}function em(){return he.lFrame.isParent}function tm(){he.lFrame.isParent=!1}function Vb(){return he.lFrame.contextLView}function nm(){return yh}function Qs(t){let n=yh;return yh=t,n}function Bb(){let t=he.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function jb(t){return he.lFrame.bindingIndex=t}function Lr(){return he.lFrame.bindingIndex++}function im(t){let n=he.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Ub(){return he.lFrame.inI18n}function Hb(t,n){let e=he.lFrame;e.bindingIndex=e.bindingRootIndex=t,Xc(n)}function zb(){return he.lFrame.currentDirectiveIndex}function Xc(t){he.lFrame.currentDirectiveIndex=t}function $b(t){let n=he.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Qc(){return he.lFrame.currentQueryIndex}function ua(t){he.lFrame.currentQueryIndex=t}function OI(t){let n=t[U];return n.type===2?n.declTNode:n.type===1?t[yt]:null}function rm(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=OI(o),r===null||(o=o[Li],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=he.lFrame=Wb();return i.currentTNode=n,i.lView=t,!0}function Jc(t){let n=Wb(),e=t[U];he.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Wb(){let t=he.lFrame,n=t===null?null:t.child;return n===null?Gb(t):n}function Gb(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function qb(){let t=he.lFrame;return he.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var om=qb;function ed(){let t=qb();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Yb(t){return(he.lFrame.contextLView=Ab(t,he.lFrame.contextLView))[We]}function ci(){return he.lFrame.selectedIndex}function Bi(t){he.lFrame.selectedIndex=t}function fa(){let t=he.lFrame;return qc(t.tView,t.selectedIndex)}function Ao(){he.lFrame.currentNamespace=Hh}function td(){PI()}function PI(){he.lFrame.currentNamespace=null}function sm(){return he.lFrame.currentNamespace}var Zb=!0;function nd(){return Zb}function id(t){Zb=t}function Ch(t,n=null,e=null,i){let r=am(t,n,e,i);return r.resolveInjectorInitializers(),r}function am(t,n=null,e=null,i,r=new Set){let o=[e||_t,Sb(t)],s;return new Ir(o,n||Io(),s||null,r)}var V=class t{static THROW_IF_NOT_FOUND=Dr;static NULL=new Eo;static create(n,e){if(Array.isArray(n))return Ch({name:""},e,n,"");{let i=n.name??"";return Ch({name:i},n.parent,n.providers,i)}}static \u0275prov=se({token:t,providedIn:"any",factory:()=>j(sa)});static __NG_ELEMENT_ID__=-1},L=new g(""),He=class{static __NG_ELEMENT_ID__=FI;static __NG_ENV_ID__=n=>n},Oc=class extends He{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Pr(this._lView)}onDestroy(n){let e=this._lView;return Kc(e,n),()=>Rb(e,n)}};function FI(){return new Oc(ae())}var Kb=!1,Xb=new g(""),di=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Je(!1);debugTaskTracker=d(Xb,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ie(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=se({token:t,providedIn:"root",factory:()=>new t})}return t})(),Sh=class extends D{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,xb()&&(this.destroyRef=d(He,{optional:!0})??void 0,this.pendingTasks=d(di,{optional:!0})??void 0)}emit(n){let e=B(null);try{super.next(n)}finally{B(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),s=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof ge&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},W=Sh;function Pc(...t){}function lm(t){let n,e;function i(){t=Pc;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function Qb(t){return queueMicrotask(()=>t()),()=>{t=Pc}}var cm="isAngularZone",Js=cm+"_ID",LI=0,T=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new W(!1);onMicrotaskEmpty=new W(!1);onStable=new W(!1);onError=new W(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=Kb}=n;if(typeof Zone>"u")throw new C(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,jI(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(cm)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new C(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new C(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,VI,Pc,Pc);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},VI={};function dm(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function BI(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){lm(()=>{t.callbackScheduled=!1,Dh(t),t.isCheckStableRunning=!0,dm(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Dh(t)}function jI(t){let n=()=>{BI(t)},e=LI++;t._inner=t._inner.fork({name:"angular",properties:{[cm]:!0,[Js]:e,[Js+e]:!0},onInvokeTask:(i,r,o,s,a,l)=>{if(UI(l))return i.invokeTask(o,s,a,l);try{return cb(t),i.invokeTask(o,s,a,l)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),db(t)}},onInvoke:(i,r,o,s,a,l,c)=>{try{return cb(t),i.invoke(o,s,a,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!HI(l)&&n(),db(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,Dh(t),dm(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function Dh(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function cb(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function db(t){t._nesting--,dm(t)}var ea=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new W;onMicrotaskEmpty=new W;onStable=new W;onError=new W;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function UI(t){return Jb(t,"__ignore_ng_zone__")}function HI(t){return Jb(t,"__scheduler_tick__")}function Jb(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var bt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},rn=new g("",{factory:()=>{let t=d(T),n=d(xe),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(bt),e.handleError(i))})}}}),ey={provide:Mr,useValue:()=>{let t=d(bt,{optional:!0})},multi:!0},zI=new g("",{factory:()=>{let t=d(L).defaultView;if(!t)return;let n=d(rn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(He).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function um(){return Bn([Cb(()=>{d(zI)})])}function H(t,n){let[e,i,r]=Zf(t,n?.equal),o=e,s=o[Ne];return o.set=i,o.update=r,o.asReadonly=ha.bind(o),o}function ha(){let t=this[Ne];if(t.readonlyFn===void 0){let n=()=>this();n[Ne]=t,t.readonlyFn=n}return t.readonlyFn}var _n=new g("",{factory:()=>$I}),$I="ng";var rd=new g(""),Vr=new g("",{providedIn:"platform",factory:()=>"unknown"}),ma=new g(""),ui=new g("",{factory:()=>d(L).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Ro=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=WI}return t})();function WI(){return new Ro(ae(),st())}var Ln=class{},pa=new g("",{factory:()=>!0});var fm=new g(""),od=(()=>{class t{static \u0275prov=se({token:t,providedIn:"root",factory:()=>new Eh})}return t})(),Eh=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Fc=class{[Ne];constructor(n){this[Ne]=n}destroy(){this[Ne].destroy()}};function nt(t,n){let e=n?.injector??d(V),i=n?.manualCleanup!==!0?e.get(He):null,r,o=e.get(Ro,null,{optional:!0}),s=e.get(Ln);return o!==null?(r=YI(o.view,s,t),i instanceof Oc&&i._lView===o.view&&(i=null)):r=ZI(t,e.get(od),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Fc(r)}var ty=P(p({},Kf),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=Qs(!1);try{Xf(this)}finally{Qs(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=B(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],B(t)}}}),GI=P(p({},ty),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(ti(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),qI=P(p({},ty),{consumerMarkedDirty(){this.view[K]|=8192,Fr(this.view),this.notifier.notify(13)},destroy(){if(ti(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Oi]?.delete(this)}});function YI(t,n,e){let i=Object.create(qI);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=ny(i,e),t[Oi]??=new Set,t[Oi].add(i),i.consumerMarkedDirty(i),i}function ZI(t,n,e){let i=Object.create(GI);return i.fn=ny(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function ny(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Wt(t){return typeof t=="function"&&t[Ne]!==void 0}var ga=(()=>{class t{internalPendingTasks=d(di);scheduler=d(Ln);errorHandler=d(rn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=se({token:t,providedIn:"root",factory:()=>new t})}return t})();function Ia(t){return{toString:t}.toString()}var ve=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(ve||{}),hd=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function $y(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Wy=null,qe=(()=>{Wy=iy;let t=()=>iy;return t.ngInherit=!0,t})();function sT(){return Wy}function iy(t){return t.type.prototype.ngOnChanges&&(t.setInput=lT),aT}function aT(){let t=Gy(this),n=t?.current;if(n){let e=t.previous;if(e===Fi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function lT(t,n,e,i,r){let o=this.declaredInputs[i],s=Gy(t)||cT(t,{previous:Fi,current:null}),a=s.current||(s.current={}),l=s.previous,c=l[o];a[o]=new hd(c&&c.currentValue,e,l===Fi),$y(t,n,r,e)}var Dm="__ngSimpleChanges__";function Gy(t){return Object.hasOwn(t,Dm)&&t[Dm]||null}function cT(t,n){return t[Dm]=n}var ry=[];var De=function(t,n=null,e){for(let i=0;i<ry.length;i++){let r=ry[i];r(t,n,e)}};function dT(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=sT()(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function qy(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function cd(t,n,e){Yy(t,n,3,e)}function dd(t,n,e,i){(t[K]&3)===e&&Yy(t,n,e,i)}function hm(t,n){let e=t[K];(e&3)===n&&(e&=16383,e+=1,t[K]=e)}function Yy(t,n,e,i){let r=i!==void 0?t[kr]&65535:0,o=i??-1,s=n.length-1,a=0;for(let l=r;l<s;l++)if(typeof n[l+1]=="number"){if(a=n[l],i!=null&&a>=i)break}else n[l]<0&&(t[kr]+=65536),(a<o||o==-1)&&(uT(t,e,n,l),t[kr]=(t[kr]&4294901760)+l+2),l++}function oy(t,n){De(ve.LifecycleHookStart,t,n);let e=B(null);try{n.call(t)}finally{B(e),De(ve.LifecycleHookEnd,t,n)}}function uT(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[K]>>14<t[kr]>>16&&(t[K]&3)===n&&(t[K]+=16384,oy(a,o)):oy(a,o)}var Po=-1,Ur=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function fT(t){return(t.flags&8)!==0}function hT(t){return(t.flags&16)!==0}function mT(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];pT(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function Zy(t){return t===3||t===4||t===6}function pT(t){return t.charCodeAt(0)===64}function Fo(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?sy(t,e,r,null,n[++i]):sy(t,e,r,null,null))}}return t}function sy(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function Ky(t){return t!==Po}function md(t){return t&32767}function gT(t){return t>>16}function pd(t,n){let e=gT(t),i=n;for(;e>0;)i=i[Li],e--;return i}var Em=!0;function ay(t){let n=Em;return Em=t,n}var vT=256,Xy=vT-1,Qy=5,_T=0,Wn={};function bT(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Tr)&&(i=e[Tr]),i==null&&(i=e[Tr]=_T++);let r=i&Xy,o=1<<r;n.data[t+(r>>Qy)]|=o}function gd(t,n){let e=Jy(t,n);if(e!==-1)return e;let i=n[U];i.firstCreatePass&&(t.injectorIndex=n.length,mm(i.data,t),mm(n,null),mm(i.blueprint,null));let r=op(t,n),o=t.injectorIndex;if(Ky(r)){let s=md(r),a=pd(r,n),l=a[U].data;for(let c=0;c<8;c++)n[o+c]=a[s+c]|l[s+c]}return n[o+8]=r,o}function mm(t,n){t.push(0,0,0,0,0,0,0,0,n)}function Jy(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function op(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=rC(r),i===null)return Po;if(e++,r=r[Li],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Po}function wm(t,n,e){bT(t,n,e)}function yT(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Zy(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function eC(t,n,e){if(e&8||t!==void 0)return t;Hc(n,"NodeInjector")}function tC(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[jn],o=zt(void 0);try{return r?r.get(n,i,e&8):Ah(n,i,e&8)}finally{zt(o)}}return eC(i,n,e)}function nC(t,n,e,i=0,r){if(t!==null){if(n[K]&2048&&!(i&2)){let s=ET(t,n,e,i,Wn);if(s!==Wn)return s}let o=iC(t,n,e,i,Wn);if(o!==Wn)return o}return tC(n,e,i,r)}function iC(t,n,e,i,r){let o=ST(e);if(typeof o=="function"){if(!rm(n,t,i))return i&1?eC(r,e,i):tC(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Hc(e);else return s}finally{om()}}else if(typeof o=="number"){let s=null,a=Jy(t,n),l=Po,c=i&1?n[At][yt]:null;for((a===-1||i&4)&&(l=a===-1?op(t,n):n[a+8],l===Po||!cy(i,!1)?a=-1:(s=n[U],a=md(l),n=pd(l,n)));a!==-1;){let u=n[U];if(ly(o,a,u.data)){let f=CT(a,n,e,s,i,c);if(f!==Wn)return f}l=n[a+8],l!==Po&&cy(i,n[U].data[a+8]===c)&&ly(o,a,n)?(s=u,a=md(l),n=pd(l,n)):a=-1}}return r}function CT(t,n,e,i,r,o){let s=n[U],a=s.data[t+8],l=i==null?li(a)&&Em:i!=s&&(a.type&3)!==0,c=r&1&&o===a,u=ud(a,s,e,l,c);return u!==null?ya(n,s,u,a,r):Wn}function ud(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,f=i?a:a+u,h=r?a+u:c;for(let m=f;m<h;m++){let v=s[m];if(m<l&&e===v||m>=l&&v.type===e)return m}if(r){let m=s[l];if(m&&vn(m)&&m.type===e)return l}return null}function ya(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof Ur){let a=o;if(a.resolving)throw kh("");let l=ay(a.canSeeViewProviders);a.resolving=!0;let c=s[e].type||s[e],u,f=a.injectImpl?zt(a.injectImpl):null,h=rm(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&dT(e,s[e],n)}finally{f!==null&&zt(f),ay(l),a.resolving=!1,om()}}return o}function ST(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Tr)?t[Tr]:void 0;return typeof n=="number"?n>=0?n&Xy:DT:n}function ly(t,n,e){let i=1<<t;return!!(e[n+(t>>Qy)]&i)}function cy(t,n){return!(t&2)&&!(t&1&&n)}var ji=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return nC(this._tNode,this._lView,n,Er(i),e)}};function DT(){return new ji(st(),ae())}function Dt(t){return Ia(()=>{let n=t.prototype.constructor,e=n[Xs]||xm(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Xs]||xm(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function xm(t){return wh(t)?()=>{let n=xm(et(t));return n&&n()}:wr(t)}function ET(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[K]&2048&&!No(s);){let a=iC(o,s,e,i|2,Wn);if(a!==Wn)return a;let l=o.parent;if(!l){let c=s[Bh];if(c){let u=c.get(e,Wn,i&-5);if(u!==Wn)return u}l=rC(s),s=s[Li]}o=l}return r}function rC(t){let n=t[U],e=n.type;return e===2?n.declTNode:e===1?t[yt]:null}function sp(t){return yT(st(),t)}function E(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function wT(){return Uo(st(),ae())}function Uo(t,n){return new F(en(t,n))}var F=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=wT}return t})();function oC(t){return t instanceof F?t.nativeElement:t}function xT(){return this._results[Symbol.iterator]()}var Hr=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new D}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=_b(n);(this._changesDetected=!vb(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=xT};function sC(t){return(t.flags&128)===128}var ap=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(ap||{}),aC=new Map,IT=0;function TT(){return IT++}function MT(t){aC.set(t[zn],t)}function Im(t){aC.delete(t[zn])}var dy="__ngContext__";function Lo(t,n){ai(n)?(t[dy]=n[zn],MT(n)):t[dy]=n}function lC(t){return dC(t[Mo])}function cC(t){return dC(t[Qt])}function dC(t){for(;t!==null&&!Jt(t);)t=t[Qt];return t}var Tm;function lp(t){Tm=t}function uC(){if(Tm!==void 0)return Tm;if(typeof document<"u")return document;throw new C(210,!1)}var fC="r";var hC="di";var mC=!1,pC=new g("",{factory:()=>mC});var uy=new WeakMap;function NT(t,n){if(t==null||typeof t!="object")return;let e=uy.get(t);e||(e=new WeakSet,uy.set(t,e)),e.add(n)}var kT=(t,n,e,i)=>{};function AT(t,n,e,i){kT(t,n,e,i)}function Md(t){return(t.flags&32)===32}var RT=()=>null;function gC(t,n,e=!1){return RT(t,n,e)}function vC(t,n){let e=t.contentQueries;if(e!==null){let i=B(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];ua(o),a.contentQueries(2,n[s],s)}}}finally{B(i)}}}function Mm(t,n,e){ua(0);let i=B(null);try{n(t,e)}finally{B(i)}}function _C(t,n,e){if(Uh(n)){let i=B(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let l=e[s];a.contentQueries(1,l,s)}}}finally{B(i)}}}var Cn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Cn||{});var sd;function OT(){if(sd===void 0&&(sd=null,wo.trustedTypes))try{sd=wo.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return sd}function Nd(t){return OT()?.createHTML(t)||t}var fi=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Lc})`}},Nm=class extends fi{getTypeName(){return"HTML"}},km=class extends fi{getTypeName(){return"Style"}},Am=class extends fi{getTypeName(){return"Script"}},Rm=class extends fi{getTypeName(){return"URL"}},Om=class extends fi{getTypeName(){return"ResourceURL"}};function hi(t){return t instanceof fi?t.changingThisBreaksApplicationSecurity:t}function Wr(t,n){let e=bC(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Lc})`)}return e===n}function bC(t){return t instanceof fi&&t.getTypeName()||null}function cp(t){return new Nm(t)}function dp(t){return new km(t)}function up(t){return new Am(t)}function fp(t){return new Rm(t)}function hp(t){return new Om(t)}function PT(t){let n=new Fm(t);return FT()?new Pm(n):n}var Pm=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Nd(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Fm=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Nd(n),e}};function FT(){try{return!!new window.DOMParser().parseFromString(Nd(""),"text/html")}catch{return!1}}var LT=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function kd(t){return t=String(t),t.match(LT)?t:"unsafe:"+t}function mi(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Ta(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var yC=mi("area,br,col,hr,img,wbr"),CC=mi("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),SC=mi("rp,rt"),VT=Ta(SC,CC),BT=Ta(CC,mi("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),jT=Ta(SC,mi("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),fy=Ta(yC,BT,jT,VT),DC=mi("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),UT=mi("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),HT=mi("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),zT=Ta(DC,UT,HT),$T=mi("script,style,template"),Lm=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=qT(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=GT(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=hy(n).toLowerCase();if(!fy.hasOwnProperty(e))return this.sanitizedSomething=!0,!$T.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!zT.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let l=o.value;DC[a]&&(l=kd(l)),this.buf.push(" ",s,'="',my(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=hy(n).toLowerCase();fy.hasOwnProperty(e)&&!yC.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(my(n))}};function WT(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function GT(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw EC(n);return n}function qT(t){let n=t.firstChild;if(n&&WT(t,n))throw EC(n);return n}function hy(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function EC(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var YT=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,ZT=/([^\#-~ |!])/g;function my(t){return t.replace(/&/g,"&amp;").replace(YT,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(ZT,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var ad;function mp(t,n){let e=null;try{ad=ad||PT(t);let i=n?String(n):"";e=ad.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=ad.getInertBodyElement(i)}while(i!==o);let a=new Lm().sanitizeChildren(py(e)||e);return Nd(a)}finally{if(e){let i=py(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function py(t){return"content"in t&&KT(t)?t.content:null}function KT(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}function XT(t,n){return t.createText(n)}function QT(t,n,e){t.setValue(n,e)}function wC(t,n,e){return t.createElement(n,e)}function Br(t,n,e,i,r){t.insertBefore(n,e,i,r)}function xC(t,n,e){t.appendChild(n,e)}function gy(t,n,e,i,r){i!==null?Br(t,n,e,i,r):xC(t,n,e)}function IC(t,n,e,i){t.removeChild(null,n,e,i)}function JT(t,n,e){t.setAttribute(n,"style",e)}function eM(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function TC(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&mT(t,n,i),r!==null&&eM(t,n,r),o!==null&&JT(t,n,o)}function pp(t){return t.ownerDocument.defaultView}function tM(t){return t instanceof Function?t():t}function nM(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var MC="ng-template";function iM(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&nM(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(gp(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function gp(t){return t.type===4&&t.value!==MC}function rM(t,n,e){let i=t.type===4&&!e?MC:t.value;return n===i}function oM(t,n,e){let i=4,r=t.attrs,o=r!==null?lM(r):0,s=!1;for(let a=0;a<n.length;a++){let l=n[a];if(typeof l=="number"){if(!s&&!bn(i)&&!bn(l))return!1;if(s&&bn(l))continue;s=!1,i=l|i&1;continue}if(!s)if(i&4){if(i=2|i&1,l!==""&&!rM(t,l,e)||l===""&&n.length===1){if(bn(i))return!1;s=!0}}else if(i&8){if(r===null||!iM(t,r,l,e)){if(bn(i))return!1;s=!0}}else{let c=n[++a],u=sM(l,r,gp(t),e);if(u===-1){if(bn(i))return!1;s=!0;continue}if(c!==""){let f;if(u>o?f="":f=r[u+1].toLowerCase(),i&2&&c!==f){if(bn(i))return!1;s=!0}}}}return bn(i)||s}function bn(t){return(t&1)===0}function sM(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return cM(n,t)}function NC(t,n,e=!1){for(let i=0;i<n.length;i++)if(oM(t,n[i],e))return!0;return!1}function aM(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function lM(t){for(let n=0;n<t.length;n++){let e=t[n];if(Zy(e))return n}return t.length}function cM(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function dM(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function vy(t,n){return t?":not("+n.trim()+")":n}function uM(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!bn(s)&&(n+=vy(o,r),r=""),i=s,o=o||!bn(i);e++}return r!==""&&(n+=vy(o,r)),n}function fM(t){return t.map(uM).join(",")}function hM(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!bn(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var on={},Gn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Gn||{}),mM;function vp(t,n){return mM(t,n)}var F8=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Vm=new WeakMap;function kC(t){return t?t[Li]??t:null}var va=new WeakSet;function pM(t,n,e){let i=Vm.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=kC(e);for(let a=i.length-1;a>=0;a--){let{el:l,declarationView:c}=i[a],u=l.parentNode;l===n?(i.splice(a,1),va.add(l),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&l===o?(i.splice(a,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l)):u&&r&&u!==r&&(s===null||c===null||s===c)&&(i.splice(a,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l))}}function gM(t,n,e){let i=kC(e),r=Vm.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):Vm.set(t,[{el:n,declarationView:i}])}var Ui=new Set,Ad=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Ad||{}),Yn=new g(""),_y=new Set;function pi(t){_y.has(t)||(_y.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Rd=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=se({token:t,providedIn:"root",factory:()=>new t})}return t})(),_p=[0,1,2,3],bp=(()=>{class t{ngZone=d(T);scheduler=d(Ln);errorHandler=d(bt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(Yn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&De(ve.AfterRenderHooksStart),this.executing=!0;for(let i of _p)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&De(ve.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Ar]??=[]).push(e),Fr(i),i[K]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Ad.AFTER_NEXT_RENDER,e):e()}static \u0275prov=se({token:t,providedIn:"root",factory:()=>new t})}return t})(),Ca=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Ar];n&&(this.view[Ar]=n.filter(e=>e!==this))}};function at(t,n){let e=n?.injector??d(V);return pi("NgAfterNextRender"),_M(t,e,n,!0)}function vM(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function _M(t,n,e,i){let r=n.get(Rd);r.impl??=n.get(bp);let o=n.get(Yn,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(He):null,a=n.get(Ro,null,{optional:!0}),l=new Ca(r.impl,vM(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(l),l}var yp=new g("",{factory:()=>{let t=d(xe),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function AC(t,n,e){let i=t.get(yp);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function bM(t,n){let e=t.get(yp);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function yM(t,n){let e=t.get(yp);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function CM(t,n){for(let[e,i]of n)AC(t,i.animateFns)}function by(t,n,e,i){let r=t?.[$n]?.enter;n!==null&&r&&r.has(e.index)&&CM(i,r)}function yy(t,n,e,i){try{e.get(sa)}catch{return i(!1)}let r=t?.[$n];r?.enter?.has(n.index)&&bM(e,r.enter.get(n.index).animateFns);let o=SM(t,n,r);if(o.size===0){let s=!1;if(t){let a=[];Od(t,n,a),s=a.length>0}if(!s)return i(!1)}t&&Ui.add(t[zn]),AC(e,()=>DM(t,n,r||void 0,o,i),r||void 0)}function SM(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,s]of r){if(i.has(o))continue;let l=t[U].data[o].parent;for(;l;){if(l===n){i.set(o,s);break}l=l.parent}}return i}function DM(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let l of a.animateFns){let{promise:c}=l();o.push(c)}e.detachedLeaveAnimationFns=void 0}if(t&&Od(t,n,o),o.length>0){let s=e||t?.[$n];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),wM(t,s.running,r)}else Promise.allSettled(o).then(()=>{t&&Ui.delete(t[zn]),r(!0)})}else t&&Ui.delete(t[zn]),r(!1)}function Od(t,n,e){if(n.type&12){let r=t[n.index];if(Jt(r))for(let o=Be;o<r.length;o++){let s=r[o];s[U].type===2&&EM(s,e)}}let i=n.child;for(;i;)Od(t,i,e),i=i.next}function EM(t,n){let e=t[$n];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=t[U].firstChild;for(;i;)Od(t,i,n),i=i.next}function wM(t,n,e){n.then(()=>{t[$n]?.running===n&&(t[$n].running=void 0,Ui.delete(t[zn])),e(!0)})}function Oo(t,n,e,i,r,o,s,a){if(r!=null){let l,c=!1;Jt(r)?l=r:ai(r)&&(c=!0,r=r[gn]);let u=ht(r);t===0&&i!==null?(by(a,i,o,e),s==null?xC(n,i,u):Br(n,i,u,s||null,!0)):t===1&&i!==null?(by(a,i,o,e),Br(n,i,u,s||null,!0),pM(o,u,a)):t===2?(a?.[$n]?.leave?.has(o.index)&&gM(o,u,a),va.delete(u),yy(a,o,e,f=>{if(va.has(u)){va.delete(u);return}IC(n,u,c,f)})):t===3&&(va.delete(u),yy(a,o,e,()=>{n.destroyNode(u)})),l!=null&&FM(n,t,e,l,o,i,s)}}function xM(t,n){RC(t,n),n[gn]=null,n[yt]=null}function IM(t,n,e,i,r,o){i[gn]=r,i[yt]=n,Fd(t,i,e,1,r,o)}function RC(t,n){n[Un].changeDetectionScheduler?.notify(9),Fd(t,n,n[ke],2,null,null)}function TM(t){let n=t[Mo];if(!n)return pm(t[U],t);for(;n;){let e=null;if(ai(n))e=n[Mo];else{let i=n[Be];i&&(e=i)}if(!e){for(;n&&!n[Qt]&&n!==t;)ai(n)&&pm(n[U],n),n=n[tt];n===null&&(n=t),ai(n)&&pm(n[U],n),e=n&&n[Qt]}n=e}}function Cp(t,n){let e=t[Or],i=e.indexOf(n);e.splice(i,1)}function Pd(t,n){if(Pr(n))return;let e=n[ke];e.destroyNode&&Fd(t,n,e,3,null,null),TM(n)}function pm(t,n){if(Pr(n))return;let e=B(null);try{n[K]&=-129,n[K]|=256,n[$t]&&ti(n[$t]),NM(t,n),MM(t,n),n[U].type===1&&n[ke].destroy();let i=n[Vi];if(i!==null&&Jt(n[tt])){i!==n[tt]&&Cp(i,n);let r=n[Hn];r!==null&&r.detachView(t)}Im(n)}finally{B(e)}}function MM(t,n){let e=t.cleanup,i=n[To];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[To]=null);let r=n[oi];if(r!==null){n[oi]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Oi];if(o!==null){n[Oi]=null;for(let s of o)s.destroy()}}function NM(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Ur)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],l=o[s+1];De(ve.LifecycleHookStart,a,l);try{l.call(a)}finally{De(ve.LifecycleHookEnd,a,l)}}else{De(ve.LifecycleHookStart,r,o);try{o.call(r)}finally{De(ve.LifecycleHookEnd,r,o)}}}}}function OC(t,n,e){return kM(t,n.parent,e)}function kM(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[gn];if(li(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Cn.None||r===Cn.Emulated)return null}return en(i,e)}function PC(t,n,e){return RM(t,n,e)}function AM(t,n,e){return t.type&40?en(t,e):null}var RM=AM,Cy;function Sp(t,n,e,i){let r=OC(t,i,n),o=n[ke],s=i.parent||n[yt],a=PC(s,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)gy(o,r,e[l],a,!1);else gy(o,r,e,a,!1);Cy!==void 0&&Cy(o,i,n,e,r)}function _a(t,n){if(n!==null){let e=n.type;if(e&3)return en(n,t);if(e&4)return Bm(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return _a(t,i);{let r=t[n.index];return Jt(r)?Bm(-1,r):ht(r)}}else{if(e&128)return _a(t,n.next);if(e&32)return vp(n,t)()||ht(t[n.index]);{let i=FC(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=si(t[At]);return _a(r,i)}else return _a(t,n.next)}}}return null}function FC(t,n){if(n!==null){let i=t[At][yt],r=n.projection;return i.projection[r]}return null}function Bm(t,n){let e=Be+t+1;if(e<n.length){let i=n[e],r=i[U].firstChild;if(r!==null)return _a(i,r)}return n[Rr]}function Dp(t,n,e,i,r,o,s){for(;e!=null;){let a=i[jn];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(s&&n===0&&(l&&Lo(ht(l),i),e.flags|=2),!Md(e))if(c&8)Dp(t,n,e.child,i,r,o,!1),Oo(n,t,a,r,l,e,o,i);else if(c&32){let u=vp(e,i),f;for(;f=u();)Oo(n,t,a,r,f,e,o,i);Oo(n,t,a,r,l,e,o,i)}else c&16?LC(t,n,i,e,r,o):Oo(n,t,a,r,l,e,o,i);e=s?e.projectionNext:e.next}}function Fd(t,n,e,i,r,o){t.type===3?OM(e,i,n,r,o):Dp(e,i,t.firstChild,n,r,o,!1)}function OM(t,n,e,i,r){let s=e[U].firstChild,a=s.next,l=ht(e[s.index]),c=ht(e[a.index]),u=a.index+1,f=e[u];if(n===1||n===0)i!==null&&(f&&f.hasChildNodes()?Br(t,i,f,r,!0):(Br(t,i,l,r,!0),Br(t,i,c,r,!0)));else if(n===2){if(f||(f=document.createDocumentFragment(),e[u]=f),l&&l.parentNode===f)return;let h=l;for(;h!==null;){let m=h.nextSibling;if(f.appendChild(h),h===c)break;h=m}}}function PM(t,n,e){let i=n[ke],r=OC(t,e,n),o=e.parent||n[yt],s=PC(o,e,n);LC(i,0,n,e,r,s)}function LC(t,n,e,i,r,o){let s=e[At],l=s[yt].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];Oo(n,t,e[jn],r,u,i,o,e)}else{let c=l,u=s[tt];sC(i)&&(c.flags|=128),Dp(t,n,c,u,r,o,!0)}}function FM(t,n,e,i,r,o,s){let a=i[Rr],l=ht(i);if(a!==l&&Oo(n,t,e,o,a,r,s),(i[K]&4)===0)for(let c=Be;c<i.length;c++){let u=i[c];Fd(u[U],u,t,n,o,a)}}function LM(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Gn.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Gn.Important),t.setStyle(e,i,r,o))}}function Ep(t,n,e,i,r,o,s,a,l,c,u){let f=Ge+i,h=f+r,m=VM(f,h),v=typeof c=="function"?c():c;return m[U]={type:t,blueprint:m,template:e,queries:null,viewQuery:a,declTNode:n,data:m.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:l,consts:v,incompleteFirstPass:!1,ssrId:u}}function VM(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:on);return e}function BM(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=Ep(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function wp(t,n,e,i,r,o,s,a,l,c,u){let f=n.blueprint.slice();return f[gn]=r,f[K]=i|4|128|8|64|1024,(c!==null||t&&t[K]&2048)&&(f[K]|=2048),$h(f),f[tt]=f[Li]=t,f[We]=e,f[Un]=s||t&&t[Un],f[ke]=a||t&&t[ke],f[jn]=l||t&&t[jn]||null,f[yt]=o,f[zn]=TT(),f[Nr]=u,f[Bh]=c,f[At]=n.type==2?t[At]:f,f}function jM(t,n,e){let i=en(n,t),r=BM(e),o=t[Un].rendererFactory,s=xp(t,wp(t,r,null,VC(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function VC(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function BC(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function xp(t,n){return t[Mo]?t[Vh][Qt]=n:t[Mo]=n,t[Vh]=n,n}function y(t=1){jC(je(),ae(),ci()+t,!1)}function jC(t,n,e,i){if(!i)if((n[K]&3)===3){let o=t.preOrderCheckHooks;o!==null&&cd(n,o,e)}else{let o=t.preOrderHooks;o!==null&&dd(n,o,0,e)}Bi(e)}var Ld=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Ld||{});function zr(t,n,e,i){let r=B(null);try{let[o,s,a]=t.inputs[e],l=null;(s&Ld.SignalBased)!==0&&(l=n[o][Ne]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):$y(n,l,o,i)}finally{B(r)}}function UC(t,n,e,i,r){let o=ci(),s=i&2;try{Bi(-1),s&&n.length>Ge&&jC(t,n,Ge,!1);let a=s?ve.TemplateUpdateStart:ve.TemplateCreateStart;De(a,r,e),e(i,r)}finally{Bi(o);let a=s?ve.TemplateUpdateEnd:ve.TemplateCreateEnd;De(a,r,e)}}function Ip(t,n,e){qM(t,n,e),(e.flags&64)===64&&YM(t,n,e)}function Vd(t,n,e=en){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function UM(t,n,e,i){let o=i.get(pC,mC)||e===Cn.ShadowDom||e===Cn.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return HM(s),s}function HM(t){zM(t)}var zM=()=>null;function $M(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function WM(t,n,e,i,r,o){let s=n[U];if(Tp(t,s,n,e,i)){li(t)&&GM(n,t.index);return}t.type&3&&(e=$M(e)),HC(t,n,e,i,r,o)}function HC(t,n,e,i,r,o){if(t.type&3){let s=en(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function GM(t,n){let e=tn(n,t);e[K]&16||(e[K]|=64)}function qM(t,n,e){let i=e.directiveStart,r=e.directiveEnd;li(e)&&jM(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||gd(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],l=ya(n,t,s,e);if(Lo(l,n),o!==null&&QM(n,s-i,l,a,e,o),vn(a)){let c=tn(e.index,n);c[We]=ya(n,t,s,e)}}}function YM(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=zb();try{Bi(o);for(let a=i;a<r;a++){let l=t.data[a],c=n[a];Xc(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&ZM(l,c)}}finally{Bi(-1),Xc(s)}}function ZM(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function zC(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];NC(n,o.selectors,!1)&&(i??=[],vn(o)?i.unshift(o):i.push(o))}return i}function KM(t,n,e,i,r,o){let s=en(t,n);XM(n[ke],s,o,t.value,e,i,r)}function XM(t,n,e,i,r,o,s){if(o==null)s?.(o,i||"",r),t.removeAttribute(n,r,e);else{let a=s==null?Nh(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function QM(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let l=s[a],c=s[a+1];zr(i,e,l,c)}}function $C(t,n,e,i,r){let o=Ge+e,s=n[U],a=r(s,n,t,i,e);n[o]=a,ko(t,!0);let l=t.type===2;return l?(TC(n[ke],a,t),(Pb()===0||ca(t))&&Lo(a,n),Fb()):Lo(a,n),nd()&&(!l||!Md(t))&&Sp(s,n,a,t),t}function WC(t){let n=t;return em()?tm():(n=n.parent,ko(n,!1)),n}function JM(t,n){let e=t[jn];if(!e)return;let i;try{i=e.get(rn,null)}catch{i=null}i?.(n)}function Tp(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let l=0;l<s.length;l+=2){let c=s[l],u=s[l+1],f=n.data[c];zr(f,e[c],u,r),a=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];zr(u,c,i,r),a=!0}return a}function eN(t,n,e,i,r,o){let s=null,a=null,l=null,c=!1,u=t.directiveToIndex.get(i.type);if(typeof u=="number"?s=u:[s,a,l]=u,a!==null&&l!==null&&t.hostDirectiveInputs?.hasOwnProperty(r)){let f=t.hostDirectiveInputs[r];for(let h=0;h<f.length;h+=2){let m=f[h];if(m>=a&&m<=l){let v=n.data[m],S=f[h+1];zr(v,e[m],S,o),c=!0}else if(m>l)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(zr(i,e[s],r,o),c=!0),c}function tN(t,n){let e=tn(n,t),i=e[U];nN(i,e);let r=e[gn];r!==null&&e[Nr]===null&&(e[Nr]=gC(r,e[jn])),De(ve.ComponentStart);try{Mp(i,e,e[We])}finally{De(ve.ComponentEnd,e[We])}}function nN(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function Mp(t,n,e){Jc(n);try{let i=t.viewQuery;i!==null&&Mm(1,i,e);let r=t.template;r!==null&&UC(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Hn]?.finishViewCreation(t),t.staticContentQueries&&vC(t,n),t.staticViewQueries&&Mm(2,t.viewQuery,e);let o=t.components;o!==null&&iN(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[K]&=-5,ed()}}function iN(t,n){for(let e=0;e<n.length;e++)tN(t,n[e])}function Ma(t,n,e,i){let r=B(null);try{let o=n.tView,a=t[K]&4096?4096:16,l=wp(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[Vi]=c;let u=t[Hn];return u!==null&&(l[Hn]=u.createEmbeddedView(o)),Mp(o,l,e),l}finally{B(r)}}function Vo(t,n){return!n||n.firstChild===null||sC(t)}function Sa(t,n,e,i,r=!1){if(t.type===3){let o=t.firstChild,s=o.next,a=ht(n[o.index]),l=ht(n[s.index]),c=a;for(;c!==null&&(i.push(c),c!==l);)c=c.nextSibling;return i}for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];if(o!==null)if(Jt(o)){let a=o[Rr];a!==o[gn]&&i.push(ht(o)),o[K]&4||GC(o,i),i.push(a)}else i.push(ht(o));let s=e.type;if(s&8)Sa(t,n,e.child,i);else if(s&32){let a=vp(e,n),l;for(;l=a();)i.push(l)}else if(s&16){let a=FC(n,e);if(Array.isArray(a))i.push(...a);else{let l=si(n[At]);Sa(l[U],l,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function GC(t,n){for(let e=Be;e<t.length;e++){let i=t[e],r=i[U].firstChild;r!==null&&Sa(i[U],i,r,n)}}function qC(t){if(t[Ar]!==null){for(let n of t[Ar])n.impl.addSequence(n);t[Ar].length=0}}var YC=[];function rN(t){return t[$t]??oN(t)}function oN(t){let n=YC.pop()??Object.create(aN);return n.lView=t,n}function sN(t){t.lView[$t]!==t&&(t.lView=null,YC.push(t))}var aN=P(p({},Qn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Fr(t.lView)},consumerOnSignalRead(){this.lView[$t]=this}});function lN(t){let n=t[$t]??Object.create(cN);return n.lView=t,n}var cN=P(p({},Qn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=si(t.lView);for(;n&&!ZC(n[U]);)n=si(n);n&&Wh(n)},consumerOnSignalRead(){this.lView[$t]=this}});function ZC(t){return t.type!==2}function KC(t){if(t[Oi]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Oi])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[K]&8192)}}var dN=100;function XC(t,n=0){let i=t[Un].rendererFactory,r=!1;r||i.begin?.();try{uN(t,n)}finally{r||i.end?.()}}function uN(t,n){let e=nm();try{Qs(!0),jm(t,n);let i=0;for(;da(t);){if(i===dN)throw new C(103,!1);i++,jm(t,1)}}finally{Qs(e)}}function fN(t,n,e,i){if(Pr(n))return;let r=n[K],o=!1,s=!1;Jc(n);let a=!0,l=null,c=null;o||(ZC(t)?(c=rN(n),l=On(c)):Xl()===null?(a=!1,c=lN(n),l=On(c)):n[$t]&&(ti(n[$t]),n[$t]=null));try{$h(n),jb(t.bindingStartIndex),e!==null&&UC(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let m=t.preOrderCheckHooks;m!==null&&cd(n,m,null)}else{let m=t.preOrderHooks;m!==null&&dd(n,m,0,null),hm(n,0)}if(s||hN(n),KC(n),QC(n,0),t.contentQueries!==null&&vC(t,n),!o)if(u){let m=t.contentCheckHooks;m!==null&&cd(n,m)}else{let m=t.contentHooks;m!==null&&dd(n,m,1),hm(n,1)}pN(t,n);let f=t.components;f!==null&&eS(n,f,0);let h=t.viewQuery;if(h!==null&&Mm(2,h,i),!o)if(u){let m=t.viewCheckHooks;m!==null&&cd(n,m)}else{let m=t.viewHooks;m!==null&&dd(n,m,2),hm(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Gc]){for(let m of n[Gc])m();n[Gc]=null}o||(qC(n),n[K]&=-73)}catch(u){throw o||Fr(n),u}finally{c!==null&&(ei(c,l),a&&sN(c)),ed()}}function QC(t,n){for(let e=lC(t);e!==null;e=cC(e))for(let i=Be;i<e.length;i++){let r=e[i];JC(r,n)}}function hN(t){for(let n=lC(t);n!==null;n=cC(n)){if(!(n[K]&2))continue;let e=n[Or];for(let i=0;i<e.length;i++){let r=e[i];Wh(r)}}}function mN(t,n,e){De(ve.ComponentStart);let i=tn(n,t);try{JC(i,e)}finally{De(ve.ComponentEnd,i[We])}}function JC(t,n){Yc(t)&&jm(t,n)}function jm(t,n){let i=t[U],r=t[K],o=t[$t],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&pr(o)),s||=!1,o&&(o.dirty=!1),t[K]&=-9217,s)fN(i,t,i.template,t[We]);else if(r&8192){let a=B(null);try{KC(t),QC(t,1);let l=i.components;l!==null&&eS(t,l,1),qC(t)}finally{B(a)}}}function eS(t,n,e){for(let i=0;i<n.length;i++)mN(t,n[i],e)}function pN(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Bi(~r);else{let o=r,s=e[++i],a=e[++i];Hb(s,o);let l=n[o];De(ve.HostBindingsUpdateStart,l);try{a(2,l)}finally{De(ve.HostBindingsUpdateEnd,l)}}}}finally{Bi(-1)}}function Np(t,n){let e=nm()?64:1088;for(t[Un].changeDetectionScheduler?.notify(n);t;){t[K]|=e;let i=si(t);if(No(t)&&!i)return t;t=i}return null}function tS(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function nS(t,n){let e=Be+n;if(e<t.length)return t[e]}function Na(t,n,e,i=!0){let r=n[U];if(gN(r,n,t,e),i){let s=Bm(e,t),a=n[ke],l=a.parentNode(t[Rr]);l!==null&&IM(r,t[yt],a,n,l,s)}let o=n[Nr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function iS(t,n){let e=Da(t,n);return e!==void 0&&Pd(e[U],e),e}function Da(t,n){if(t.length<=Be)return;let e=Be+n,i=t[e];if(i){let r=i[Vi];r!==null&&r!==t&&Cp(r,i),n>0&&(t[e-1][Qt]=i[Qt]);let o=oa(t,Be+n);xM(i[U],i);let s=o[Hn];s!==null&&s.detachView(o[U]),i[tt]=null,i[Qt]=null,i[K]&=-129}return i}function gN(t,n,e,i){let r=Be+i,o=e.length;i>0&&(e[r-1][Qt]=n),i<o-Be?(n[Qt]=e[r],Rh(e,Be+i,n)):(e.push(n),n[Qt]=null),n[tt]=e;let s=n[Vi];s!==null&&e!==s&&rS(s,n);let a=n[Hn];a!==null&&a.insertView(t),Zc(n),n[K]|=128}function rS(t,n){let e=t[Or],i=n[tt];if(ai(i))t[K]|=2;else{let r=i[tt][At];n[At]!==r&&(t[K]|=2)}e===null?t[Or]=[n]:e.push(n)}var Hi=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[U];return Sa(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[We]}set context(n){this._lView[We]=n}get destroyed(){return Pr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[tt];if(Jt(n)){let e=n[la],i=e?e.indexOf(this):-1;i>-1&&(Da(n,i),oa(e,i))}this._attachedToViewContainer=!1}Pd(this._lView[U],this._lView)}onDestroy(n){Kc(this._lView,n)}markForCheck(){Np(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[K]&=-129}reattach(){Zc(this._lView),this._lView[K]|=128}detectChanges(){this._lView[K]|=1024,XC(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new C(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=No(this._lView),e=this._lView[Vi];e!==null&&!n&&Cp(e,this._lView),RC(this._lView[U],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new C(902,!1);this._appRef=n;let e=No(this._lView),i=this._lView[Vi];i!==null&&!e&&rS(i,this._lView),Zc(this._lView)}};var jt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=vN;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Ma(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Hi(o)}}return t})();function vN(){return Bd(st(),ae())}function Bd(t,n){return t.type&4?new jt(n,t,Uo(t,n)):null}function Ho(t,n,e,i,r){let o=t.data[n];if(o===null)o=_N(t,n,e,i,r),Ub()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=Lb();o.injectorIndex=s===null?-1:s.injectorIndex}return ko(o,!0),o}function _N(t,n,e,i,r){let o=Jh(),s=em(),a=s?o:o&&o.parent,l=t.data[n]=yN(t,a,e,n,i,r);return bN(t,l,o,s),l}function bN(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function yN(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return Kh()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:sm(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function CN(t){let n=t[jh]??[],i=t[tt][ke],r=[];for(let o of n)o.data[hC]!==void 0?r.push(o):SN(o,i);t[jh]=r}function SN(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[fC];for(;e<r;){let o=i.nextSibling;IC(n,i,!1),i=o,e++}}}var DN=()=>null,EN=()=>null;function vd(t,n){return DN(t,n)}function oS(t,n,e){return EN(t,n,e)}var sS=class{},Ze=class{},we=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>wN()};function wN(){let t=ae(),n=st(),e=tn(n.index,t);return(ai(e)?e:t)[ke]}var aS=(()=>{class t{static \u0275prov=se({token:t,providedIn:"root",factory:()=>null})}return t})();function lS(t){return t.debugInfo?.className||t.type.name||null}var fd={},_d=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,fd,i);return r!==fd||e===fd?r:this.parentInjector.get(n,e,i)}};function xN(t,n,e){return t[n]=e}function IN(t,n){return t[n]}function zi(t,n,e){if(e===on)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function jr(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&NT(r,o);let s=li(t)?tn(t.index,n):n;Np(s,5);let a=n[We],l=Sy(n,a,e,r),c=i.__ngNextListenerFn__;for(;c;)l=Sy(n,a,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function Sy(t,n,e,i){let r=B(null);try{return De(ve.OutputStart,n,e),e(i)!==!1}catch(o){return JM(t,o),!1}finally{De(ve.OutputEnd,n,e),B(r)}}function kp(t,n,e,i,r,o,s,a){let l=ca(t),c=!1,u=null;if(!i&&l&&(u=MN(n,e,o,t.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,c=!0}else{let f=en(t,e),h=i?i(f):f;AT(e,h,o,a),i||(a.__ngNativeEl__=f);let m=r.listen(h,o,a);if(!TN(o)){let v=i?S=>i(ht(S[t.index])):t.index;cS(v,n,e,o,a,m,!1)}}return c}function TN(t){return t.startsWith("animation")||t.startsWith("transition")}function MN(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[To],l=r[o+2];return a&&a.length>l?a[l]:null}typeof s=="string"&&(o+=2)}return null}function cS(t,n,e,i,r,o,s){let a=n.firstCreatePass?qh(n):null,l=Gh(e),c=l.length;l.push(r,o),a&&a.push(i,t,c,(c+1)*(s?-1:1))}function Dy(t,n,e,i,r){let o=null,s=null,a=null,l=!1,c=t.directiveToIndex.get(e.type);if(typeof c=="number"?o=c:[o,s,a]=c,s!==null&&a!==null&&t.hostDirectiveOutputs?.hasOwnProperty(i)){let u=t.hostDirectiveOutputs[i];for(let f=0;f<u.length;f+=2){let h=u[f];if(h>=s&&h<=a)l=!0,bd(t,n,h,u[f+1],i,r);else if(h>a)break}}return e.outputs.hasOwnProperty(i)&&(l=!0,bd(t,n,o,i,i,r)),l}function bd(t,n,e,i,r,o){let s=n[e],a=n[U],c=a.data[e].outputs[i],f=s[c].subscribe(o);cS(t.index,a,n,r,o,f,!0)}function Ke(){NN()}function NN(){let t=ae(),n=je(),e=st();if(n.firstCreatePass&&AN(n,e),e.controlDirectiveIndex===-1)return;pi("NgSignalForms");let i=t[e.controlDirectiveIndex];n.data[e.controlDirectiveIndex].controlDef.create(i,new yd(t,n,e))}function Xe(){kN()}function kN(){let t=ae(),n=je(),e=fa();if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef,r=t[e.controlDirectiveIndex];i.update(r,new yd(t,n,e))}var yd=class{lView;tView;tNode;hasPassThrough;constructor(n,e,i){this.lView=n,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return en(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,e){let i=this.tView.data[this.tNode.customControlIndex];Dy(this.tNode,this.lView,i,n,jr(this.tNode,this.lView,e))}listenToCustomControlModel(n){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];Dy(this.tNode,this.lView,i,e,jr(this.tNode,this.lView,n))}listenToDom(n,e){kp(this.tNode,this.tView,this.lView,void 0,this.lView[ke],n,e,jr(this.tNode,this.lView,e))}setInputOnDirectives(n,e){let i=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],l=this.lView[s];zr(a,l,n,e),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let l=r[s+1],c=this.tView.data[a],u=this.lView[a];zr(c,u,l,e),o=!0}return o}setCustomControlModelInput(n){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";eN(this.tNode,this.tView,this.lView,e,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[n]===!0}_buildCustomControlInputCache(n){let e={};for(let i in n.inputs)e[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)e[r.inputs[s]]=!0;let o=Ey(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let l=o.inputs[a+1]||o.inputs[a];e[l]=!0}let s=Ey(o.directive);s!==null&&i.push(...s)}}}return e}};function Ey(t){return typeof t=="function"&&"\u0275dir"in t?t.\u0275dir.hostDirectives??null:null}function AN(t,n,e){for(let r=n.directiveStart;r<n.directiveEnd;r++)if(t.data[r].controlDef){n.controlDirectiveIndex=r;break}if(n.controlDirectiveIndex===-1)return;let i=t.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}RN(t,n)}function RN(t,n){for(let e=n.directiveStart;e<n.directiveEnd;e++){let i=t.data[e];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(wy(i,"value")){n.flags|=1024,n.customControlIndex=e;return}if(wy(i,"checked")){n.flags|=2048,n.customControlIndex=e;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let e=(i,r)=>{let o=n.hostDirectiveInputs[i],s=n.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let l=o[a];for(let c=0;c<s.length;c+=2){let u=s[c];if(l===u)for(let f of n.directiveToIndex.values()){if(!Array.isArray(f))continue;let[h,m,v]=f;if(l>=m&&l<=v)return n.flags|=r,n.customControlIndex=h,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function wy(t,n){return ON(t,n)&&PN(t,n+"Change")}function ON(t,n){return n in t.inputs}function PN(t,n){return n in t.outputs}var Um=Symbol("BINDING");var Gr=new g("");function Cd(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=Vc(r,a);else if(o==2){let l=a,c=n[++s];i=Vc(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function lt(t,n=0){let e=ae();if(e===null)return j(t,n);let i=st();return nC(i,e,et(t),n)}function dS(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,l=null,c=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,l,c]=u.resolveHostDirectives(s);break}VN(t,n,e,a,o,l,c)}o!==null&&i!==null&&FN(e,i,o)}function FN(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new C(-301,!1);i.push(n[r],o)}}function LN(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function VN(t,n,e,i,r,o,s){let a=i.length,l=null;for(let h=0;h<a;h++){let m=i[h];l===null&&vn(m)&&(l=m,LN(t,e,h)),wm(gd(e,n),t,m.type)}$N(e,t.data.length,a),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let h=0;h<a;h++){let m=i[h];m.providersResolver&&m.providersResolver(m)}let c=!1,u=!1,f=BC(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let h=0;h<a;h++){let m=i[h];if(e.mergedAttrs=Fo(e.mergedAttrs,m.hostAttrs),jN(t,e,n,f,m),zN(f,m,r),s!==null&&s.has(m)){let[S,M]=s.get(m);e.directiveToIndex.set(m.type,[f,S+e.directiveStart,M+e.directiveStart])}else(o===null||!o.has(m))&&e.directiveToIndex.set(m.type,f);m.contentQueries!==null&&(e.flags|=4),(m.hostBindings!==null||m.hostAttrs!==null||m.hostVars!==0)&&(e.flags|=64);let v=m.type.prototype;!c&&(v.ngOnChanges||v.ngOnInit||v.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(v.ngOnChanges||v.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),f++}BN(t,e,o)}function BN(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))xy(0,n,r,i),xy(1,n,r,i),Ty(n,i,!1);else{let o=e.get(r);Iy(0,n,o,i),Iy(1,n,o,i),Ty(n,i,!0)}}}function xy(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),uS(n,o)}}function Iy(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),uS(n,s)}}function uS(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Ty(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||gp(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let l=i[a];if(l===0){a+=4;continue}else if(l===5){a+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===n){s??=[],s.push(l,i[a+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){s??=[],s.push(c[u+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function jN(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=wr(r.type,!0)),s=new Ur(o,vn(r),lt,null);t.blueprint[i]=s,e[i]=s,UN(t,n,i,BC(t,e,r.hostVars,on),r)}function UN(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;HN(s)!=a&&s.push(a),s.push(e,i,o)}}function HN(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function zN(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;vn(n)&&(e[""]=t)}}function $N(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function fS(t,n,e,i,r,o,s,a){let l=n[U],c=l.consts,u=nn(c,s),f=Ho(l,t,e,i,u);return o&&dS(l,n,f,nn(c,a),r),f.mergedAttrs=Fo(f.mergedAttrs,f.attrs),f.attrs!==null&&Cd(f,f.attrs,!1),f.mergedAttrs!==null&&Cd(f,f.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,f),f}function hS(t,n){qy(t,n),Uh(n)&&t.queries.elementEnd(n)}function WN(t,n,e,i,r,o){let s=n.consts,a=nn(s,r),l=Ho(n,t,e,i,a);if(l.mergedAttrs=Fo(l.mergedAttrs,l.attrs),o!=null){let c=nn(s,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&Cd(l,l.attrs,!1),l.mergedAttrs!==null&&Cd(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}var mS=typeof ShadowRoot<"u",GN=typeof Document<"u";function qN(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Ld.SignalBased)!==0};return r&&(o.transform=r),o})}function YN(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function ZN(t,n,e){let i=n instanceof xe?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new _d(e,i):e}function KN(t){let n=t.get(Ze,null);if(n===null)throw new C(407,!1);let e=t.get(aS,null),i=t.get(Ln,null),r=t.get(Yn,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function XN(t,n){let e=pS(t);return wC(n,e,e==="svg"?Hh:e==="math"?Tb:null)}function QN(t){if(t?.toLowerCase()==="script")throw new C(905,!1)}function pS(t){return(t.selectors[0][0]||"div").toLowerCase()}var Bo=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=qN(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=YN(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=fM(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){De(ve.DynamicComponentStart);let a=B(null);try{let l=this.componentDef,c=ZN(l,r||this.ngModule,n),u=KN(c),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(lS(l),()=>this.createComponentRef(u,c,e,i,o,s)):this.createComponentRef(u,c,e,i,o,s)}finally{B(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,l=JN(r,a,s,o),c=n.rendererFactory.createRenderer(null,a),u=r?UM(c,r,a.encapsulation,e):XN(a,c);QN(u?.tagName);let f=e.get(Gr,null),h=ek(u,()=>e.get(L,null)??uC());f&&f.addHost(h);let m=s?.some(My)||o?.some(M=>typeof M!="function"&&M.bindings.some(My)),v=wp(null,l,null,512|VC(a),null,null,n,c,e,null,gC(u,e,!0));f&&mS&&h instanceof ShadowRoot&&Kc(v,()=>{f.removeHost(h)}),v[Ge]=u,Jc(v);let S=null;try{let M=fS(Ge,v,2,"#host",()=>l.directiveRegistry,!0,0);TC(c,u,M),Lo(u,v),Ip(l,v,M),_C(l,M,v),hS(l,M),i!==void 0&&nk(M,this.ngContentSelectors,i),S=tn(M.index,v),v[We]=S[We],Mp(l,v,null)}catch(M){throw S!==null&&Im(S),Im(v),M}finally{De(ve.DynamicComponentEnd),ed()}return new Sd(this.componentType,v,!!m)}};function JN(t,n,e,i){let r=t?["ng-version","22.1.0"]:hM(n.selectors[0]),o=null,s=null,a=0;if(e)for(let u of e)a+=u[Um].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let h of f.bindings){a+=h[Um].requiredVars;let m=u+1;h.create&&(h.targetIdx=m,(o??=[]).push(h)),h.update&&(h.targetIdx=m,(s??=[]).push(h))}}let l=[n];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,h=jc(f);l.push(h)}return Ep(0,null,tk(o,s),1,a,l,null,null,null,[r],null)}function ek(t,n){let e=t.getRootNode?.();return GN&&e instanceof Document?e.head:e&&mS&&e instanceof ShadowRoot?e:n().head}function tk(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function My(t){let n=t[Um].kind;return n==="input"||n==="twoWay"}var Sd=class extends sS{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=qc(e[U],Ge),this.location=Uo(this._tNode,e),this.instance=tn(this._tNode.index,e)[We],this.hostView=this.changeDetectorRef=new Hi(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=Tp(i,r[U],r,n,e);this.previousInputValues.set(n,e);let s=tn(i.index,r);Np(s,1)}get injector(){return new ji(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function nk(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Rt=(()=>{class t{static __NG_ELEMENT_ID__=ik}return t})();function ik(){let t=st();return gS(t,ae())}var Hm=class t extends Rt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Uo(this._hostTNode,this._hostLView)}get injector(){return new ji(this._hostTNode,this._hostLView)}get parentInjector(){let n=op(this._hostTNode,this._hostLView);if(Ky(n)){let e=pd(n,this._hostLView),i=md(n),r=e[U].data[i+8];return new ji(r,e)}else return new ji(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Ny(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-Be}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=vd(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,Vo(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let l,c=e||{};l=c.index,i=c.injector,r=c.projectableNodes,o=c.environmentInjector||c.ngModuleRef,s=c.directives,a=c.bindings;let u=new Bo(Pi(n)),f=i||this.parentInjector;if(!o&&u.ngModule==null){let O=this.parentInjector.get(xe,null);O&&(o=O)}let h=Pi(u.componentType??{}),m=vd(this._lContainer,h?.id??null),v=m?.firstChild??null,S=u.create(f,r,v,o,s,a);return this.insertImpl(S.hostView,l,Vo(this._hostTNode,m)),S}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(kb(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let l=r[tt],c=new t(l,l[yt],l[tt]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Na(s,r,o,i),n.attachToViewContainerRef(),Rh(gm(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Ny(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Da(this._lContainer,e);i&&(oa(gm(this._lContainer),e),Pd(i[U],i))}detach(n){let e=this._adjustIndex(n,-1),i=Da(this._lContainer,e);return i&&oa(gm(this._lContainer),e)!=null?new Hi(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Ny(t){return t[la]}function gm(t){return t[la]||(t[la]=[])}function gS(t,n){let e,i=n[t.index];return Jt(i)?e=i:(e=tS(i,n,null,t),n[t.index]=e,xp(n,e)),ok(e,n,t,i),new Hm(e,t,n)}function rk(t,n){let e=t[ke],i=e.createComment(""),r=en(n,t),o=e.parentNode(r);return Br(e,o,i,e.nextSibling(r),!1),i}var ok=lk,sk=()=>!1;function ak(t,n,e){return sk(t,n,e)}function lk(t,n,e,i){if(t[Rr])return;let r;e.type&8?r=ht(i):r=rk(n,e),t[Rr]=r}var zm=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},$m=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Rp(n,e).matches!==null&&this.queries[e].setDirty()}},Dd=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=hk(n):this.predicate=n}},Wm=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Gm=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,ck(e,o)),this.matchTNodeWithReadOption(n,e,ud(e,n,o,!1,!1))}else i===jt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,ud(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===F||r===Rt||r===jt&&e.type&4)this.addMatch(e.index,-2);else{let o=ud(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function ck(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function dk(t,n){return t.type&11?Uo(t,n):t.type&4?Bd(t,n):null}function uk(t,n,e,i){return e===-1?dk(n,t):e===-2?fk(t,n,i):ya(t,t[U],e,n)}function fk(t,n,e){if(e===F)return Uo(n,t);if(e===jt)return Bd(n,t);if(e===Rt)return gS(n,t)}function vS(t,n,e,i){let r=n[Hn].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let l=0;s!==null&&l<s.length;l+=2){let c=s[l];if(c<0)a.push(null);else{let u=o[c];a.push(uk(n,u,s[l+1],e.metadata.read))}}r.matches=a}return r.matches}function qm(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=vS(t,n,r,e);for(let a=0;a<o.length;a+=2){let l=o[a];if(l>0)i.push(s[a/2]);else{let c=o[a+1],u=n[-l];for(let f=Be;f<u.length;f++){let h=u[f];h[Vi]===h[tt]&&qm(h[U],h,c,i)}if(u[Or]!==null){let f=u[Or];for(let h=0;h<f.length;h++){let m=f[h];qm(m[U],m,c,i)}}}}}return i}function Ap(t,n){return t[Hn].queries[n].queryList}function _S(t,n,e){let i=new Hr((e&4)===4);return Ob(t,n,i,i.destroy),(n[Hn]??=new $m).queries.push(new zm(i))-1}function bS(t,n,e){let i=je();return i.firstCreatePass&&(CS(i,new Dd(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),_S(i,ae(),n)}function yS(t,n,e,i){let r=je();if(r.firstCreatePass){let o=st();CS(r,new Dd(n,e,i),o.index),mk(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return _S(r,ae(),e)}function hk(t){return t.split(",").map(n=>n.trim())}function CS(t,n,e){t.queries===null&&(t.queries=new Wm),t.queries.track(new Gm(n,e))}function mk(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Rp(t,n){return t.queries.getByIndex(n)}function SS(t,n){let e=t[U],i=Rp(e,n);return i.crossesNgTemplate?qm(e,t,n,[]):vS(e,t,i,n)}function DS(t,n,e){let i,r=Ls(()=>{i._dirtyCounter();let o=pk(i,t);if(n&&o===void 0)throw new C(-951,!1);return o});return i=r[Ne],i._dirtyCounter=H(0),i._flatValue=void 0,r}function Op(t){return DS(!0,!1,t)}function Pp(t){return DS(!0,!0,t)}function ES(t,n){let e=t[Ne];e._lView=ae(),e._queryIndex=n,e._queryList=Ap(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function pk(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[K]&4)return n?void 0:_t;let r=Ap(e,i),o=SS(e,i);return r.reset(o,oC),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function gi(t){return!!t&&typeof t.then=="function"}function Fp(t){return!!t&&typeof t.subscribe=="function"}var qn=class{},jd=class{};var Ed=class extends qn{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=fb(n);this._bootstrapComponents=tM(o.bootstrap),this._r3Injector=am(n,e,[{provide:qn,useValue:this},...i],ta(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},wd=class extends jd{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Ed(this.moduleType,n,[])}};var Ea=class extends qn{injector;instance=null;constructor(n){super();let e=new Ir([...n.providers,{provide:qn,useValue:this}],n.parent||Io(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function ka(t,n,e=null){return new Ea({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var gk=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Ph(!1,e.type),r=i.length>0?ka([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=se({token:t,providedIn:"environment",factory:()=>new t(j(xe))})}return t})();function x(t){return Ia(()=>{let n=wS(t),e=P(p({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==ap.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(gk).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Cn.Emulated,styles:t.styles||_t,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&pi("NgStandalone"),xS(e);let i=t.dependencies;return e.directiveDefs=ky(i,vk),e.pipeDefs=ky(i,hb),e.id=yk(e),e})}function vk(t){return Pi(t)||jc(t)}function te(t){return Ia(()=>({type:t.type,bootstrap:t.bootstrap||_t,declarations:t.declarations||_t,imports:t.imports||_t,exports:t.exports||_t,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function _k(t,n){if(t==null)return Fi;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,l;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,l=r[3]||null):(o=r,s=r,a=Ld.None,l=null),e[o]=[i,a,l],n[o]=s}return e}function bk(t){if(t==null)return Fi;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function N(t){return Ia(()=>{let n=wS(t);return xS(n),n})}function wS(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Fi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||_t,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:_k(t.inputs,n),outputs:bk(t.outputs),debugInfo:null}}function xS(t){t.features?.forEach(n=>n(t))}function ky(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function yk(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var IS=new g("");var Lp=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(IS,{optional:!0})??[];injector=d(V);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=Ve(this.injector,r);if(gi(o))e.push(o);else if(Fp(o)){let s=new Promise((a,l)=>{o.subscribe({complete:a,error:l})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function Ud(t){return n=>{n.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:t}}}function Vp(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=Ck,e.hostDirectives=i?t.map(Ym):[t]):i?e.hostDirectives.unshift(...t.map(Ym)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function Ck(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let s=t[o];if(s.hostDirectives!==null){let a=n.length;i??=new Map,r??=new Map,TS(s,n,i,t),r.set(s,[a,n.length-1])}o===0&&vn(s)&&(e=!0,n.push(s))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return i!==null&&i.forEach((o,s)=>{Sk(s.declaredInputs,o.inputs)}),[n,i,r]}function TS(t,n,e,i){if(t.hostDirectives!==null)for(let r of t.hostDirectives)if(typeof r=="function"){let o=r();for(let s of o)Ay(Ym(s),n,e,i)}else Ay(r,n,e,i)}function Ay(t,n,e,i){let r=jc(t.directive);if(TS(r,n,e,i),e.has(r)){let o=e.get(r);Ry(o,t.inputs,"input"),Ry(o,t.outputs,"output")}else i.includes(r)||(e.set(r,t),n.push(r))}function Ry(t,n,e){let i=e==="input"?t.inputs:t.outputs;Object.keys(n).forEach(r=>{let o=n[r];(!i.hasOwnProperty(r)||i[r]===o)&&(i[r]=o)})}function Ym(t){return typeof t=="function"?{directive:et(t),inputs:{},outputs:{}}:{directive:et(t.directive),inputs:Oy(t.inputs),outputs:Oy(t.outputs)}}function Oy(t){let n={};if(t!==void 0&&t.length>0)for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function Sk(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function Dk(t){return Object.getPrototypeOf(t.prototype).constructor}function Ae(t){let n=Dk(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,ia)?n[ia]:void 0,s=Object.hasOwn(n,ra)?n[ra]:void 0;if(vn(t))r=o??s;else{if(o)throw new C(903,!1);r=s}if(r){if(e){i.push(r);let l=t;l.inputs=vm(t.inputs),l.declaredInputs=vm(t.declaredInputs),l.outputs=vm(t.outputs);let c=r.hostBindings;c&&Tk(t,c);let u=r.viewQuery,f=r.contentQueries;if(u&&xk(t,u),f&&Ik(t,f),Ek(t,r),ub(t.outputs,r.outputs),vn(r)&&r.data.animation){let h=t.data;h.animation=(h.animation||[]).concat(r.data.animation)}}let a=r.features;if(a)for(let l=0;l<a.length;l++){let c=a[l];c&&c.ngInherit&&c(t),c===Ae&&(e=!1)}}n=Object.getPrototypeOf(n)}wk(i)}function Ek(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function wk(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Fo(r.hostAttrs,e=Fo(e,r.hostAttrs))}}function vm(t){return t===Fi?{}:t===_t?[]:t}function xk(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function Ik(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function Tk(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function MS(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=Fo(t.mergedAttrs,t.attrs);let u=t.tView=Ep(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),ko(t,!1);let l=Nk(e,n,t,i);nd()&&Sp(e,n,l,t),Lo(l,n);let c=tS(l,n,l,t);n[i+Ge]=c,xp(n,c),ak(c,t,n)}function Mk(t,n,e,i,r,o,s,a,l,c,u){let f=e+Ge,h;return n.firstCreatePass?(h=Ho(n,f,4,s||null,a||null),Zh()&&dS(n,t,h,nn(n.consts,c),zC),qy(n,h)):h=n.data[f],MS(h,t,n,e,i,r,o,l),ca(h)&&Ip(n,t,h),c!=null&&Vd(t,h,u),h}function wa(t,n,e,i,r,o,s,a,l,c,u){let f=e+Ge,h;if(n.firstCreatePass){if(h=Ho(n,f,4,s||null,a||null),c!=null){let m=nn(n.consts,c);h.localNames=[];for(let v=0;v<m.length;v+=2)h.localNames.push(m[v],-1)}}else h=n.data[f];return MS(h,t,n,e,i,r,o,l),c!=null&&Vd(t,h,u),h}function Ot(t,n,e,i,r,o,s,a){let l=ae(),c=je(),u=nn(c.consts,o);return Mk(l,c,t,n,e,i,r,u,void 0,s,a),Ot}var Nk=kk;function kk(t,n,e,i){return id(!0),n[ke].createComment("")}var Hd=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var Bp=new g("");var Aa=new g("");function NS(){Yf(()=>{let t="";throw new C(600,t)})}var Ak=10;var Gt=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(rn);afterRenderManager=d(Rd);zonelessEnabled=d(pa);rootEffectScheduler=d(od);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new D;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(di);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(re(e=>!e))}constructor(){d(Yn,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(xe);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=V.NULL){return this._injector.get(T).run(()=>{if(De(ve.BootstrapComponentStart),!this._injector.get(Lp).done){let O="";throw new C(405,O)}let a=Pi(e),l=this._injector.get(qn),c=new Bo(a,l);this.componentTypes.push(e);let{hostElement:u,directives:f,bindings:h}=Rk(i),m=u||c.selector,v=c.create(r,[],m,l.injector,f,h),S=v.location.nativeElement,M=v.injector.get(Bp,null);return M?.registerApplication(S),v.onDestroy(()=>{this.detachView(v.hostView),ba(this.components,v),M?.unregisterApplication(S)}),this._loadComponent(v),De(ve.BootstrapComponentEnd,v),v})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){De(ve.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Ad.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw De(ve.ChangeDetectionEnd),new C(101,!1);let e=B(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,B(e),this.afterTick.next(),De(ve.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ze,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<Ak;){De(ve.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{De(ve.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!da(r))continue;let o=i&&!this.zonelessEnabled?0:1;XC(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>da(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;ba(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Aa,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>ba(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new C(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function Rk(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function ba(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function _e(t,n,e,i){let r=ae(),o=Lr();if(zi(r,o,n)){let s=je(),a=fa();KM(a,r,t,n,e,i)}return _e}var Zm=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function _m(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function Ok(t,n,e,i){let r,o,s=0,a=t.length-1,l=void 0;if(Array.isArray(n)){B(i);let c=n.length-1;for(B(null);s<=a&&s<=c;){let u=t.at(s),f=n[s],h=_m(s,u,s,f,e);if(h!==0){h<0&&t.updateValue(s,f),s++;continue}let m=t.at(a),v=n[c],S=_m(a,m,c,v,e);if(S!==0){S<0&&t.updateValue(a,v),a--,c--;continue}let M=e(s,u),O=e(a,m),pe=e(s,f);if(Object.is(pe,O)){let ye=e(c,v);Object.is(ye,M)?(t.swap(s,a),t.updateValue(a,v),c--,a--):t.move(a,s),t.updateValue(s,f),s++;continue}if(r??=new xd,o??=Fy(t,s,a,e),Km(t,r,s,pe))t.updateValue(s,f),s++,a++;else if(o.has(pe))r.set(M,t.detach(s)),a--;else{let ye=t.create(s,n[s]);t.attach(s,ye),s++,a++}}for(;s<=c;)Py(t,r,e,s,n[s]),s++}else if(n!=null){B(i);let c=n[Symbol.iterator]();B(null);let u=c.next();for(;!u.done&&s<=a;){let f=t.at(s),h=u.value,m=_m(s,f,s,h,e);if(m!==0)m<0&&t.updateValue(s,h),s++,u=c.next();else{r??=new xd,o??=Fy(t,s,a,e);let v=e(s,h);if(Km(t,r,s,v))t.updateValue(s,h),s++,a++,u=c.next();else if(!o.has(v))t.attach(s,t.create(s,h)),s++,a++,u=c.next();else{let S=e(s,f);r.set(S,t.detach(s)),a--}}}for(;!u.done;)Py(t,r,e,t.length,u.value),u=c.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(c=>{t.destroy(c)})}function Km(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function Py(t,n,e,i,r){if(Km(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function Fy(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var xd=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function Y(t,n,e,i,r,o,s,a){pi("NgControlFlow");let l=ae(),c=je(),u=nn(c.consts,o);return wa(l,c,t,n,e,i,r,u,256,s,a),jp}function jp(t,n,e,i,r,o,s,a){pi("NgControlFlow");let l=ae(),c=je(),u=nn(c.consts,o);return wa(l,c,t,n,e,i,r,u,512,s,a),jp}function Z(t,n){pi("NgControlFlow");let e=ae(),i=Lr(),r=e[i]!==on?e[i]:-1,o=r!==-1?Id(e,Ge+r):void 0,s=0;if(zi(e,i,t)){let a=B(null);try{if(o!==void 0&&iS(o,s),t!==-1){let l=Ge+t,c=Id(e,l),u=ep(e[U],l),f=oS(c,u,e),h=Ma(e,u,n,{dehydratedView:f});Na(c,h,s,Vo(u,f))}}finally{B(a)}}else if(o!==void 0){let a=nS(o,s);a!==void 0&&(a[We]=n)}}var Xm=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-Be}};function Sn(t){return t}function zo(t,n){return n}var Qm=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function ct(t,n,e,i,r,o,s,a,l,c,u,f,h){pi("NgControlFlow");let m=ae(),v=je(),S=l!==void 0,M=ae(),O=a?s.bind(M[At][We]):s,pe=new Qm(S,O);M[Ge+t]=pe,wa(m,v,t+1,n,e,i,r,nn(v.consts,o),256),S&&wa(m,v,t+2,l,c,u,f,nn(v.consts,h),512)}var Jm=class extends Zm{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-Be}at(n){return this.getLView(n)[We].$implicit}attach(n,e){let i=e[Nr];this.needsIndexUpdate||=n!==this.length,Na(this.lContainer,e,n,Vo(this.templateTNode,i)),Pk(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,Fk(this.lContainer,n),Lk(this.lContainer,n)}create(n,e){let i=vd(this.lContainer,this.templateTNode.tView.ssrId);return Ma(this.hostLView,this.templateTNode,new Xm(this.lContainer,e,n),{dehydratedView:i})}destroy(n){Pd(n[U],n)}updateValue(n,e){this.getLView(n)[We].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[We].$index=n}getLView(n){return Vk(this.lContainer,n)}};function dt(t){let n=B(null),e=ci();try{let i=ae(),r=i[U],o=i[e],s=e+1,a=Id(i,s);if(o.liveCollection===void 0){let c=ep(r,s);o.liveCollection=new Jm(a,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(Ok(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=Lr(),u=l.length===0;if(zi(i,c,u)){let f=e+2,h=Id(i,f);if(u){let m=ep(r,f),v=oS(h,m,i),S=Ma(i,m,void 0,{dehydratedView:v});Na(h,S,0,Vo(m,v))}else r.firstUpdatePass&&CN(h),iS(h,0)}}}finally{B(n)}}function Id(t,n){return t[n]}function Pk(t,n){if(t.length<=Be)return;let e=Be+n,i=t[e],r=i?i[$n]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[jn];yM(o,r),Ui.delete(i[zn]),r.detachedLeaveAnimationFns=void 0}}function Fk(t,n){if(t.length<=Be)return;let e=Be+n,i=t[e],r=i?i[$n]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function Lk(t,n){return Da(t,n)}function Vk(t,n){return nS(t,n)}function ep(t,n){return qc(t,n)}function k(t,n,e){let i=ae(),r=Lr();if(zi(i,r,n)){let o=je(),s=fa();WM(s,i,t,n,i[ke],e)}return k}function tp(t,n,e,i,r){Tp(n,t,e,r?"class":"style",i)}function _(t,n,e,i){let r=ae(),o=r[U],s=t+Ge,a=o.firstCreatePass?fS(s,r,2,n,zC,Zh(),e,i):o.data[s];if(li(a)){let l=r[Un].tracingService;if(l&&l.componentCreate){let c=o.data[a.directiveStart+a.componentOffset];return l.componentCreate(lS(c),()=>(Ly(t,n,r,a,i),_))}}return Ly(t,n,r,a,i),_}function Ly(t,n,e,i,r){if($C(i,e,t,n,kS),ca(i)){let o=e[U];Ip(o,e,i),_C(o,i,e)}r!=null&&Vd(e,i)}function b(){let t=je(),n=st(),e=WC(n);return t.firstCreatePass&&hS(t,e),Xh(e)&&Qh(),Yh(),e.classesWithoutHost!=null&&fT(e)&&tp(t,e,ae(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&hT(e)&&tp(t,e,ae(),e.stylesWithoutHost,!1),b}function A(t,n,e,i){return _(t,n,e,i),b(),A}function mt(t,n,e,i){let r=ae(),o=r[U],s=t+Ge,a=o.firstCreatePass?WN(s,o,2,n,e,i):o.data[s];return $C(a,r,t,n,kS),i!=null&&Vd(r,a),mt}function Et(){let t=st(),n=WC(t);return Xh(n)&&Qh(),Yh(),Et}function sn(t,n,e,i){return mt(t,n,e,i),Et(),sn}var kS=(t,n,e,i,r)=>(id(!0),wC(n[ke],i,sm()));function Pt(){return ae()}function qt(t,n,e){let i=ae(),r=Lr();if(zi(i,r,n)){let o=je(),s=fa();HC(s,i,t,n,i[ke],e)}return qt}var Ra="en-US";var Bk=Ra;function AS(t){typeof t=="string"&&(Bk=t.toLowerCase().replace(/_/g,"-"))}function z(t,n,e){let i=ae(),r=je(),o=st();return jk(r,i,i[ke],o,t,n,e),z}function zd(t,n,e){let i=ae(),r=je(),o=st();return(o.type&3||e)&&kp(o,r,i,e,i[ke],t,n,jr(o,i,n)),zd}function jk(t,n,e,i,r,o,s){let a=!0,l=null;if((i.type&3||s)&&(l??=jr(i,n,o),kp(i,t,n,s,e,r,o,l)&&(a=!1)),a){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let h=u[f],m=u[f+1];l??=jr(i,n,o),bd(i,n,h,m,r,l)}if(c&&c.length)for(let f of c)l??=jr(i,n,o),bd(i,n,f,r,r,l)}}function G(t=1){return Yb(t)}function Uk(t,n){let e=null,i=aM(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?NC(t,o,!0):dM(i,o))return r}return e}function Te(t){let n=ae()[At][yt];if(!n.projection){let e=t?t.length:1,i=n.projection=bb(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?Uk(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function le(t,n=0,e,i,r,o){let s=ae(),a=je(),l=i?t+1:null;l!==null&&wa(s,a,l,i,r,o,null,e);let c=Ho(a,Ge+t,16,null,e||null);c.projection===null&&(c.projection=n),tm();let f=!s[Nr]||Kh();s[At][yt].projection[c.projection]===null&&l!==null?Hk(s,a,l):f&&!Md(c)&&PM(a,s,c)}function Hk(t,n,e){let i=Ge+e,r=n.data[i],o=t[i],s=vd(o,r.tView.ssrId),a=Ma(t,r,void 0,{dehydratedView:s});Na(o,a,0,Vo(r,s))}function Dn(t,n,e,i){return yS(t,n,e,i),Dn}function Fe(t,n,e){return bS(t,n,e),Fe}function Q(t){let n=ae(),e=je(),i=Qc();ua(i+1);let r=Rp(e,i);if(t.dirty&&Nb(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=SS(n,i);t.reset(o,oC),t.notifyOnChanges()}return!0}return!1}function J(){return Ap(ae(),Qc())}function $d(t,n,e,i,r){return ES(n,yS(t,e,i,r)),$d}function Wd(t,n,e,i){return ES(t,bS(n,e,i)),Wd}function Gd(t=1){ua(Qc()+t)}function vi(t){let n=Vb();return Mb(n,Ge+t)}function ld(t,n){return t<<17|n<<2}function $r(t){return t>>17&32767}function zk(t){return(t&2)==2}function $k(t,n){return t&131071|n<<17}function np(t){return t|2}function jo(t){return(t&131068)>>2}function bm(t,n){return t&-131069|n<<2}function Wk(t){return(t&1)===1}function ip(t){return t|1}function Gk(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=$r(s),l=jo(s);t[i]=e;let c=!1,u;if(Array.isArray(e)){let f=e;u=f[1],(u===null||xo(f,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let h=$r(t[a+1]);t[i+1]=ld(h,a),h!==0&&(t[h+1]=bm(t[h+1],i)),t[a+1]=$k(t[a+1],i)}else t[i+1]=ld(a,0),a!==0&&(t[a+1]=bm(t[a+1],i)),a=i;else t[i+1]=ld(l,0),a===0?a=i:t[l+1]=bm(t[l+1],i),l=i;c&&(t[i+1]=np(t[i+1])),Vy(t,u,i,!0),Vy(t,u,i,!1),qk(n,u,t,i,o),s=ld(a,l),o?n.classBindings=s:n.styleBindings=s}function qk(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&xo(o,n)>=0&&(e[i+1]=ip(e[i+1]))}function Vy(t,n,e,i){let r=t[e+1],o=n===null,s=i?$r(r):jo(r),a=!1;for(;s!==0&&(a===!1||o);){let l=t[s],c=t[s+1];Yk(l,n)&&(a=!0,t[s+1]=i?ip(c):np(c)),s=i?$r(c):jo(c)}a&&(t[e+1]=i?np(r):ip(r))}function Yk(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?xo(t,n)>=0:!1}var yn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function Zk(t){return t.substring(yn.key,yn.keyEnd)}function Kk(t){return Xk(t),RS(t,OS(t,0,yn.textEnd))}function RS(t,n){let e=yn.textEnd;return e===n?-1:(n=yn.keyEnd=Qk(t,yn.key=n,e),OS(t,n,e))}function Xk(t){yn.key=0,yn.keyEnd=0,yn.value=0,yn.valueEnd=0,yn.textEnd=t.length}function OS(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function Qk(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function an(t,n,e){return PS(t,n,e,!1),an}function ee(t,n){return PS(t,n,null,!0),ee}function pt(t){eA(sA,Jk,t,!0)}function Jk(t,n){for(let e=Kk(n);e>=0;e=RS(n,e))$c(t,Zk(n),!0)}function PS(t,n,e,i){let r=ae(),o=je(),s=im(2);if(o.firstUpdatePass&&LS(o,t,s,i),n!==on&&zi(r,s,n)){let a=o.data[ci()];VS(o,a,r,r[ke],t,r[s+1]=lA(n,e),i,s)}}function eA(t,n,e,i){let r=je(),o=im(2);r.firstUpdatePass&&LS(r,null,o,i);let s=ae();if(e!==on&&zi(s,o,e)){let a=r.data[ci()];if(BS(a,i)&&!FS(r,o)){let l=i?a.classesWithoutHost:a.stylesWithoutHost;l!==null&&(e=Vc(l,e||"")),tp(r,a,s,e,i)}else aA(r,a,s,s[ke],s[o+1],s[o+1]=oA(t,n,e),i,o)}}function FS(t,n){return n>=t.expandoStartIndex}function LS(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[ci()],s=FS(t,e);BS(o,i)&&n===null&&!s&&(n=!1),n=tA(r,o,n,i),Gk(r,o,n,e,s,i)}}function tA(t,n,e,i){let r=$b(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=ym(null,t,n,e,i),e=xa(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=ym(r,t,n,e,i),o===null){let l=nA(t,n,i);l!==void 0&&Array.isArray(l)&&(l=ym(null,t,n,l[1],i),l=xa(l,n.attrs,i),iA(t,n,i,l))}else o=rA(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function nA(t,n,e){let i=e?n.classBindings:n.styleBindings;if(jo(i)!==0)return t[$r(i)]}function iA(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[$r(r)]=i}function rA(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=xa(i,s,e)}return xa(i,n.attrs,e)}function ym(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=xa(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function xa(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),$c(t,s,e?!0:n[++o]))}return t===void 0?null:t}function oA(t,n,e){if(e==null||e==="")return _t;let i=[],r=hi(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function sA(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&$c(t,i,e)}function aA(t,n,e,i,r,o,s,a){r===on&&(r=_t);let l=0,c=0,u=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;u!==null||f!==null;){let h=l<r.length?r[l+1]:void 0,m=c<o.length?o[c+1]:void 0,v=null,S;u===f?(l+=2,c+=2,h!==m&&(v=f,S=m)):f===null||u!==null&&u<f?(l+=2,v=u):(c+=2,v=f,S=m),v!==null&&VS(t,n,e,i,v,S,s,a),u=l<r.length?r[l]:null,f=c<o.length?o[c]:null}}function VS(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let l=t.data,c=l[a+1],u=Wk(c)?By(l,n,e,r,jo(c),s):void 0;if(!Td(u)){Td(o)||zk(c)&&(o=By(l,null,e,r,a,s));let f=zh(ci(),e);LM(i,s,f,r,o)}}function By(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,f=u===null,h=e[r+1];h===on&&(h=f?_t:void 0);let m=f?Wc(h,i):u===i?h:void 0;if(c&&!Td(m)&&(m=Wc(l,i)),Td(m)&&(a=m,s))return a;let v=t[r+1];r=s?$r(v):jo(v)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(a=Wc(l,i))}return a}function Td(t){return t!==void 0}function lA(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=ta(hi(t)))),t}function BS(t,n){return(t.flags&(n?8:16))!==0}function R(t,n=""){let e=ae(),i=je(),r=t+Ge,o=i.firstCreatePass?Ho(i,r,1,n,null):i.data[r],s=cA(i,e,o,n);e[r]=s,nd()&&Sp(i,e,s,o),ko(o,!1)}var cA=(t,n,e,i)=>(id(!0),XT(n[ke],i));function dA(t,n,e,i=""){return zi(t,Lr(),e)?n+Nh(e)+i:on}function wt(t){return Yt("",t),wt}function Yt(t,n,e){let i=ae(),r=dA(i,t,n,e);return r!==on&&uA(i,ci(),r),Yt}function uA(t,n,e){let i=zh(n,t);QT(t[ke],i,e)}function jy(t,n,e){let i=je();i.firstCreatePass&&jS(n,i.data,i.blueprint,vn(t),e)}function jS(t,n,e,i,r){if(t=et(t),Array.isArray(t))for(let o=0;o<t.length;o++)jS(t[o],n,e,i,r);else{let o=je(),s=ae(),a=st(),l=xr(t)?t:et(t.provide),c=Lh(t),u=a.providerIndexes&1048575,f=a.directiveStart,h=a.providerIndexes>>20;if(xr(t)||!t.multi){let m=new Ur(c,r,lt,null),v=Sm(l,n,r?u:u+h,f);v===-1?(wm(gd(a,s),o,l),Cm(o,t,n.length),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(m),s.push(m)):(e[v]=m,s[v]=m)}else{let m=Sm(l,n,u+h,f),v=Sm(l,n,u,u+h),S=m>=0&&e[m],M=v>=0&&e[v];if(r&&!M||!r&&!S){wm(gd(a,s),o,l);let O=mA(r?hA:fA,e.length,r,i,c,t);!r&&M&&(e[v].providerFactory=O),Cm(o,t,n.length,0),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(O),s.push(O)}else{let O=US(e[r?v:m],c,!r&&i);Cm(o,t,m>-1?m:v,O)}!r&&i&&M&&e[v].componentProviders++}}}function Cm(t,n,e,i){let r=xr(n),o=wb(n);if(r||o){let l=(o?et(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function US(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Sm(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function fA(t,n,e,i,r){return rp(this.multi,[])}function hA(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,l=ya(i,i[U],this.providerFactory.index,r);s=l.slice(0,a),rp(o,s);for(let c=a;c<l.length;c++)s.push(l[c])}else s=[],rp(o,s);return s}function rp(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function mA(t,n,e,i,r,o){let s=new Ur(t,e,lt,null);return s.multi=[],s.index=n,s.componentProviders=0,US(s,r,i&&!e),s}function Re(t,n){return e=>{e.providersResolver=(i,r)=>jy(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>jy(i,r?r(n):n,!0))}}function qr(t,n){let e=Bb()+t,i=ae();return i[e]===on?xN(i,e,n()):IN(i,e)}function Up(t,n){return Bd(t,n)}var HS=(()=>{class t{applicationErrorHandler=d(rn);appRef=d(Gt);taskService=d(di);ngZone=d(T);zonelessEnabled=d(pa);tracing=d(Yn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ge;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Js):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(fm,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Qb:lm;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Js+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function zS(){return[{provide:Ln,useExisting:HS},{provide:T,useClass:ea},{provide:pa,useValue:!0}]}var Hp=(()=>{class t{compileModuleSync(e){return new wd(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function pA(){return typeof $localize<"u"&&$localize.locale||Ra}var qd=new g("",{factory:()=>d(qd,{optional:!0,skipSelf:!0})||pA()});var Yd=class{destroyed=!1;listeners=null;errorHandler=d(bt,{optional:!0});isEmitting=!1;hasNullListeners=!1;destroyRef=d(He);constructor(){this.destroyRef.onDestroy(()=>{this.destroyed=!0,this.listeners=null})}subscribe(n){if(this.destroyed)throw new C(953,!1);return(this.listeners??=[]).push(n),{unsubscribe:()=>{let e=this.listeners?this.listeners.indexOf(n):-1;e>-1&&(this.isEmitting?(this.hasNullListeners=!0,this.listeners[e]=null):this.listeners.splice(e,1))}}}emit(n){if(this.destroyed){console.warn(Vn(953,!1));return}if(this.listeners===null)return;this.isEmitting=!0;let e=B(null);try{for(let i of this.listeners)try{i!==null&&i(n)}catch(r){this.errorHandler?.handleError(r)}}finally{this.hasNullListeners&&(this.hasNullListeners=!1,this.listeners&&gA(this.listeners)),B(e),this.isEmitting=!1}}};function gA(t){let n=t.length-1;for(;n>-1;)t[n]===null&&t.splice(n,1),n--}function w(t,n){return Ls(t,n?.equal)}function $(t){return A_(t)}var $S=class t extends Error{_brand;constructor(n){super(n)}static IDLE=new t("IDLE");static LOADING=new t("LOADING")},vA=t=>t;function _i(t,n){if(typeof t=="function"){let e=Jf(t,vA,n?.equal);return WS(e,n?.debugName,n?.set)}else{let e=Jf(t.source,t.computation,t.equal);return WS(e,t.debugName,t.set)}}function WS(t,n,e){let i=t[Ne],r=t;if(e!==void 0){let o=s=>eh(i,s);r.set=s=>e(s,o),r.update=s=>e(s($(t)),o)}else r.set=o=>eh(i,o),r.update=o=>k_(i,o);return r.asReadonly=ha.bind(t),r}var GS=!1;function zp(){return GS}function Zd(t){GS=t}var Xd=Symbol("InputSignalNode#UNSET"),eD=P(p({},Vs),{transformFn:void 0,applyValueToInputSignal(t,n){ki(t,n)}});function tD(t,n){let e=Object.create(eD);e.value=t,e.transformFn=n?.transform;function i(){if(Jn(e),e.value===Xd){let r=null;throw new C(-950,r)}return e.value}return i[Ne]=e,i}var bi=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>sp(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function qp(t){return LA(t)?t.default:t}function LA(t){return t&&typeof t=="object"&&"default"in t}function qS(t,n){return tD(t,n)}function VA(t){return tD(Xd,t)}var it=(qS.required=VA,qS);function nD(t,n){let e=Object.create(eD),i=new Yd;e.value=t;function r(){return Jn(e),YS(e.value),e.value}return r[Ne]=e,r.asReadonly=ha.bind(r),r.set=o=>{e.equal(e.value,o)||(ki(e,o),i.emit(o))},r.update=o=>{YS(e.value),r.set(o(e.value))},r.subscribe=i.subscribe.bind(i),r.destroyRef=i.destroyRef,r}function YS(t){if(t===Xd)throw new C(952,!1)}function ZS(t,n){return nD(t,n)}function BA(t){return nD(Xd,t)}var Qd=(ZS.required=BA,ZS);function KS(t,n){return Op(n)}function jA(t,n){return Pp(n)}var Pa=(KS.required=jA,KS);function XS(t,n){return Op(n)}function UA(t,n){return Pp(n)}var iD=(XS.required=UA,XS);var HA=1e4;var j6=HA-1e3;var Le=(()=>{class t{static __NG_ELEMENT_ID__=zA}return t})();function zA(t){return $A(st(),ae(),(t&16)===16)}function $A(t,n,e){if(li(t)&&!e){let i=tn(t.index,n);return new Hi(i,i)}else if(t.type&175){let i=n[At];return new Hi(i,n)}return null}var Wp=new g(""),WA=new g("");function Oa(t){return!t.moduleRef}function GA(t){let n=Oa(t)?t.r3Injector:t.moduleRef.injector,e=n.get(T);return e.run(()=>{Oa(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(rn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Oa(t)){let o=()=>n.destroy(),s=t.platformInjector.get(Wp);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(Wp);s.add(o),t.moduleRef.onDestroy(()=>{ba(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return YA(i,e,()=>{let o=n.get(di),s=o.add(),a=n.get(Lp);return a.runInitializers(),a.donePromise.then(()=>{let l=n.get(qd,Ra);if(AS(l||Ra),!n.get(WA,!0))return Oa(t)?n.get(Gt):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Oa(t)){let u=n.get(Gt);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return qA?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var qA;function YA(t,n,e){try{let i=e();return gi(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Kd=null;function ZA(t=[],n){return V.create({name:n,providers:[{provide:aa,useValue:"platform"},{provide:Wp,useValue:new Set([()=>Kd=null])},...t]})}function KA(t=[]){if(Kd)return Kd;let n=ZA(t);return Kd=n,NS(),XA(n),n}function XA(t){let n=t.get(rd,null);Ve(t,()=>{n?.forEach(e=>e())})}function rD(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;De(ve.BootstrapApplicationStart);try{let o=r?.injector??KA(i),s=[zS(),ey,...e||[]],a=new Ea({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return GA({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{De(ve.BootstrapApplicationEnd)}}function ne(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function xt(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var $p=Symbol("NOT_SET"),oD=new Set,QA=P(p({},Vs),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:$p,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==$p&&!pr(this))return this.signal;try{for(let r of this.cleanup??oD)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=On(this),i;try{i=this.userFn.apply(null,n)}finally{ei(this,e)}return(this.value===$p||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Gp=class extends Ca{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(He),s),this.scheduler=r;for(let a of _p){let l=e[a];if(l===void 0)continue;let c=Object.create(QA);c.sequence=this,c.phase=a,c.userFn=l,c.dirty=!0,c.signal=()=>(Jn(c),c.value),c.signal[Ne]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[a]=c,this.hooks[a]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??oD)e()}finally{ti(n)}}};function $o(t,n){let e=n?.injector??d(V),i=e.get(Ln),r=e.get(Rd),o=e.get(Yn,null,{optional:!0});r.impl??=e.get(bp);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(Ro,null,{optional:!0}),l=new Gp(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function Jd(t,n){let e=Pi(t),i=n.elementInjector||Io();return new Bo(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var sD=null;function cn(){return sD}function Yp(t){sD??=t}var Fa=class{},Wo=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:()=>d(aD),providedIn:"platform"})}return t})();var aD=(()=>{class t extends Wo{_location;_history;_doc=d(L);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return cn().getBaseHref(this._doc)}onPopState(e){let i=cn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=cn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function dD(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function lD(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function $i(t){return t&&t[0]!=="?"?`?${t}`:t}var eu=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:()=>d(eR),providedIn:"root"})}return t})(),JA=new g(""),eR=(()=>{class t extends eu{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(L).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return dD(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+$i(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+$i(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+$i(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(j(Wo),j(JA,8))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Wi=(()=>{class t{_subject=new D;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=iR(lD(cD(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+$i(i))}normalize(e){return t.stripTrailingSlash(nR(this._basePath,cD(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+$i(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+$i(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=$i;static joinWithSlash=dD;static stripTrailingSlash=lD;static \u0275fac=function(i){return new(i||t)(j(eu))};static \u0275prov=se({token:t,factory:()=>tR(),providedIn:"root"})}return t})();function tR(){return new Wi(j(eu))}function nR(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function cD(t){return t.replace(/\/index\.html$/,"")}function iR(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Zp=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(V);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(lt(Rt))};static \u0275dir=N({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[qe]})}return t})();function La(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var Kp="browser";function uD(t){return t===Kp}var Va=class{_doc;constructor(n){this._doc=n}manager},tu=(()=>{class t extends Va{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(j(L))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),ru=new g(""),eg=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof tu));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof tu);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new C(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(j(ru),j(T))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),Xp="ng-app-id";function fD(t){for(let n of t)n.remove()}function hD(t,n){let e=n.createElement("style");return e.textContent=t,e}function lR(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Xp}="${n}"],link[${Xp}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(Xp),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Jp(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var tg=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,lR(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,hD);i?.forEach(r=>this.addUsage(r,this.external,Jp))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(fD(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])fD(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,hD(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Jp(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(j(L),j(_n),j(ui,8),j(Vr))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),Qp={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},ng=/%COMP%/g;var pD="%COMP%",cR=`_nghost-${pD}`,dR=`_ngcontent-${pD}`,uR=!0,fR=new g("",{factory:()=>uR}),hR=new g("");function mR(t){return dR.replace(ng,t)}function pR(t){return cR.replace(ng,t)}function gD(t,n){return n.map(e=>e.replace(ng,t))}var ig=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(e,i,r,o,s,a,l=null,c=null,u=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=l,this.tracingService=c,this.cssVarNamespace=u??"",this.defaultRenderer=new Ba(e,s,a,this.tracingService,this.cssVarNamespace)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof iu?r.applyToHost(e):r instanceof ja&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case Cn.Emulated:o=new iu(l,c,i,this.appId,u,s,a,f,this.cssVarNamespace);break;case Cn.ShadowDom:return new nu(l,e,i,s,a,this.nonce,f,this.cssVarNamespace,c);case Cn.ExperimentalIsolatedShadowDom:return new nu(l,e,i,s,a,this.nonce,f,this.cssVarNamespace);default:o=new ja(l,c,i,u,s,a,f,this.cssVarNamespace);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(j(eg),j(Gr),j(_n),j(fR),j(L),j(T),j(ui),j(Yn,8),j(hR,8))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),Ba=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r,o=""){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r,this.cssVarNamespace=o}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Qp[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(mD(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(mD(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new C(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Qp[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Qp[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){let o=e.startsWith("--");o&&(e=e.replace("%NS%",this.cssVarNamespace)),o||r&(Gn.DashCase|Gn.Important)?n.style.setProperty(e,i,r&Gn.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){let r=e.startsWith("--");r&&(e=e.replace("%NS%",this.cssVarNamespace)),r||i&Gn.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=cn().getGlobalEventTarget(this.doc,n),!n))throw new C(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function mD(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var nu=class extends Ba{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,l,c){super(n,r,o,a,l),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let u=i.styles;u=gD(i.id,u).map(h=>h.replace(/%NS%/g,l));for(let h of u){let m=document.createElement("style");s&&m.setAttribute("nonce",s),m.textContent=h,this.shadowRoot.appendChild(m)}let f=i.getExternalStyles?.();if(f)for(let h of f){let m=Jp(h,r);s&&m.setAttribute("nonce",s),this.shadowRoot.appendChild(m)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ja=class extends Ba{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,l,c){super(n,o,s,a,l),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let u=i.styles,f=c?gD(c,u):u;this.styles=f.map(h=>h.replace(/%NS%/g,l)),this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Ui.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},iu=class extends ja{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,l,c){let u=r+"-"+i.id;super(n,e,i,o,s,a,l,c,u),this.contentAttr=mR(u),this.hostAttr=pR(u)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var ou=class t extends Fa{supportsDOMEvents=!0;static makeCurrent(){Yp(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=gR();return e==null?null:vR(e)}resetBaseElement(){Ua=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return La(document.cookie,n)}},Ua=null;function gR(){return Ua=Ua||document.head.querySelector("base"),Ua?Ua.getAttribute("href"):null}function vR(t){return new URL(t,document.baseURI).pathname}var vD=["alt","control","meta","shift"],_R={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},bR={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},_D=(()=>{class t extends Va{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>cn().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),vD.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),s+=c+".")}),s+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=s,l}static matchEventFullKeyCode(e,i){let r=_R[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),vD.forEach(s=>{if(s!==r){let a=bR[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(j(L))};static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})();async function rg(t,n,e){let i=p({rootComponent:t},yR(n,e));return rD(i)}function yR(t,n){return{platformRef:n?.platformRef,appProviders:[...wR,...t?.providers??[]],platformProviders:ER}}function CR(){ou.makeCurrent()}function SR(){return new bt}function DR(){return lp(document),document}var ER=[{provide:Vr,useValue:Kp},{provide:rd,useValue:CR,multi:!0},{provide:L,useFactory:DR}];var wR=[{provide:aa,useValue:"root"},{provide:bt,useFactory:SR},{provide:ru,useClass:tu,multi:!0},{provide:ru,useClass:_D,multi:!0},ig,{provide:Gr,useClass:tg},{provide:tg,useExisting:Gr},eg,{provide:Ze,useExisting:ig},[]];var Ci=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init();for(let[e,i]of n.headers.entries())this.headers.set(e,i),this.normalizedNames.set(e,n.normalizedNames.get(e))}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=n.op==="a"?(this.headers.get(e)||[]).slice():[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(o===void 0)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=Array.isArray(o)?o:[o],a=this.headers.get(e);if(!a)return;a=a.filter(l=>s.indexOf(l)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var ag=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},lg=class{encodeKey(n){return bD(n)}encodeValue(n){return bD(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function xR(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(s)||[];l.push(a),e.set(s,l)}),e}var IR=/%(\d[a-f0-9])/gi,TR={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function bD(t){return encodeURIComponent(t).replace(IR,(n,e)=>TR[e]??n)}function su(t){return`${t}`}var yi=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new lg,n.fromString){if(n.fromObject)throw new C(2805,!1);this.map=xR(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(su):[su(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){if(this.map===null&&(this.map=new Map),this.cloneFrom!==null){this.cloneFrom.init();for(let[n,e]of this.cloneFrom.map.entries())this.map.set(n,e);this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=n.op==="a"?(this.map.get(n.param)||[]).slice():[];e.push(su(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=(this.map.get(n.param)||[]).slice(),r=i.indexOf(su(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null}}};function MR(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function yD(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function CD(t){return typeof Blob<"u"&&t instanceof Blob}function SD(t){return typeof FormData<"u"&&t instanceof FormData}function NR(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var og="Content-Type",DD="Accept",wD="text/plain",xD="application/json",kR=`${xD}, ${wD}, */*`,Go=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(MR(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new C(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Ci,this.context??=new ag,!this.params)this.params=new yi,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,l="",c=e.indexOf("#");c!==-1&&(l=e.substring(c),a=e.substring(0,c));let u=a.indexOf("?"),f=u===-1?"?":u<a.length-1?"&":"";this.urlWithParams=a+f+s+l}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||yD(this.body)||CD(this.body)||SD(this.body)||NR(this.body)?this.body:this.body instanceof yi?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||SD(this.body)?null:CD(this.body)?this.body.type||null:yD(this.body)?null:typeof this.body=="string"?wD:this.body instanceof yi?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?xD:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,f=n.referrer??this.referrer,h=n.integrity||this.integrity,m=n.referrerPolicy||this.referrerPolicy,v=n.transferCache??this.transferCache,S=n.timeout??this.timeout,M=n.body!==void 0?n.body:this.body,O=n.withCredentials??this.withCredentials,pe=n.reportProgress??this.reportProgress,ye=n.reportUploadProgress??this.reportUploadProgress,Nn=n.reportDownloadProgress??this.reportDownloadProgress,Ni=n.headers||this.headers,kn=n.params||this.params,cr=n.context??this.context;return n.setHeaders!==void 0&&(Ni=Object.keys(n.setHeaders).reduce((An,dr)=>An.set(dr,n.setHeaders[dr]),Ni)),n.setParams&&(kn=Object.keys(n.setParams).reduce((An,dr)=>An.set(dr,n.setParams[dr]),kn)),new t(e,i,M,{params:kn,headers:Ni,context:cr,reportProgress:pe,reportUploadProgress:ye,reportDownloadProgress:Nn,responseType:r,withCredentials:O,transferCache:v,keepalive:o,cache:a,priority:s,timeout:S,mode:l,redirect:c,credentials:u,referrer:f,integrity:h,referrerPolicy:m})}},qo=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(qo||{}),Ha=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new Ci,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},cg=class t extends Ha{constructor(n={}){super(n)}type=qo.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},za=class t extends Ha{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=qo.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Yr=class extends Ha{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},AR=200;var RR=/^\)\]\}',?\n/,sZ=1024*1024,OR=new g("",{factory:()=>null}),PR=(()=>{class t{fetchImpl=d(dg,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=d(T);destroyRef=d(He);maxResponseSize=d(OR);handle(e){return new ie(i=>{let r=new AbortController;this.doRequest(e,r.signal,i).then(ug,s=>i.error(new Yr({error:s})));let o;return e.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}async doRequest(e,i,r){let o=this.createRequestInit(e),s;try{let M=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,p({signal:i},o)));FR(M),r.next({type:qo.Sent}),s=await M}catch(M){r.error(new Yr({error:M,status:M.status??0,statusText:M.statusText,url:e.urlWithParams,headers:M.headers}));return}let a=new Ci(s.headers),l=s.statusText,c=s.url||e.urlWithParams,u=s.status,f=null,h=e.reportProgress||e.reportDownloadProgress;if(h&&r.next(new cg({headers:a,status:u,statusText:l,url:c})),s.body){let M=s.headers.get("content-length"),O=M!==null?Number(M):NaN;this.maxResponseSize!==null&&Number.isFinite(O)&&O>this.maxResponseSize&&ED(this.maxResponseSize);let pe=[],ye=s.body.getReader(),Nn=0,Ni,kn,cr=typeof Zone<"u"&&Zone.current,An=!1;if(await this.ngZone.runOutsideAngular(async()=>{for(;;){if(this.destroyRef.destroyed){await ye.cancel(),An=!0;break}let{done:Os,value:$f}=await ye.read();if(Os)break;if(pe.push($f),Nn+=$f.length,this.maxResponseSize!==null&&Nn>this.maxResponseSize&&(await ye.cancel(),ED(this.maxResponseSize)),h){kn=e.responseType==="text"?(kn??"")+(Ni??=new TextDecoder).decode($f,{stream:!0}):void 0;let b_=()=>r.next({type:qo.DownloadProgress,total:Number.isFinite(O)?O:void 0,loaded:Nn,partialText:kn});cr?cr.run(b_):b_()}}}),An){r.complete();return}let dr=this.concatChunks(pe,Nn);try{let Os=s.headers.get(og)??"";f=this.parseBody(e,dr,Os,u)}catch(Os){r.error(new Yr({error:Os,headers:new Ci(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}u===0&&(u=f?AR:0);let m=u>=200&&u<300,v=s.redirected,S=s.type;m?(r.next(new za({body:f,headers:a,status:u,statusText:l,url:c,redirected:v,responseType:S})),r.complete()):r.error(new Yr({error:f,headers:a,status:u,statusText:l,url:c,redirected:v,responseType:S}))}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(RR,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new C(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(DD)||(i[DD]=kR),!e.headers.has(og)){let o=e.detectContentTypeHeader();o!==null&&(i[og]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),dg=class{};function ug(){}function FR(t){t.then(ug,ug)}function ED(t){throw new C(-2825,!1)}var LR=new g("",{factory:()=>!0}),VR="XSRF-TOKEN",BR=new g("",{factory:()=>VR}),jR="X-XSRF-TOKEN",UR=new g("",{factory:()=>jR}),HR=(()=>{class t{cookieName=d(BR);doc=d(L);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=La(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),zR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=j(HR),r},providedIn:"root"})}return t})();function $R(t,n){if(!d(LR)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(Wo).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch{return n(t)}let e=d(zR).getToken(),i=d(UR);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}function WR(t,n){return n(t)}function GR(t,n,e){return(i,r)=>Ve(e,()=>n(i,o=>t(o,r)))}var qR=new g("",{factory:()=>[$R]}),ID=new g(""),YR=new g("",{factory:()=>!0});var ZR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=j(PR),r},providedIn:"root"})}return t})();var KR=(()=>{class t{backend;injector;chain=null;pendingTasks=d(ga);contributeToStability=d(YR);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let r=Array.from(new Set([...this.injector.get(qR),...this.injector.get(ID,[])]));this.chain=r.reduceRight((o,s)=>GR(o,s,this.injector),WR)}let i=this.chain;if(this.contributeToStability){let r=this.pendingTasks.add();return $(()=>i(e,o=>this.backend.handle(o))).pipe(Sr(r))}else return $(()=>i(e,r=>this.backend.handle(r)))}static \u0275fac=function(i){return new(i||t)(j(ZR),j(xe))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),XR=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=j(KR),r},providedIn:"root"})}return t})();function sg(t,n){return p({body:n},t)}var $a=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof Go)o=e;else{let l;r.headers instanceof Ci?l=r.headers:l=new Ci(r.headers);let c;r.params&&(r.params instanceof yi?c=r.params:c=new yi({fromObject:r.params})),o=new Go(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=q(o).pipe(So(l=>this.handler.handle(l)));if(e instanceof Go||r.observe==="events")return s;let a=s.pipe(be(l=>l instanceof za));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(re(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new C(2806,!1);return l.body}));case"blob":return a.pipe(re(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new C(2807,!1);return l.body}));case"text":return a.pipe(re(l=>{if(l.body!==null&&typeof l.body!="string")throw new C(2808,!1);return l.body}));default:return a.pipe(re(l=>l.body))}case"response":return a;default:throw new C(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new yi().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,sg(r,i))}post(e,i,r={}){return this.request("POST",e,sg(r,i))}put(e,i,r={}){return this.request("PUT",e,sg(r,i))}static \u0275fac=function(i){return new(i||t)(j(XR))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var TD=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(j(L))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Wa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=j(JR),r},providedIn:"root"})}return t})(),JR=(()=>{class t extends Wa{_doc=d(L);sanitize(e,i){if(i==null)return null;switch(e){case ot.NONE:return i;case ot.HTML:return Wr(i,"HTML")?hi(i):mp(this._doc,String(i)).toString();case ot.STYLE:return Wr(i,"Style")?hi(i):i;case ot.SCRIPT:if(Wr(i,"Script"))return hi(i);throw new C(5200,!1);case ot.URL:return Wr(i,"URL")?hi(i):kd(String(i));case ot.RESOURCE_URL:if(Wr(i,"ResourceURL"))return hi(i);throw new C(-5201,!1);default:throw new C(5202,!1)}}bypassSecurityTrustHtml(e){return cp(e)}bypassSecurityTrustStyle(e){return dp(e)}bypassSecurityTrustScript(e){return up(e)}bypassSecurityTrustUrl(e){return fp(e)}bypassSecurityTrustResourceUrl(e){return hp(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var de="primary",il=Symbol("RouteTitle"),gg=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function Xo(t){return new gg(t)}function fg(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function eO(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return fg(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!fg(o,t.slice(0,o.length),a)||!fg(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function fu(t){return new Promise((n,e)=>{t.pipe(ri()).subscribe({next:i=>n(i),error:i=>e(i)})})}function tO(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Zn(t[e],n[e]))return!1;return!0}function Zn(t,n){let e=t?vg(t):void 0,i=n?vg(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!VD(t[r],n[r]))return!1;return!0}function vg(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function VD(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function nO(t){return t.length>0?t[t.length-1]:null}function Jr(t){return $s(t)?t:gi(t)?Pe(Promise.resolve(t)):q(t)}function BD(t){return $s(t)?fu(t):Promise.resolve(t)}var iO={exact:HD,subset:zD},jD={exact:rO,subset:oO,ignored:()=>!0},UD={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},_g={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function ND(t,n,e){return iO[e.paths](t.root,n.root,e.matrixParams)&&jD[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function rO(t,n){return Zn(t,n)}function HD(t,n,e){if(!Kr(t.segments,n.segments)||!cu(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!HD(t.children[i],n.children[i],e))return!1;return!0}function oO(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>VD(t[e],n[e]))}function zD(t,n,e){return $D(t,n,n.segments,e)}function $D(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Kr(r,e)||n.hasChildren()||!cu(r,e,i))}else if(t.segments.length===e.length){if(!Kr(t.segments,e)||!cu(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!zD(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Kr(t.segments,r)||!cu(t.segments,r,i)||!t.children[de]?!1:$D(t.children[de],n,o,i)}}function cu(t,n,e){return n.every((i,r)=>jD[e](t[r].parameters,i.parameters))}var xn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Ee([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Xo(this.queryParams),this._queryParamMap}toString(){return lO.serialize(this)}},Ee=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return du(this)}},Zr=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=Xo(this.parameters),this._parameterMap}toString(){return GD(this)}};function sO(t,n){return Kr(t,n)&&t.every((e,i)=>Zn(e.parameters,n[i].parameters))}function Kr(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function aO(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===de&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==de&&(e=e.concat(n(r,i)))}),e}var Su=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:()=>new Xr})}return t})(),Xr=class{parse(n){let e=new yg(n);return new xn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Ga(n.root,!0)}`,i=uO(n.queryParams),r=typeof n.fragment=="string"?`#${cO(n.fragment)}`:"";return`${e}${i}${r}`}},lO=new Xr;function du(t){return t.segments.map(n=>GD(n)).join("/")}function Ga(t,n){if(!t.hasChildren())return du(t);if(n){let e=t.children[de]?Ga(t.children[de],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==de&&i.push(`${r}:${Ga(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=aO(t,(i,r)=>r===de?[Ga(t.children[de],!1)]:[`${r}:${Ga(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[de]!=null?`${du(t)}/${e[0]}`:`${du(t)}/(${e.join("//")})`}}function WD(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function au(t){return WD(t).replace(/%3B/gi,";")}function cO(t){return encodeURI(t)}function bg(t){return WD(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function uu(t){return decodeURIComponent(t)}function kD(t){return uu(t.replace(/\+/g,"%20"))}function GD(t){return`${bg(t.path)}${dO(t.parameters)}`}function dO(t){return Object.entries(t).map(([n,e])=>`;${bg(n)}=${bg(e)}`).join("")}function uO(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${au(e)}=${au(r)}`).join("&"):`${au(e)}=${au(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var fO=/^[^\/()?;#]+/;function hg(t){let n=t.match(fO);return n?n[0]:""}var hO=/^[^\/()?;=#]+/;function mO(t){let n=t.match(hO);return n?n[0]:""}var pO=/^[^=?&#]+/;function gO(t){let n=t.match(pO);return n?n[0]:""}var vO=/^[^&#]+/;function _O(t){let n=t.match(vO);return n?n[0]:""}var yg=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Ee([],{}):new Ee([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new C(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[de]=new Ee(e,i)),r}parseSegment(){let n=hg(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new C(4009,!1);return this.capture(n),new Zr(uu(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=mO(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=hg(this.remaining);r&&(i=r,this.capture(i))}n[uu(e)]=uu(i)}parseQueryParam(n){let e=gO(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=_O(this.remaining);s&&(i=s,this.capture(i))}let r=kD(e),o=kD(i);if(Object.hasOwn(n,r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i=Object.create(null);for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=hg(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new C(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=de);let a=this.parseChildren(e+1);i[s??de]=Object.keys(a).length===1&&a[de]?a[de]:new Ee([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new C(4011,!1)}};function qD(t){return t.segments.length>0?new Ee([],{[de]:t}):t}function YD(t){let n=Object.create(null);for(let[i,r]of Object.entries(t.children)){let o=YD(r);if(i===de&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Ee(t.segments,n);return bO(e)}function bO(t){if(t.numberOfChildren===1&&t.children[de]){let n=t.children[de];return new Ee(t.segments.concat(n.segments),n.children)}return t}function Qo(t){return t instanceof xn}function yO(t,n,e=null,i=null,r=new Xr){let o=ZD(t);return KD(o,n,e,i,r)}function ZD(t){let n;function e(o){let s={};for(let l of o.children){let c=e(l);s[l.outlet]=c}let a=new Ee(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=qD(i);return n??r}function KD(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return mg(o,o,o,e,i,r);let s=CO(n);if(s.toRoot())return mg(o,o,new Ee([],{}),e,i,r);let a=SO(s,o,t),l=a.processChildren?Ya(a.segmentGroup,a.index,s.commands):QD(a.segmentGroup,a.index,s.commands);return mg(o,a.segmentGroup,l,e,i,r)}function hu(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Xa(t){return typeof t=="object"&&t!=null&&t.outlets}function AD(t,n,e){t||="\u0275";let i=new xn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function mg(t,n,e,i,r,o){let s={};for(let[c,u]of Object.entries(i??{}))s[c]=Array.isArray(u)?u.map(f=>AD(c,f,o)):AD(c,u,o);let a;t===n?a=e:a=XD(t,n,e);let l=qD(YD(a));return new xn(l,s,r)}function XD(t,n,e){let i=Object.create(null);return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=XD(o,n,e)}),new Ee(t.segments,i)}var mu=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&hu(i[0]))throw new C(4003,!1);let r=i.find(Xa);if(r&&r!==nO(i))throw new C(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function CO(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new mu(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([l,c])=>{a[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,l)=>{l==0&&a==="."||(l==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new mu(e,n,i)}var Zo=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function SO(t,n,e){if(t.isAbsolute)return new Zo(n,!0,0);if(!e)return new Zo(n,!1,NaN);if(e.parent===null)return new Zo(e,!0,0);let i=hu(t.commands[0])?0:1,r=e.segments.length-1+i;return DO(e,r,t.numberOfDoubleDots)}function DO(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new C(4005,!1);r=i.segments.length}return new Zo(i,!1,r-o)}function EO(t){return Xa(t[0])?t[0].outlets:{[de]:t}}function QD(t,n,e){if(t??=new Ee([],{}),t.segments.length===0&&t.hasChildren())return Ya(t,n,e);let i=wO(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Ee(t.segments.slice(0,i.pathIndex),{});return o.children[de]=new Ee(t.segments.slice(i.pathIndex),t.children),Ya(o,0,r)}else return i.match&&r.length===0?new Ee(t.segments,{}):i.match&&!t.hasChildren()?Cg(t,n,e):i.match?Ya(t,0,r):Cg(t,n,e)}function Ya(t,n,e){if(e.length===0)return new Ee(t.segments,{});{let i=EO(e),r=Object.create(null);if(Object.keys(i).some(o=>o!==de)&&t.children[de]&&t.numberOfChildren===1&&t.children[de].segments.length===0){let o=Ya(t.children[de],n,e);return new Ee(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=QD(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new Ee(t.segments,r)}}function wO(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(Xa(a))break;let l=`${a}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!OD(l,c,s))return o;i+=2}else{if(!OD(l,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Cg(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(Xa(o)){let l=xO(o.outlets);return new Ee(i,l)}if(r===0&&hu(e[0])){let l=t.segments[n];i.push(new Zr(l.path,RD(e[0]))),r++;continue}let s=Xa(o)?o.outlets[de]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&hu(a)?(i.push(new Zr(s,RD(a))),r+=2):(i.push(new Zr(s,{})),r++)}return new Ee(i,{})}function xO(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=Cg(new Ee([],{}),0,i))}),n}function RD(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function OD(t,n,e){return t==e.path&&Zn(n,e.parameters)}var Za="imperative",gt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(gt||{}),dn=class{id;url;constructor(n,e){this.id=n,this.url=e}},Jo=class extends dn{type=gt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Gi=class extends dn{urlAfterRedirects;type=gt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Ft=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(Ft||{}),pu=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(pu||{}),En=class extends dn{reason;code;type=gt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function JD(t){return t instanceof En&&(t.code===Ft.Redirect||t.code===Ft.SupersededByNewNavigation)}var qi=class extends dn{reason;code;type=gt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},es=class extends dn{error;target;type=gt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},gu=class extends dn{urlAfterRedirects;state;type=gt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Sg=class extends dn{urlAfterRedirects;state;type=gt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Dg=class extends dn{urlAfterRedirects;state;shouldActivate;type=gt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Eg=class extends dn{urlAfterRedirects;state;type=gt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},wg=class extends dn{urlAfterRedirects;state;type=gt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},xg=class{route;type=gt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Ig=class{route;type=gt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Tg=class{snapshot;type=gt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Mg=class{snapshot;type=gt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Ng=class{snapshot;type=gt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},kg=class{snapshot;type=gt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var ts=class{},Qa=class{},ns=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function IO(t){return!(t instanceof ts)&&!(t instanceof ns)&&!(t instanceof Qa)}var Ag=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new rl(this.rootInjector)}},rl=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Ag(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(j(xe))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),vu=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=Rg(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=Rg(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=Og(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Og(n,this._root).map(e=>e.value)}};function Rg(t,n){if(t===n.value)return n;for(let e of n.children){let i=Rg(t,e);if(i)return i}return null}function Og(t,n){if(t===n.value)return[n];for(let e of n.children){let i=Og(t,e);if(i.length)return i.unshift(n),i}return[]}var Zt=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function Yo(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var _u=class extends vu{snapshot;constructor(n,e){super(n),this.snapshot=e,$g(this,n)}toString(){return this.snapshot.toString()}};function e0(t,n){let e=TO(t,n),i=new Je([new Zr("",{})]),r=new Je({}),o=new Je({}),s=new Je({}),a=new Je(""),l=new Qr(i,r,s,a,o,de,t,e.root);return l.snapshot=e.root,new _u(new Zt(l,[]),e)}function TO(t,n){let e={},i={},r={},s=new Ja([],e,r,"",i,de,t,null,{},n);return new bu("",new Zt(s,[]))}var Qr=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;_localInjector;constructor(n,e,i,r,o,s,a,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(re(c=>c[il]))??q(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(re(n=>Xo(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(re(n=>Xo(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}},MO="always";function zg(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:p(p({},n.params),t.params),data:p(p({},n.data),t.data),resolve:p(p(p(p({},t.data),n.data),r?.data),t._resolvedData)}:i={params:p({},t.params),data:p({},t.data),resolve:p(p({},t.data),t._resolvedData??{})},r&&n0(r)&&(i.resolve[il]=r.title),i}var Ja=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[il]}constructor(n,e,i,r,o,s,a,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Xo(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Xo(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},bu=class extends vu{url;constructor(n,e){super(e),this.url=n,$g(this,e)}toString(){return t0(this._root)}};function $g(t,n){n.value._routerState=t,n.children.forEach(e=>$g(t,e))}function t0(t){let n=t.children.length>0?` { ${t.children.map(t0).join(", ")} } `:"";return`${t.value}${n}`}function pg(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Zn(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Zn(n.params,e.params)||t.paramsSubject.next(e.params),tO(n.url,e.url)||t.urlSubject.next(e.url),Zn(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Pg(t,n){let e=Zn(t.params,n.params)&&sO(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||Pg(t.parent,n.parent))}function n0(t){return typeof t.title=="string"||t.title===null}var NO=new g(""),i0=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=de;activateEvents=new W;deactivateEvents=new W;attachEvents=new W;detachEvents=new W;routerOutletData=it();parentContexts=d(rl);location=d(Rt);changeDetector=d(Le);inputBinder=d(Du,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new C(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new C(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new C(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new C(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,l=new Fg(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[qe]})}return t})(),Fg=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Qr?this.route:n===rl?this.childContexts:n===NO?this.outletData:this.parent.get(n,e)}},Du=new g("");var r0=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&A(0,"router-outlet")},dependencies:[i0],encapsulation:2,changeDetection:1})}return t})();function Wg(t){let n=t.children&&t.children.map(Wg),e=n?P(p({},t),{children:n}):p({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==de&&(e.component=r0),e}function kO(t,n,e){let i=new Set,r=el(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new _u(r,n)}}function el(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._futureSnapshot=n.value;let o=AO(t,n,e,i);return new Zt(r,o)}else{if(t.shouldAttach(n.value)){let s=t.retrieve(n.value);if(s!==null){let a=s.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(l=>el(t,l,void 0,i)),a}}let r=RO(n.value);i.add(r);let o=n.children.map(s=>el(t,s,void 0,i));return new Zt(r,o)}}function AO(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return el(t,r,o,i);return el(t,r,void 0,i)})}function RO(t){return new Qr(new Je(t.url),new Je(t.params),new Je(t.queryParams),new Je(t.fragment),new Je(t.data),t.outlet,t.component,t)}var tl=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},o0="ngNavigationCancelingError";function yu(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=Qo(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=s0(!1,Ft.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function s0(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[o0]=!0,e.cancellationCode=n,e}function OO(t){return a0(t)&&Qo(t.url)}function a0(t){return!!t&&t[o0]}var Lg=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),pg(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=Yo(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Yo(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=Yo(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=Yo(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new kg(o.value.snapshot))}),n.children.length&&this.forwardEvent(new Mg(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(pg(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),pg(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},Cu=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},Ko=class{component;route;constructor(n,e){this.component=n,this.route=e}};function PO(t,n,e){let i=t._root,r=n?n._root:null;return qa(i,r,e,[i.value])}function FO(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function rs(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!xh(t)?t:n.get(t):i}function qa(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=Yo(n);return t.children.forEach(s=>{LO(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>Ka(a,e.getContext(s),r)),r}function LO(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let l=VO(s,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new Cu(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?qa(t,n,a?a.children:null,i,r):qa(t,n,e,i,r),l&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new Ko(a.outlet.component,s))}else s&&Ka(n,a,r),r.canActivateChecks.push(new Cu(i)),o.component?qa(t,null,a?a.children:null,i,r):qa(t,null,e,i,r);return r}function VO(t,n,e){if(typeof e=="function")return Ve(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Kr(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Kr(t.url,n.url)||!Zn(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Pg(t,n)||!Zn(t.queryParams,n.queryParams);default:return!Pg(t,n)}}function Ka(t,n,e){let i=Yo(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?Ka(s,n.children.getContext(o),e):Ka(s,null,e):Ka(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new Ko(n.outlet.component,r)):e.canDeactivateChecks.push(new Ko(null,r)):e.canDeactivateChecks.push(new Ko(null,r))}function ol(t){return typeof t=="function"}function BO(t){return typeof t=="boolean"}function jO(t){return t&&ol(t.canLoad)}function UO(t){return t&&ol(t.canActivate)}function HO(t){return t&&ol(t.canActivateChild)}function zO(t){return t&&ol(t.canDeactivate)}function $O(t){return t&&ol(t.canMatch)}function l0(t){return t instanceof yr||t?.name==="EmptyError"}var lu=Symbol("INITIAL_VALUE");function is(){return ft(t=>Ws(t.map(n=>n.pipe($e(1),kt(lu)))).pipe(re(n=>{for(let e of n)if(e!==!0){if(e===lu)return lu;if(e===!1||WO(e))return e}return!0}),be(n=>n!==lu),$e(1)))}function WO(t){return Qo(t)||t instanceof tl}function c0(t){return t.aborted?q(void 0).pipe($e(1)):new ie(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function d0(t){return Ce(c0(t))}function GO(t){return Nt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?q(P(p({},n),{guardsResult:!0})):qO(o,e,i).pipe(Nt(s=>s&&BO(s)?YO(e,r,t):q(s)),re(s=>P(p({},n),{guardsResult:s})))})}function qO(t,n,e){return Pe(t).pipe(Nt(i=>JO(i.component,i.route,e,n)),ri(i=>i!==!0,!0))}function YO(t,n,e){return Pe(n).pipe(So(i=>Ri(KO(i.route.parent,e),ZO(i.route,e),QO(t,i.path),XO(t,i.route))),ri(i=>i!==!0,!0))}function ZO(t,n){return t!==null&&n&&n(new Ng(t)),q(!0)}function KO(t,n){return t!==null&&n&&n(new Tg(t)),q(!0)}function XO(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return q(!0);let i=e.map(r=>pn(()=>{let o=n._environmentInjector,s=rs(r,o),a=UO(s)?s.canActivate(n,t):Ve(o,()=>s(n,t));return Jr(a).pipe(ri())}));return q(i).pipe(is())}function QO(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>FO(o)).filter(o=>o!==null).map(o=>pn(()=>{let s=o.guards.map(a=>{let l=o.node._environmentInjector,c=rs(a,l),u=HO(c)?c.canActivateChild(e,t):Ve(l,()=>c(e,t));return Jr(u).pipe(ri())});return q(s).pipe(is())}));return q(r).pipe(is())}function JO(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return q(!0);let o=r.map(s=>{let a=n._environmentInjector,l=rs(s,a),c=zO(l)?l.canDeactivate(t,n,e,i):Ve(a,()=>l(t,n,e,i));return Jr(c).pipe(ri())});return q(o).pipe(is())}function eP(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return q(!0);let s=o.map(a=>{let l=rs(a,t),c=jO(l)?l.canLoad(n,e):Ve(t,()=>l(n,e)),u=Jr(c);return r?u.pipe(d0(r)):u});return q(s).pipe(is(),u0(i))}function u0(t){return sh(rt(n=>{if(typeof n!="boolean")throw yu(t,n)}),re(n=>n===!0))}function tP(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return q(!0);let a=s.map(l=>{let c=rs(l,t),u=$O(c)?c.canMatch(n,e,r):Ve(t,()=>c(n,e,r));return Jr(u).pipe(d0(o))});return q(a).pipe(is(),u0(i))}var Si=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},nl=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function nP(t){throw new C(4e3,!1)}function iP(t){throw s0(!1,Ft.GuardRejected)}var Vg=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[de])throw nP(`${n.redirectTo}`);r=r.children[de]}}async applyRedirectCommands(n,e,i,r,o){let s=await rP(e,r,o);if(s instanceof xn)throw new nl(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new nl(a);return a}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new xn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s=Object.create(null);return Object.entries(e.children).forEach(([a,l])=>{s[a]=this.createSegmentGroup(n,l,i,r)}),new Ee(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new C(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function rP(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return fu(Jr(Ve(e,()=>i(n))))}function oP(t,n){return t.providers&&!t._injector&&(t._injector=ka(t.providers,n,`Route: ${t.path}`)),t._injector??n}function wn(t){return t.outlet||de}function sP(t,n){let e=t.filter(i=>wn(i)===n);return e.push(...t.filter(i=>wn(i)!==n)),e}var Bg={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function f0(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function aP(t,n,e,i,r,o,s){let a=h0(t,n,e);if(!a.matched)return q(a);let l=f0(o(a));return i=oP(n,i),tP(i,n,e,r,l,s).pipe(re(c=>c===!0?a:p({},Bg)))}function h0(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?p({},Bg):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||eO)(e,t,n);if(!r)return p({},Bg);let o={};Object.entries(r.posParams??{}).forEach(([a,l])=>{o[a]=l.path});let s=r.consumed.length>0?p(p({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function PD(t,n,e,i,r){return e.length>0&&dP(t,e,i,r)?{segmentGroup:new Ee(n,cP(i,new Ee(e,t.children))),slicedSegments:[]}:e.length===0&&uP(t,e,i)?{segmentGroup:new Ee(t.segments,lP(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Ee(t.segments,t.children),slicedSegments:e}}function lP(t,n,e,i){let r={};for(let o of e)if(Eu(t,n,o)&&!i[wn(o)]){let s=new Ee([],{});r[wn(o)]=s}return p(p({},i),r)}function cP(t,n){let e={};e[de]=n;for(let i of t)if(i.path===""&&wn(i)!==de){let r=new Ee([],{});e[wn(i)]=r}return e}function dP(t,n,e,i){return e.some(r=>!Eu(t,n,r)||!(wn(r)!==de)?!1:!(i!==void 0&&wn(r)===i))}function uP(t,n,e){return e.some(i=>Eu(t,n,i))}function Eu(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function fP(t,n,e){return n.length===0&&!t.children[e]}var jg=class{};async function hP(t,n,e,i,r,o,s,a){return new Ug(t,n,e,i,r,s,o,a).recognize()}var mP=31,Ug=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=l,this.applyRedirects=new Vg(this.urlSerializer,this.urlTree)}noMatchError(n){return new C(4002,`'${n.segmentGroup}'`)}async recognize(){let n=PD(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Zt(i,e),o=new bu("",r),s=yO(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let e=new Ja([],Object.freeze({}),Object.freeze(p({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),de,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,de,e),rootSnapshot:e}}catch(i){if(i instanceof nl)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Si?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=await this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof Zt?[s]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let s=[];for(let l of o){let c=i.children[l],u=sP(e,l),f=await this.processSegmentGroup(n,u,c,l,r);s.push(...f)}let a=m0(s);return pP(a),a}async processSegment(n,e,i,r,o,s,a){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,s,a)}catch(c){if(c instanceof Si||l0(c))continue;throw c}if(fP(i,r,o))return new jg;throw new Si(i)}async processSegmentAgainstRoute(n,e,i,r,o,s,a,l){if(wn(i)!==s&&(s===de||!Eu(r,o,i)))throw new Si(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,l);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,l);throw new Si(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:f,remainingSegments:h}=h0(e,r,o);if(!l)throw new Si(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>mP&&(this.allowRedirects=!1));let m=this.createSnapshot(n,r,o,c,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let v=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,f,f0(m),n),S=await this.applyRedirects.lineralizeSegments(r,v);return this.processSegment(n,i,e,S.concat(h),s,!1,a)}createSnapshot(n,e,i,r,o){let s=new Ja(i,r,Object.freeze(p({},this.urlTree.queryParams)),this.urlTree.fragment,vP(e),wn(e),e.component??e._loadedComponent??null,e,_P(e),n),a=zg(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,e,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=ye=>this.createSnapshot(n,i,ye.consumedSegments,ye.parameters,s),l=await fu(aP(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new Si(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:f,consumedSegments:h,remainingSegments:m}=l,v=this.createSnapshot(n,i,h,f,s),{segmentGroup:S,slicedSegments:M}=PD(e,h,m,c,o);if(M.length===0&&S.hasChildren()){let ye=await this.processChildren(u,c,S,v);return new Zt(v,ye)}if(c.length===0&&M.length===0)return new Zt(v,[]);let O=wn(i)===o,pe=await this.processSegment(u,c,S,M,O?de:o,!0,v);return new Zt(v,pe instanceof Zt?[pe]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await fu(eP(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw iP(e)}return{routes:[],injector:n}}};function pP(t){t.sort((n,e)=>n.value.outlet===de?-1:e.value.outlet===de?1:n.value.outlet.localeCompare(e.value.outlet))}function gP(t){let n=t.value.routeConfig;return n&&n.path===""}function m0(t){let n=[],e=new Set;for(let i of t){if(!gP(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=m0(i.children);n.push(new Zt(i.value,r))}return n.filter(i=>!e.has(i))}function vP(t){return t.data||{}}function _P(t){return t.resolve||{}}function bP(t,n,e,i,r,o,s){return Nt(async a=>{let{state:l,tree:c}=await hP(t,n,e,i,a.extractedUrl,r,o,s);return P(p({},a),{targetSnapshot:l,urlAfterRedirects:c})})}function yP(t){return Nt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return q(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let l of p0(a))o.add(l);let s=0;return Pe(o).pipe(So(a=>r.has(a)?CP(a,e,t):(a.data=zg(a,a.parent,t).resolve,q(void 0))),rt(()=>s++),Tc(1),Nt(a=>s===o.size?q(n):ze))})}function p0(t){let n=t.children.map(e=>p0(e)).flat();return[t,...n]}function CP(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!n0(i)&&(r[il]=i.title),pn(()=>(t.data=zg(t,t.parent,e).resolve,SP(r,t,n).pipe(re(o=>(t._resolvedData=o,t.data=p(p({},t.data),o),null)))))}function SP(t,n,e){let i=vg(t);if(i.length===0)return q({});let r={};return Pe(i).pipe(Nt(o=>DP(t[o],n,e).pipe(ri(),rt(s=>{if(s instanceof tl)throw yu(new Xr,s);r[o]=s}))),Tc(1),re(()=>r),Cr(o=>l0(o)?ze:zs(o)))}function DP(t,n,e){let i=n._environmentInjector,r=rs(t,i),o=r.resolve?r.resolve(n,e):Ve(i,()=>r(n,e));return Jr(o)}function FD(t){return ft(n=>{let e=t(n);return e?Pe(e).pipe(re(()=>n)):q(n)})}var g0=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===de);return i}getResolvedTitleForRoute(e){return e.data[il]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:()=>d(EP)})}return t})(),EP=(()=>{class t extends g0{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(j(TD))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),wu=new g("",{factory:()=>({})}),xu=new g(""),v0=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(Hp);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await BD(Ve(e,()=>i.loadComponent())),s=await _0(qp(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await wP(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();async function wP(t,n,e,i){let r=await BD(Ve(e,()=>t.loadChildren())),o=await _0(qp(r)),s;o instanceof jd||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(t);let a,l,c=!1,u;return Array.isArray(s)?(l=s,c=!0):(a=s.create(e).injector,u=s,l=a.get(xu,[],{optional:!0,self:!0}).flat()),{routes:l.map(Wg),injector:a,factory:u}}async function _0(t){return t}var Gg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:()=>d(xP)})}return t})(),xP=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),b0=new g("");var y0=new g(""),IP=()=>{},C0=new g(""),S0=(()=>{class t{currentNavigation=H(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=H(null);events=new D;transitionAbortWithErrorSubject=new D;configLoader=d(v0);environmentInjector=d(xe);destroyRef=d(He);urlSerializer=d(Su);rootContexts=d(rl);location=d(Wi);inputBindingEnabled=d(Du,{optional:!0})!==null;titleStrategy=d(g0);options=d(wu,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||MO;urlHandlingStrategy=d(Gg);createViewTransition=d(b0,{optional:!0});navigationErrorHandler=d(C0,{optional:!0});activatedRouteInjectorFeature=d(y0,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>q(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new xg(r)),i=r=>this.events.next(new Ig(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;$(()=>{this.transitions?.next(P(p({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Je(null),this.transitions.pipe(be(i=>i!==null),ft(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return q(i).pipe(ft(l=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",Ft.SupersededByNewNavigation),ze;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:l.id,initialUrl:l.rawUrl,extractedUrl:l.extractedUrl,targetBrowserUrl:typeof l.extras.browserUrl=="string"?this.urlSerializer.parse(l.extras.browserUrl):l.extras.browserUrl,trigger:l.source,extras:l.extras,previousNavigation:c?P(p({},c),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:l.routesRecognizeHandler,beforeActivateHandler:l.beforeActivateHandler});let u=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=l.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!u&&f!=="reload")return this.events.next(new qi(l.id,this.urlSerializer.serialize(l.rawUrl),"",pu.IgnoredSameUrlNavigation)),l.resolve(!1),ze;if(this.urlHandlingStrategy.shouldProcessUrl(l.rawUrl))return q(l).pipe(ft(h=>(this.events.next(new Jo(h.id,this.urlSerializer.serialize(h.extractedUrl),h.source,h.restoredState)),h.id!==this.navigationId?ze:Promise.resolve(h))),bP(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),rt(h=>{i.targetSnapshot=h.targetSnapshot,i.urlAfterRedirects=h.urlAfterRedirects,this.currentNavigation.update(m=>(m.finalUrl=h.urlAfterRedirects,m)),this.events.next(new Qa)}),ft(h=>Pe(i.routesRecognizeHandler.deferredHandle??q(void 0)).pipe(re(()=>h))),rt(()=>{let h=new gu(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(h)}));if(u&&this.urlHandlingStrategy.shouldProcessUrl(l.currentRawUrl)){let{id:h,extractedUrl:m,source:v,restoredState:S,extras:M}=l,O=new Jo(h,this.urlSerializer.serialize(m),v,S);this.events.next(O);let pe=e0(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=P(p({},l),{targetSnapshot:pe,urlAfterRedirects:m,extras:P(p({},M),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(ye=>(ye.finalUrl=m,ye)),q(i)}else return this.events.next(new qi(l.id,this.urlSerializer.serialize(l.extractedUrl),"",pu.IgnoredByUrlHandlingStrategy)),l.resolve(!1),ze}),re(l=>{let c=new Sg(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);return this.events.next(c),this.currentTransition=i=P(p({},l),{guards:PO(l.targetSnapshot,l.currentSnapshot,this.rootContexts)}),i}),GO(l=>this.events.next(l)),ft(l=>{if(i.guardsResult=l.guardsResult,l.guardsResult&&typeof l.guardsResult!="boolean")throw yu(this.urlSerializer,l.guardsResult);let c=new Dg(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot,!!l.guardsResult);if(this.events.next(c),!a())return ze;if(!l.guardsResult)return this.cancelNavigationTransition(l,"",Ft.GuardRejected),ze;if(l.guards.canActivateChecks.length===0)return q(l);let u=new Eg(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);if(this.events.next(u),!a())return ze;let f=!1;return q(l).pipe(yP(this.paramsInheritanceStrategy),rt({next:()=>{f=!0;let h=new wg(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(h)},complete:()=>{f||this.cancelNavigationTransition(l,"",Ft.NoDataFromResolver)}}))}),FD(l=>{let c=f=>{let h=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let m=f._environmentInjector;h.push(this.configLoader.loadComponent(m,f.routeConfig).then(v=>{f.component=v}))}for(let m of f.children)h.push(...c(m));return h},u=c(l.targetSnapshot.root);return u.length===0?q(l):Pe(Promise.all(u).then(()=>l))}),ft(l=>{let{newlyCreatedRoutes:c,state:u}=kO(e.routeReuseStrategy,l.targetSnapshot,l.currentRouterState);return this.currentTransition=i=l=P(p({},l),{targetRouterState:u,newlyCreatedRoutes:c}),this.currentNavigation.update(f=>(f.targetRouterState=u,f)),q(l)}),this.activatedRouteInjectorFeature?.operator()??(l=>l),FD(()=>this.afterPreactivation()),ft(()=>{let{currentSnapshot:l,targetSnapshot:c}=i,u=this.createViewTransition?.(this.environmentInjector,l.root,c.root);return u?Pe(u).pipe(re(()=>i)):q(i)}),$e(1),ft(l=>{r=!1,this.events.next(new ts);let c=i.beforeActivateHandler.deferredHandle;return c?Pe(c.then(()=>l)):q(l)}),rt(l=>{new Lg(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),l.newlyCreatedRoutes?.clear(),a()&&(o=!0,this.currentNavigation.update(c=>(c.abort=IP,c)),this.lastSuccessfulNavigation.set($(this.currentNavigation)),this.events.next(new Gi(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects))),this.titleStrategy?.updateTitle(l.targetRouterState.snapshot),l.resolve(!0))}),Ce(c0(s.signal).pipe(be(()=>!o&&r),rt(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",Ft.Aborted)}))),rt({complete:()=>{o=!0}}),Ce(this.transitionAbortWithErrorSubject.pipe(rt(l=>{throw l}))),Sr(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",Ft.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Cr(l=>{if(o=!0,LD(i),this.destroyed)return i.resolve(!1),ze;if(a0(l))this.events.next(new En(i.id,this.urlSerializer.serialize(i.extractedUrl),l.message,l.cancellationCode)),OO(l)?this.events.next(new ns(l.url,l.navigationBehaviorOptions)):i.resolve(!1);else{let c=new es(i.id,this.urlSerializer.serialize(i.extractedUrl),l,i.targetSnapshot??void 0);try{let u=Ve(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(u instanceof tl){let{message:f,cancellationCode:h}=yu(this.urlSerializer,u);this.events.next(new En(i.id,this.urlSerializer.serialize(i.extractedUrl),f,h)),this.events.next(new ns(u.redirectTo,u.navigationBehaviorOptions))}else throw this.events.next(c),l}catch(u){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(u)}}return ze}))}))}cancelNavigationTransition(e,i,r){LD(e);let o=new En(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=$(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function TP(t){return t!==Za}function LD(t){if(t.newlyCreatedRoutes)for(let n of t.newlyCreatedRoutes)n._localInjector?.destroy()}var D0=new g("");var MP=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:()=>d(NP)})}return t})(),Hg=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},NP=(()=>{class t extends Hg{static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),qg=(()=>{class t{urlSerializer=d(Su);options=d(wu,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(Wi);urlHandlingStrategy=d(Gg);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new xn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof xn?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=e0(null,d(xe));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:()=>d(kP)})}return t})(),kP=(()=>{class t extends qg{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof Jo?this.updateStateMemento():e instanceof qi?this.commitTransition(i):e instanceof gu?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof ts?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof En&&!JD(e)?this.restoreHistory(i):e instanceof es?this.restoreHistory(i,!0):e instanceof Gi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let l=this.browserPageId,c=p(p({},a),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=p(p({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?p({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):p({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function E0(t,n){t.events.pipe(be(e=>e instanceof Gi||e instanceof En||e instanceof es||e instanceof qi),re(e=>e instanceof Gi||e instanceof qi?0:(e instanceof En?e.code===Ft.Redirect||e.code===Ft.SupersededByNewNavigation:!1)?2:1),be(e=>e!==2),$e(1)).subscribe(()=>{n()})}var Yg=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(Hd);stateManager=d(qg);options=d(wu,{optional:!0})||{};pendingTasks=d(di);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(S0);urlSerializer=d(Su);location=d(Wi);urlHandlingStrategy=d(Gg);injector=d(xe);_events=new D;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(MP);injectorCleanup=d(D0,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(xu,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(Du,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new ge;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=$(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof En&&i.code!==Ft.Redirect&&i.code!==Ft.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Gi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof ns){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=p({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||TP(r.source)},s);this.scheduleNavigation(a,Za,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}IO(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Za,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=P(p({},o),{browserUrl:e})),r){let c=p({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(a);this.scheduleNavigation(l,i,s,o).catch(c=>{this.disposed||this.injector.get(rn)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return $(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Wg),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=p(p({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let f;try{let h=r?r.snapshot:this.routerState.snapshot.root;f=ZD(h)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return KD(f,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=Qo(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Za,null,i)}navigate(e,i={skipLocationChange:!1}){return AP(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(Vn(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=p({},UD):i===!1?r=p({},_g):r=p(p({},_g),i),Qo(e))return ND(this.currentUrlTree,e,r);let o=this.parseUrl(e);return ND(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,l,c;s?(a=s.resolve,l=s.reject,c=s.promise):c=new Promise((f,h)=>{a=f,l=h});let u=this.pendingTasks.add();return E0(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function AP(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new C(4008,!1)}var RP=new g("");function Zg(t,...n){return Bn([{provide:xu,multi:!0,useValue:t},{provide:Qr,useFactory:OP},{provide:Aa,multi:!0,useFactory:PP},n.map(e=>e.\u0275providers)])}function OP(){return d(Yg).routerState.root}function PP(){let t=d(V);return n=>{let e=t.get(Gt);if(n!==e.components[0])return;let i=t.get(Yg),r=t.get(FP);t.get(LP)===1&&i.initialNavigation(),t.get(VP,null,{optional:!0})?.setUpPreloading(),t.get(RP,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var FP=new g("",{factory:()=>new D}),LP=new g("",{factory:()=>1});var VP=new g("");var w0=[];var x0={providers:[um(),Zg(w0)]};var R0=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(lt(we),lt(F))};static \u0275dir=N({type:t})}return t})(),jP=(()=>{class t extends R0{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Dt(t)))(r||t)}})();static \u0275dir=N({type:t,features:[Ae]})}return t})(),eo=new g("");var UP={provide:eo,useExisting:Bt(()=>O0),multi:!0};function HP(){let t=cn()?cn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var zP=new g(""),O0=(()=>{class t extends R0{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!HP())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(lt(we),lt(F),lt(zP,8))};static \u0275dir=N({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&z("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Re([UP]),Ae]})}return t})();function Qg(t){return t==null||Jg(t)===0}function Jg(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var to=new g(""),P0=new g(""),$P=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Kn=class{static min(n){return WP(n)}static max(n){return GP(n)}static required(n){return F0(n)}static requiredTrue(n){return qP(n)}static email(n){return YP(n)}static minLength(n){return ZP(n)}static maxLength(n){return KP(n)}static pattern(n){return XP(n)}static nullValidator(n){return Tu()}static compose(n){return H0(n)}static composeAsync(n){return z0(n)}};function WP(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function GP(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function F0(t){return Qg(t.value)?{required:!0}:null}function qP(t){return t.value===!0?null:{required:!0}}function YP(t){return Qg(t.value)||$P.test(t.value)?null:{email:!0}}function ZP(t){return n=>{let e=n.value?.length??Jg(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function KP(t){return n=>{let e=n.value?.length??Jg(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function XP(t){if(!t)return Tu;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(Qg(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function Tu(t){return null}function L0(t){return t!=null}function V0(t){return gi(t)?Pe(t):t}function B0(t){let n={};return t.forEach(e=>{n=e!=null?p(p({},n),e):n}),Object.keys(n).length===0?null:n}function j0(t,n){return n.map(e=>e(t))}function QP(t){return!t.validate}function U0(t){return t.map(n=>QP(n)?n:e=>n.validate(e))}function H0(t){if(!t)return null;let n=t.filter(L0);return n.length==0?null:function(e){return B0(j0(e,n))}}function ev(t){return t!=null?H0(U0(t)):null}function z0(t){if(!t)return null;let n=t.filter(L0);return n.length==0?null:function(e){let i=j0(e,n).map(V0);return Gs(i).pipe(re(B0))}}function tv(t){return t!=null?z0(U0(t)):null}function I0(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function $0(t){return t._rawValidators}function W0(t){return t._rawAsyncValidators}function Kg(t){return t?Array.isArray(t)?t:[t]:[]}function Mu(t,n){return Array.isArray(t)?t.includes(n):t===n}function T0(t,n){let e=Kg(n);return Kg(t).forEach(r=>{Mu(e,r)||e.push(r)}),e}function M0(t,n){return Kg(n).filter(e=>!Mu(t,e))}var Nu=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=ev(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=tv(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},as=class extends Nu{name;get formDirective(){return null}get path(){return null}};var sl="VALID",Iu="INVALID",os="PENDING",al="DISABLED",Yi=class{},ku=class extends Yi{value;source;constructor(n,e){super(),this.value=n,this.source=e}},cl=class extends Yi{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},dl=class extends Yi{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},ss=class extends Yi{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Au=class extends Yi{source;constructor(n){super(),this.source=n}},ls=class extends Yi{source;constructor(n){super(),this.source=n}};function G0(t){return(Pu(t)?t.validators:t)||null}function JP(t){return Array.isArray(t)?ev(t):t||null}function q0(t,n){return(Pu(n)?n.asyncValidators:t)||null}function eF(t){return Array.isArray(t)?tv(t):t||null}function Pu(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function tF(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new C(1e3,"");if(!Y0(i,e))throw new C(1001,"")}function nF(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new C(-1002,"")})}var cs=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=H(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return $(this.statusReactive)}set status(n){$(()=>this.statusReactive.set(n))}_status=w(()=>this.statusReactive());statusReactive=H(void 0);get valid(){return this.status===sl}get invalid(){return this.status===Iu}get pending(){return this.status===os}get disabled(){return this.status===al}get enabled(){return this.status!==al}errors;get pristine(){return $(this.pristineReactive)}set pristine(n){$(()=>this.pristineReactive.set(n))}_pristine=w(()=>this.pristineReactive());pristineReactive=H(!0);get dirty(){return!this.pristine}get touched(){return $(this.touchedReactive)}set touched(n){$(()=>this.touchedReactive.set(n))}_touched=w(()=>this.touchedReactive());touchedReactive=H(!1);get untouched(){return!this.touched}_events=new D;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(T0(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(T0(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(M0(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(M0(n,this._rawAsyncValidators))}hasValidator(n){return Mu(this._rawValidators,n)}hasAsyncValidator(n){return Mu(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(P(p({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new dl(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new dl(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(P(p({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new cl(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new cl(!0,i))}markAsPending(n={}){this.status=os;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new ss(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(P(p({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=al,this.errors=null,this._forEachChild(r=>{r.disable(P(p({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new ku(this.value,i)),this._events.next(new ss(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(P(p({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=sl,this._forEachChild(i=>{i.enable(P(p({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(P(p({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===sl||this.status===os)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new ku(this.value,e)),this._events.next(new ss(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(P(p({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?al:sl}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=os,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=V0(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new ss(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new W,this.statusChanges=new W}_calculateStatus(){return this._allControlsDisabled()?al:this.errors?Iu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(os)?os:this._anyControlsHaveStatus(Iu)?Iu:sl}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new cl(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new dl(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Pu(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=JP(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=eF(this._rawAsyncValidators)}_updateHasRequiredValidator(){$(()=>this._hasRequired.set(this.hasValidator(Kn.required)))}};function Y0(t,n){return Object.hasOwn(t,n)}function nv(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function Z0(t){if(t.tagName!=="INPUT")return!1;let n=t.type;return n==="number"||n==="range"||n==="date"||n==="month"}function K0(t){return t.tagName==="INPUT"||t.tagName==="TEXTAREA"}function fl(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var Xg=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var iF=(()=>{class t{_validator=Tu;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):Tu,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,features:[qe]})}return t})();var rF={provide:to,useExisting:Bt(()=>X0),multi:!0};var X0=(()=>{class t extends iF{required;inputName="required";normalizeInput=ne;createValidator=e=>F0;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Dt(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&_e("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Re([rF]),Ae]})}return t})();var Q0=new g(""),J0=new g("",{factory:()=>oF}),oF="always";function N0(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Ou(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Ru(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function eE(t,n){let e=$0(t);n.validator!==null?t.setValidators(I0(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=W0(t);n.asyncValidator!==null?t.setAsyncValidators(I0(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Ru(n._rawValidators,r),Ru(n._rawAsyncValidators,r)}function Ou(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=$0(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=W0(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Ru(n._rawValidators,i),Ru(n._rawAsyncValidators,i),e}function tE(t,n){t==null,eE(t,n)}function sF(t,n){return Ou(t,n)}function aF(t){return Object.getPrototypeOf(t.constructor)===jP}function nE(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function iv(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===O0?e=o:aF(o)?i=o:r=o}),r||i||e||null}function lF(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var Zi=class extends Nu{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof ls&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=iv(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(He)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(Le);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new ge,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof ls&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=nv(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof X0))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&fl(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new Xg({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=w(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),nt(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}};var ul=class extends cs{constructor(n,e,i){super(G0(e),q0(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){$(()=>{nF(this,!0,n),Object.keys(n).forEach(i=>{tF(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,P(p({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new ls(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return Y0(this.controls,n)?this.controls[n]:null}};var cF={provide:as,useExisting:Bt(()=>hl)},ll=Promise.resolve(),hl=(()=>{class t extends as{callSetDisabledState;get submitted(){return $(this.submittedReactive)}_submitted=w(()=>this.submittedReactive());submittedReactive=H(!1);_directives=new Set;form;ngSubmit=new W;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new ul({},ev(e),tv(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){ll.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){ll.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){ll.then(()=>{let i=this._findContainer(e.path),r=new ul({});tE(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){ll.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){ll.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),nE(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Au(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(lt(to,10),lt(P0,10),lt(J0,8))};static \u0275dir=N({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&z("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Re([cF]),Ae]})}return t})();function k0(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function A0(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var dF=class extends cs{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(G0(e),q0(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Pu(e)&&(e.nonNullable||e.initialValueIsDefault)&&(A0(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){$(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new ls(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){k0(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){k0(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){A0(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var uF=t=>t instanceof dF;var fF=(()=>{class t extends as{callSetDisabledState;get submitted(){return $(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=w(()=>this._submittedReactive());_submittedReactive=H(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Ou(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){N0(e.control||null,e,!1),lF(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,nE(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Au(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(N0(i||null,e),uF(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);tE(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&sF(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){eE(this.form,this),this._oldForm&&Ou(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(lt(to,10),lt(P0,10),lt(J0,8))};static \u0275dir=N({type:t,features:[Ae,qe]})}return t})();var hF={provide:as,useExisting:Bt(()=>ml)},ml=(()=>{class t extends fF{form=null;ngSubmit=new W;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Dt(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&z("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Re([hF]),Ae]})}return t})();var aE=Symbol("FIELD_TREE"),rv=0;function mF(){return rv}function Ki(t,n){return(...e)=>{try{return rv=n,t(...e)}finally{rv=0}}}function pF(t){return!t}function iE(t){return t}function Qi(t){return Array.isArray(t)}function Lu(t){return(typeof t=="object"||typeof t=="function")&&t!=null}var Xi=Symbol(),$u=Symbol(),gl=class{predicates;fns=[];constructor(n){this.predicates=n}push(n){this.fns.push(rE(this.predicates,n))}mergeIn(n){let e=this.predicates?n.fns.map(i=>rE(this.predicates,i)):n.fns;this.fns.push(...e)}hasRules(){return this.fns.length>0}},Vu=class extends gl{get defaultValue(){return!1}compute(n){return this.fns.some(e=>{let i=e(n);return i&&i!==$u})}},us=class t extends gl{ignore;static ignoreNull(n){return new t(n,e=>e===null)}constructor(n,e){super(n),this.ignore=e}get defaultValue(){return[]}compute(n){return this.fns.reduce((e,i)=>{let r=i(n);return r===void 0||r===$u?e:Qi(r)?[...e,...this.ignore?r.filter(o=>!this.ignore(o)):r]:this.ignore&&this.ignore(r)?e:[...e,r]},[])}},ov=class extends us{constructor(n){super(n,void 0)}},sv=class extends gl{key;get defaultValue(){return this.key.reducer.getInitial()}constructor(n,e){super(n),this.key=e}compute(n){if(this.fns.length===0)return this.key.reducer.getInitial();let e=this.key.reducer.getInitial();for(let i=0;i<this.fns.length;i++){let r=this.fns[i](n);r!==$u&&(e=this.key.reducer.reduce(e,r))}return e}};function rE(t,n){return t.length===0?n:e=>{for(let i of t){let r=e.stateOf(i.path),o=$(r.structure.pathKeys).length-i.depth;for(let s=0;s<o;s++)r=r.structure.parent;if(!i.fn(r.context))return $u}return n(e)}}var fs=class{predicates;hidden;disabledReasons;readonly;syncErrors;syncTreeErrors;asyncErrors;metadata=new Map;constructor(n){this.predicates=n,this.hidden=new Vu(n),this.disabledReasons=new ov(n),this.readonly=new Vu(n),this.syncErrors=us.ignoreNull(n),this.syncTreeErrors=us.ignoreNull(n),this.asyncErrors=us.ignoreNull(n)}hasAnyLogic(){return this.hidden.hasRules()||this.disabledReasons.hasRules()||this.readonly.hasRules()||this.syncErrors.hasRules()||this.syncTreeErrors.hasRules()||this.asyncErrors.hasRules()||this.metadata.size>0}hasMetadata(n){return this.metadata.has(n)}hasMetadataKeys(){return this.metadata.size>0}getMetadataKeys(){return this.metadata.keys()}getMetadata(n){return this.metadata.has(n)||this.metadata.set(n,new sv(this.predicates,n)),this.metadata.get(n)}mergeIn(n){this.hidden.mergeIn(n.hidden),this.disabledReasons.mergeIn(n.disabledReasons),this.readonly.mergeIn(n.readonly),this.syncErrors.mergeIn(n.syncErrors),this.syncTreeErrors.mergeIn(n.syncTreeErrors),this.asyncErrors.mergeIn(n.asyncErrors);for(let e of n.getMetadataKeys()){let i=n.metadata.get(e);this.getMetadata(e).mergeIn(i)}}},Bu=class{depth;constructor(n){this.depth=n}build(){return new ju(this,[],0)}},hs=class t extends Bu{constructor(n){super(n)}current;all=[];addHiddenRule(n){this.getCurrent().addHiddenRule(n)}addDisabledReasonRule(n){this.getCurrent().addDisabledReasonRule(n)}addReadonlyRule(n){this.getCurrent().addReadonlyRule(n)}addSyncErrorRule(n){this.getCurrent().addSyncErrorRule(n)}addSyncTreeErrorRule(n){this.getCurrent().addSyncTreeErrorRule(n)}addAsyncErrorRule(n){this.getCurrent().addAsyncErrorRule(n)}addMetadataRule(n,e){this.getCurrent().addMetadataRule(n,e)}getChild(n){if(n===Xi){let e=this.getCurrent().children;e.size>(e.has(Xi)?1:0)&&(this.current=void 0)}return this.getCurrent().getChild(n)}hasLogic(n){return this===n?!0:this.all.some(({builder:e})=>e.hasLogic(n))}hasRules(){return this.all.length>0}anyChildHasLogic(){return this.all.some(({builder:n})=>n.anyChildHasLogic())}mergeIn(n,e){e?this.all.push({builder:n,predicate:{fn:Ki(e.fn,this.depth),path:e.path}}):this.all.push({builder:n}),this.current=void 0}getCurrent(){return this.current===void 0&&(this.current=new vl(this.depth),this.all.push({builder:this.current})),this.current}static newRoot(){return new t(0)}},vl=class extends Bu{logic=new fs([]);children=new Map;constructor(n){super(n)}addHiddenRule(n){this.logic.hidden.push(Ki(n,this.depth))}addDisabledReasonRule(n){this.logic.disabledReasons.push(Ki(n,this.depth))}addReadonlyRule(n){this.logic.readonly.push(Ki(n,this.depth))}addSyncErrorRule(n){this.logic.syncErrors.push(Ki(n,this.depth))}addSyncTreeErrorRule(n){this.logic.syncTreeErrors.push(Ki(n,this.depth))}addAsyncErrorRule(n){this.logic.asyncErrors.push(Ki(n,this.depth))}addMetadataRule(n,e){this.logic.getMetadata(n).push(Ki(e,this.depth))}getChild(n){return this.children.has(n)||this.children.set(n,new hs(this.depth+1)),this.children.get(n)}hasLogic(n){return this===n}hasRules(){return this.logic.hasAnyLogic()||this.children.size>0}anyChildHasLogic(){for(let n of this.children.values())if(n.hasRules())return!0;return!1}},ju=class t{builder;predicates;depth;logic;constructor(n,e,i){this.builder=n,this.predicates=e,this.depth=i,this.logic=n?gF(n,e,i):new fs([])}getChild(n){let e=this.builder?lE(this.builder,n):[];if(e.length===0)return new t(void 0,[],this.depth+1);if(e.length===1){let{builder:i,predicates:r}=e[0];return new t(i,[...this.predicates,...r.map(o=>lv(o,this.depth))],this.depth+1)}else{let i=e.map(({builder:r,predicates:o})=>new t(r,[...this.predicates,...o.map(s=>lv(s,this.depth))],this.depth+1));return new av(i)}}hasLogic(n){return this.builder?this.builder.hasLogic(n):!1}hasRules(){return this.builder?this.builder.hasRules():!1}anyChildHasLogic(){return this.builder?this.builder.anyChildHasLogic():!1}},av=class t{all;logic;constructor(n){this.all=n,this.logic=new fs([]);for(let e of n)this.logic.mergeIn(e.logic)}getChild(n){return new t(this.all.flatMap(e=>e.getChild(n)))}hasLogic(n){return this.all.some(e=>e.hasLogic(n))}hasRules(){return this.all.some(n=>n.hasRules())}anyChildHasLogic(){return this.all.some(n=>n.anyChildHasLogic())}};function lE(t,n){if(t instanceof hs)return t.all.flatMap(({builder:e,predicate:i})=>{let r=lE(e,n);return i?r.map(({builder:o,predicates:s})=>({builder:o,predicates:[...s,i]})):r});if(t instanceof vl)return[...n!==Xi&&t.children.has(Xi)?[{builder:t.getChild(Xi),predicates:[]}]:[],...t.children.has(n)?[{builder:t.getChild(n),predicates:[]}]:[]];throw new C(1909,!1)}function gF(t,n,e){let i=new fs(n);if(t instanceof hs){let r=t.all.map(({builder:o,predicate:s})=>new ju(o,s?[...n,lv(s,e)]:n,e));for(let o of r)i.mergeIn(o.logic)}else if(t instanceof vl)i.mergeIn(t.logic);else throw new C(1909,!1);return i}function lv(t,n){return P(p({},t),{depth:n})}var cE=Symbol("PATH"),un=class t{keys;parent;keyInParent;root;children=new Map;fieldPathProxy=new Proxy(this,vF);logicBuilder;constructor(n,e,i,r){this.keys=n,this.parent=i,this.keyInParent=r,this.root=e??this,i||(this.logicBuilder=hs.newRoot())}get builder(){return this.logicBuilder?this.logicBuilder:this.parent.builder.getChild(this.keyInParent)}getChild(n){return this.children.has(n)||this.children.set(n,new t([...this.keys,n],this.root,this,n)),this.children.get(n)}mergeIn(n,e){let i=n.compile();this.builder.mergeIn(i.builder,e)}static unwrapFieldPath(n){return n[cE]}static newRoot(){return new t([],void 0,void 0,void 0)}},vF={get(t,n){return n===cE?t:t.getChild(n).fieldPathProxy}},Fu,pl=new Map,_l=class t{schemaFn;constructor(n){this.schemaFn=n}compile(){if(pl.has(this))return pl.get(this);let n=un.newRoot();pl.set(this,n);let e=Fu;try{Fu=n,this.schemaFn(n.fieldPathProxy)}finally{Fu=e}return n}static create(n){return n instanceof t?n:new t(n)}static rootCompile(n){try{return pl.clear(),n===void 0?un.newRoot():n instanceof t?n.compile():new t(n).compile()}finally{pl.clear()}}};function _F(t){return t instanceof _l||typeof t=="function"}function ms(t){if(Fu!==un.unwrapFieldPath(t).root)throw new C(1908,!1)}function no(t,n,e){return ms(t),un.unwrapFieldPath(t).builder.addMetadataRule(n,e),n}var Ji={list(){return{reduce:(t,n)=>n===void 0?t:[...t,n],getInitial:()=>[]}},min(){return{reduce:(t,n)=>t===void 0||n===void 0?t??n:n<t?n:t,getInitial:()=>{}}},max(){return{reduce:(t,n)=>t===void 0||n===void 0?t??n:n>t?n:t,getInitial:()=>{}}},or(){return{reduce:(t,n)=>t||n,getInitial:()=>!1}},and(){return{reduce:(t,n)=>t&&n,getInitial:()=>!0}},override:bF};function bF(t){return{reduce:(n,e)=>e,getInitial:()=>t?.()}}var bv=Symbol("IS_ASYNC_VALIDATION_RESOURCE"),Uu=class{reducer;create;brand;[bv];constructor(n,e){this.reducer=n,this.create=e}};function In(t){return new Uu(t??Ji.override())}function yv(){return In()}var dE=In(Ji.or()),Cv=yv();var Sv=In(Ji.max()),Dv=yv();var Ev=In(Ji.min()),uE=In(Ji.max()),fE=In(Ji.min()),hE=In(Ji.list());function Kt(t,n){if(t===n)return!0;if(!t||!n||t.length!==n.length)return!1;for(let e=0;e<t.length;e++)if(!Object.is(t[e],n[e]))return!1;return!0}function yF(t){return t.errors().length>0?"invalid":t.pending()?"unknown":"valid"}var cv=class{node;constructor(n){this.node=n}rawSyncTreeErrors=w(()=>this.shouldSkipValidation()?[]:[...this.node.logicNode.logic.syncTreeErrors.compute(this.node.context),...this.node.structure.parent?.validationState.rawSyncTreeErrors()??[]],{equal:Kt});syncErrors=w(()=>this.shouldSkipValidation()?[]:[...this.node.logicNode.logic.syncErrors.compute(this.node.context),...this.syncTreeErrors(),...CF(this.node.submitState.submissionErrors())],{equal:Kt});syncValid=w(()=>this.shouldSkipValidation()?!0:this.node.structure.reduceChildren(this.syncErrors().length===0,(n,e)=>e&&n.validationState.syncValid(),pF));syncTreeErrors=w(()=>this.rawSyncTreeErrors().filter(n=>n.fieldTree===this.node.fieldTree),{equal:Kt});rawAsyncErrors=w(()=>this.shouldSkipValidation()?[]:[...this.node.logicNode.logic.asyncErrors.compute(this.node.context),...this.node.structure.parent?.validationState.rawAsyncErrors()??[]],{equal:Kt});asyncErrors=w(()=>this.shouldSkipValidation()?[]:this.rawAsyncErrors().filter(n=>n==="pending"||n.fieldTree===this.node.fieldTree),{equal:Kt});parseErrors=w(()=>this.node.formFieldBindings().flatMap(n=>n.parseErrors()),{equal:Kt});errors=w(()=>[...this.parseErrors(),...this.syncErrors(),...this.asyncErrors().filter(n=>n!=="pending")],{equal:Kt});errorSummary=w(()=>{let n=this.node.structure.reduceChildren(this.errors(),(e,i)=>[...i,...e.errorSummary()]);return $(()=>n.sort(SF)),n},{equal:Kt});pending=w(()=>this.node.structure.reduceChildren(this.asyncErrors().includes("pending"),(n,e)=>e||n.validationState.pending()));status=w(()=>{if(this.shouldSkipValidation())return"valid";let n=yF(this);return this.node.structure.reduceChildren(n,(e,i)=>i==="invalid"||e.validationState.status()==="invalid"?"invalid":i==="unknown"||e.validationState.status()==="unknown"?"unknown":"valid",e=>e==="invalid")});valid=w(()=>this.status()==="valid");invalid=w(()=>this.status()==="invalid");shouldSkipValidation=w(()=>this.node.hidden()||this.node.disabled()||this.node.readonly()||this.node.structure.isOrphaned())};function CF(t){return t===void 0?[]:Qi(t)?t:[t]}function mE(t,n){if(Qi(t))for(let e of t)e.fieldTree??=n;else t&&(t.fieldTree??=n);return t}function oE(t){return t.formField?t.formField.element:t.fieldTree().formFieldBindings().reduce((n,e)=>!n||!e.element?n??e.element:n.compareDocumentPosition(e.element)&Node.DOCUMENT_POSITION_PRECEDING?e.element:n,void 0)}function SF(t,n){let e=oE(t),i=oE(n);return e===i?0:e===void 0||i===void 0?e===void 0?1:-1:e.compareDocumentPosition(i)&Node.DOCUMENT_POSITION_PRECEDING?1:-1}var dv=In(),uv=class{node;cache=new WeakMap;constructor(n){this.node=n,this.fieldTreeOf=this.fieldTreeOf.bind(this),this.stateOf=this.stateOf.bind(this)}resolve(n){if(!this.cache.has(n)){let e=w(()=>{let i=un.unwrapFieldPath(n),r=this.node,o=mF();for(;o>0||!r.structure.logic.hasLogic(i.root.builder);)if(o--,r=r.structure.parent,r===void 0)throw new C(1900,!1);for(let s of i.keys)if(r=r.structure.getChild(s),r===void 0)throw new C(1901,!1);return r.fieldTree});this.cache.set(n,e)}return this.cache.get(n)()}get fieldTree(){return this.node.fieldProxy}get state(){return this.node}get value(){return this.node.structure.value}get key(){return this.node.structure.keyInParent}get pathKeys(){return this.node.structure.pathKeys}index=w(()=>{let n=this.key();if(!Qi($(this.node.structure.parent.value)))throw new C(1906,!1);return Number(n)});fieldTreeOf(n){return this.resolve(n)}stateOf(n){return this.resolve(n)()}valueOf=n=>{let e=this.resolve(n)().value();if(e instanceof cs)throw new C(1907,!1);return e}},fv=class{node;metadata=new Map;constructor(n){this.node=n}runMetadataCreateLifecycle(){if(!this.node.logicNode.logic.hasMetadataKeys())return;let n=zp();n&&Zd(!1);try{$(()=>Ve(this.node.structure.injector,()=>{for(let e of this.node.logicNode.logic.getMetadataKeys())if(e.create){let i=this.node.logicNode.logic.getMetadata(e),r=e.create(this.node,w(()=>i.compute(this.node.context)));this.metadata.set(e,r)}}))}finally{n&&Zd(!0)}}get(n){if(this.has(n)&&!this.metadata.has(n)){if(n.create)throw new C(1912,!1);let e=this.node.logicNode.logic.getMetadata(n);this.metadata.set(n,w(()=>e.compute(this.node.context)))}return this.metadata.get(n)}has(n){return this.node.logicNode.logic.hasMetadata(n)}},DF={get(t,n,e){if(n===aE)return!0;let i=t(),r=i.structure.getChild(n);if(r!==void 0)return r.fieldTree;let o=$(i.value);if(Qi(o)){if(n==="length")return i.value().length;if(n===Symbol.iterator)return()=>(i.value(),Array.prototype[Symbol.iterator].apply(i.fieldTree))}if(Lu(o)&&n===Symbol.iterator)return function*(){for(let s in e)yield[s,e[s]]}},getOwnPropertyDescriptor(t,n){let e=$(t().value),i=Reflect.getOwnPropertyDescriptor(e,n);return i&&!i.configurable&&(i.configurable=!0),i},ownKeys(t){let n=$(t().value);return typeof n=="object"&&n!==null?Reflect.ownKeys(n):[]}};function EF(t,n){let e=w(()=>t()[n()]);return e[Ne]=t[Ne],e.set=i=>{Object.is($(e),i)||t.update(r=>wF(r,i,n()))},e.update=i=>{e.set(i($(e)))},e.asReadonly=()=>e,e}function wF(t,n,e){if(Qi(t)){let i=[...t];return i[e]=n,i}else return P(p({},t),{[e]:n})}var ds=Symbol(""),pE=w(()=>!1),Hu=class{logic;node;createChildNode;identitySymbol=Symbol();_injector=void 0;_anyChildHasLogic;get injector(){return this._injector??=V.create({providers:[],parent:this.fieldManager.injector}),this._injector}constructor(n,e,i){this.logic=n,this.node=e,this.createChildNode=i}children(){this.ensureChildrenMap();let n=this.childrenMap();return n===void 0?[]:Array.from(n.byPropertyKey.values()).map(e=>$(e.reader))}materializedChildren(){let n=this.childrenMap();return n===void 0?[]:Array.from(n.byPropertyKey.values()).map(e=>e.node)}_areChildrenMaterialized(){return $(this.childrenMap)!==void 0}ensureChildrenMap(){this._areChildrenMaterialized()||$(()=>{this.childrenMap.update(n=>this.computeChildrenMap(this.value(),n,!0))})}getChild(n){this.ensureChildrenMap();let e=n.toString(),i=$(this.childrenMap)?.byPropertyKey.get(e)?.reader;return i||(i=this.createReader(e)),i()}reduceChildren(n,e,i){let r=this.childrenMap();if(!r)return n;let o=n;for(let s of r.byPropertyKey.values()){if(i?.(o))break;o=e($(s.reader),o)}return o}destroy(){this.injector.destroy()}createKeyOrOrphanSignals(n,e,i){if(n==="root")return{keyInParent:gE,isOrphaned:pE};let r=this.parent,o=i,s=w(()=>{if(r.structure.isOrphaned())return ds;let c=r.structure.childrenMap();if(!c)return ds;let u=c.byPropertyKey.get(o);if(u&&u.node===this.node)return o;if(e===void 0)return ds;for(let[f,h]of c.byPropertyKey)if(h.node===this.node)return o=f;return ds}),a=w(()=>s()===ds);return{keyInParent:w(()=>{let c=s();if(c===ds)throw e===void 0?new C(-1902,!1):new C(1904,!1);return c}),isOrphaned:a}}createChildrenMap(){return _i({source:this.value,computation:(n,e)=>this.computeChildrenMap(n,e?.value,!1)})}computeChildrenMap(n,e,i){if(!Lu(n)||!i&&e===void 0&&!(this._anyChildHasLogic??=this.logic.anyChildHasLogic()))return;e??={byPropertyKey:new Map};let r,o=Qi(n);e!==void 0&&(o?r=IF(e,n,this.identitySymbol):r=TF(e,n));for(let s of Object.keys(n)){let a,l=n[s];if(l===void 0){e.byPropertyKey.has(s)&&(r??=p({},e),r.byPropertyKey.delete(s));continue}o&&Lu(l)&&!Qi(l)&&(a=l[this.identitySymbol]??=Symbol(""));let c;a&&(e.byTrackingKey?.has(a)||(r??=p({},e),r.byTrackingKey??=new Map,r.byTrackingKey.set(a,this.createChildNode(s,a,o))),c=(r??e).byTrackingKey.get(a));let u=e.byPropertyKey.get(s);u===void 0?(r??=p({},e),r.byPropertyKey.set(s,{reader:this.createReader(s),node:c??this.createChildNode(s,a,o)})):c&&c!==u.node&&(r??=p({},e),u.node=c)}return r??e}createReader(n){return w(()=>this.childrenMap()?.byPropertyKey.get(n)?.node)}},hv=class extends Hu{fieldManager;value;get parent(){}get root(){return this.node}get pathKeys(){return xF}get keyInParent(){return gE}isOrphaned=pE;childrenMap;constructor(n,e,i,r,o){super(e,n,o),this.fieldManager=i,this.value=r,this.childrenMap=this.createChildrenMap()}},mv=class extends Hu{logic;parent;root;pathKeys;keyInParent;value;childrenMap;isOrphaned;get fieldManager(){return this.root.structure.fieldManager}constructor(n,e,i,r,o,s){super(e,n,s),this.logic=e,this.parent=i,this.root=this.parent.structure.root;let a=this.createKeyOrOrphanSignals("child",r,o);this.isOrphaned=a.isOrphaned,this.keyInParent=a.keyInParent,this.pathKeys=w(()=>[...i.structure.pathKeys(),this.keyInParent()]),this.value=EF(this.parent.structure.value,this.keyInParent),this.childrenMap=this.createChildrenMap(),this.fieldManager.structures.add(this)}};var xF=w(()=>[]),gE=w(()=>{throw new C(1905,!1)});function IF(t,n,e){let i,r=new Set(t.byPropertyKey.keys()),o=t.byTrackingKey&&new Set(t.byTrackingKey.keys());for(let s=0;s<n.length;s++){let a=n[s];r.delete(s.toString()),o&&Lu(a)&&Object.hasOwn(a,e)&&o.delete(a[e])}if(r.size>0){i??=p({},t);for(let s of r)i.byPropertyKey.delete(s)}if(o&&o.size>0){i??=p({},t);for(let s of o)i.byTrackingKey.delete(s)}return i}function TF(t,n){let e;for(let i of t.byPropertyKey.keys())n.hasOwnProperty(i)||(e??=p({},t),e.byPropertyKey.delete(i));return e}var pv=class{node;selfSubmitting=H(!1);submissionErrors;constructor(n){this.node=n,this.submissionErrors=_i({source:this.node.structure.value,computation:()=>[]})}submitting=w(()=>this.selfSubmitting()||(this.node.structure.parent?.submitting()??!1))},bl=class{structure;validationState;metadataState;nodeState;submitState;fieldAdapter;controlValue;_context=void 0;get context(){return this._context??=new uv(this)}fieldProxy=new Proxy(()=>this,DF);pathNode;constructor(n){this.pathNode=n.pathNode,this.fieldAdapter=n.fieldAdapter,this.structure=this.fieldAdapter.createStructure(this,n),this.validationState=this.fieldAdapter.createValidationState(this,n),this.nodeState=this.fieldAdapter.createNodeState(this,n),this.metadataState=new fv(this),this.submitState=new pv(this),this.controlValue=this.controlValueSignal(),this.metadataState.runMetadataCreateLifecycle()}focusBoundControl(n){this.getBindingForFocus()?.focus(n)}getBindingForFocus(){let n=this.formFieldBindings().filter(e=>e.focus!==void 0).reduce(sE,void 0);return n||this.structure.children().map(e=>e.getBindingForFocus()).reduce(sE,void 0)}pendingSync=_i({source:()=>this.value(),computation:(n,e)=>{e?.value?.abort()}});get fieldTree(){return this.fieldProxy}get logicNode(){return this.structure.logic}get value(){return this.structure.value}get keyInParent(){return this.structure.keyInParent}get errors(){return this.validationState.errors}get parseErrors(){return this.validationState.parseErrors}get errorSummary(){return this.validationState.errorSummary}get pending(){return this.validationState.pending}get valid(){return this.validationState.valid}get invalid(){return this.validationState.invalid}get dirty(){return this.nodeState.dirty}get touched(){return this.nodeState.touched}get disabled(){return this.nodeState.disabled}get disabledReasons(){return this.nodeState.disabledReasons}get hidden(){return this.nodeState.hidden}get readonly(){return this.nodeState.readonly}get formFieldBindings(){return this.nodeState.formFieldBindings}get submitting(){return this.submitState.submitting}get name(){return this.nodeState.name}get max(){let n=this.metadata(Dv)?.();return n?this.metadata(n):void 0}get maxLength(){return this.metadata(fE)}get min(){let n=this.metadata(Cv)?.();return n?this.metadata(n):void 0}get minLength(){return this.metadata(uE)}get pattern(){return this.metadata(hE)??MF}get required(){return this.metadata(dE)??NF}metadata(n){return this.metadataState.get(n)}getError(n){return this.errors().find(e=>e.kind===n)}hasMetadata(n){return this.metadataState.has(n)}markAsTouched(n){this.structure.isOrphaned()||$(()=>{this.markAsTouchedInternal(n),this.flushSync()})}markAsTouchedInternal(n){if(!this.structure.isOrphaned()&&!this.validationState.shouldSkipValidation()&&(this.nodeState.markAsTouched(),!n?.skipDescendants))for(let e of this.structure.children())e.markAsTouchedInternal()}markAsDirty(){this.nodeState.markAsDirty()}markAsPristine(){this.nodeState.markAsPristine()}markAsUntouched(){this.nodeState.markAsUntouched()}reset(n){$(()=>this._reset(n))}_reset(n){this.pendingSync()?.abort(),n!==void 0&&this.value.set(n),this.controlValue.rawSet(this.value()),this.nodeState.markAsUntouched(),this.nodeState.markAsPristine();for(let e of this.formFieldBindings())e.reset();for(let e of this.structure.materializedChildren())e._reset()}reloadValidation(){$(()=>this._reloadValidation())}_reloadValidation(){let n=this.logicNode.logic.getMetadataKeys();for(let e of n)e[bv]&&this.metadata(e).reload?.();for(let e of this.structure.children())e._reloadValidation()}controlValueSignal(){let n=_i(this.value);n.rawSet=n.set,n.set=i=>{n.rawSet(i),this.markAsDirty(),this.debounceSync()};let e=n.update;return n.update=i=>{e(i),this.markAsDirty(),this.debounceSync()},n}sync(){this.value.set(this.controlValue())}flushSync(){let n=this.pendingSync();n&&!n.signal.aborted&&(n.abort(),this.sync())}async debounceSync(){let n=$(()=>(this.pendingSync()?.abort(),this.nodeState.debouncer()));if(n){let e=new AbortController,i=n(e.signal);if(i&&(this.pendingSync.set(e),await i,e.signal.aborted))return}this.structure.isOrphaned()||this.sync()}static newRoot(n,e,i,r){return r.newRoot(n,e,i,r)}createStructure(n){return n.kind==="root"?new hv(this,n.logic,n.fieldManager,n.value,this.newChild.bind(this)):new mv(this,n.logic,n.parent,n.identityInParent,n.initialKeyInParent,this.newChild.bind(this))}newChild(n,e,i){let r,o;return i?(r=this.pathNode.getChild(Xi),o=this.structure.logic.getChild(Xi)):(r=this.pathNode.getChild(n),o=this.structure.logic.getChild(n)),this.fieldAdapter.newChild({kind:"child",parent:this,pathNode:r,logic:o,initialKeyInParent:n,identityInParent:e,fieldAdapter:this.fieldAdapter})}},MF=w(()=>[]),NF=w(()=>!1);function sE(t,n){return t?n&&t.element.compareDocumentPosition(n.element)&Node.DOCUMENT_POSITION_PRECEDING?n:t:n}var gv=class{node;selfTouched=H(!1);selfDirty=H(!1);markAsTouched(){this.selfTouched.set(!0)}markAsDirty(){this.selfDirty.set(!0)}markAsPristine(){this.selfDirty.set(!1)}markAsUntouched(){this.selfTouched.set(!1)}formFieldBindings=H([]);constructor(n){this.node=n}dirty=w(()=>{let n=this.selfDirty()&&!this.isNonInteractive();return this.node.structure.reduceChildren(n,(e,i)=>i||e.nodeState.dirty(),iE)});touched=w(()=>{let n=this.selfTouched()&&!this.isNonInteractive();return this.node.structure.reduceChildren(n,(e,i)=>i||e.nodeState.touched(),iE)});disabledReasons=w(()=>[...this.node.structure.parent?.nodeState.disabledReasons()??[],...this.node.logicNode.logic.disabledReasons.compute(this.node.context)],{equal:Kt});disabled=w(()=>!!this.disabledReasons().length);readonly=w(()=>(this.node.structure.parent?.nodeState.readonly()||this.node.logicNode.logic.readonly.compute(this.node.context))??!1);hidden=w(()=>(this.node.structure.parent?.nodeState.hidden()||this.node.logicNode.logic.hidden.compute(this.node.context))??!1);name=w(()=>{let n=this.node.structure.parent;return n?`${n.name()}.${this.node.structure.keyInParent()}`:this.node.structure.fieldManager.rootName});debouncer=w(()=>{if(this.node.logicNode.logic.hasMetadata(dv)){let e=this.node.logicNode.logic.getMetadata(dv).compute(this.node.context);if(e)return i=>e(this.node.context,i)}return this.node.structure.parent?.nodeState.debouncer?.()});isNonInteractive=w(()=>this.hidden()||this.disabled()||this.readonly())},vv=class{newRoot(n,e,i,r){return new bl({kind:"root",fieldManager:n,value:e,pathNode:i,logic:i.builder.build(),fieldAdapter:r})}newChild(n){return new bl(n)}createNodeState(n){return new gv(n)}createValidationState(n){return new cv(n)}createStructure(n,e){return n.createStructure(e)}},_v=class{injector;rootName;submitOptions;constructor(n,e,i){this.injector=n,this.rootName=e??`${this.injector.get(_n)}.form${kF++}`,this.submitOptions=i}structures=new Set;createFieldManagementEffect(n){nt(()=>{let e=new Set;this.markStructuresLive(n,e);for(let i of this.structures)e.has(i)||(this.structures.delete(i),$(()=>i.destroy()))},{injector:this.injector})}markStructuresLive(n,e){e.add(n);for(let i of n.children())this.markStructuresLive(i.structure,e)}},kF=0,vE=new g("");function AF(t){let n,e,i;return t.length===3?[n,e,i]=t:t.length===2?_F(t[1])?[n,e]=t:[n,i]=t:[n]=t,[n,e,i]}function io(...t){let[n,e,i]=AF(t),r=i?.injector??d(V),o=Ve(r,()=>_l.rootCompile(e)),s=new _v(r,i?.name,i?.submission),a=i?.adapter??new vv,l=bl.newRoot(s,n,o,a);s.createFieldManagementEffect(l.structure);let{experimentalWebMcpTool:c}=i??{};if(c){let u=Ve(r,()=>d(vE,{optional:!0}));u&&Ve(r,()=>u(l.fieldTree,{name:c.name,description:c.description}))}return l.fieldTree}function Wu(t,n){ms(t);let e=un.unwrapFieldPath(t).getChild(Xi).fieldPathProxy;_E(e,n)}function _E(t,n){ms(t),un.unwrapFieldPath(t).mergeIn(_l.create(n))}var zu=class{kind="compat";control;fieldTree;context;message;constructor({context:n,kind:e,control:i}){this.context=n,this.kind=e,this.control=i}};function bE(t){if(t.length===0)return null;let n={};for(let e of t)n[e.kind]=e instanceof zu?e.context:e;return n}function yE(t,n){return t===null?[]:Object.entries(t).map(([e,i])=>new zu({context:i,kind:e,control:n}))}var RF=new g("");function EE(t,n){ms(t);let e=un.unwrapFieldPath(t),i;typeof n=="function"||typeof n=="string"?i=n:i=n?.when,e.builder.addDisabledReasonRule(r=>{let o=!0;return typeof i=="string"?o=i:i&&(o=i(r)),typeof o=="string"?{fieldTree:r.fieldTree,message:o}:o?{fieldTree:r.fieldTree}:void 0})}function Gu(t,n){return t instanceof Function?t(n):t}function CE(t){return t===void 0?[]:Array.isArray(t)?t:[t]}function wE(t,n){ms(t),un.unwrapFieldPath(t).builder.addSyncErrorRule(i=>mE(n(i),i.fieldTree))}function OF(t,n){return new wv(t,n)}function PF(t,n){return new xv(t,n)}var yl=class{__brand=void 0;kind="";fieldTree;message;constructor(n){n&&Object.assign(this,n)}};var wv=class extends yl{min;kind="min";constructor(n,e){super(e),this.min=n}};var xv=class extends yl{max;kind="max";constructor(n,e){super(e),this.max=n}};var qu=class extends yl{kind="parse"};function xE(t,n,e){let i=In();no(t,i,r=>{if(!(e?.when&&!e.when(r)))return typeof n=="function"?n(r):n}),no(t,Ev,({state:r})=>r.metadata(i)()),no(t,Dv,()=>Ev),wE(t,r=>{let o=r.value();if(o===null||Number.isNaN(o))return;let s=r.state.metadata(i)();if(!(s===void 0||Number.isNaN(s))&&o>s)return e?.error?Gu(e.error,r):PF(s,{message:Gu(e?.message,r)})})}function IE(t,n,e){let i=In();no(t,i,r=>{if(!(e?.when&&!e.when(r)))return typeof n=="function"?n(r):n}),no(t,Sv,({state:r})=>r.metadata(i)()),no(t,Cv,()=>Sv),wE(t,r=>{let o=r.value();if(o===null||Number.isNaN(o))return;let s=r.state.metadata(i)();if(!(s===void 0||Number.isNaN(s))&&o<s)return e?.error?Gu(e.error,r):OF(s,{message:Gu(e?.message,r)})})}function FF(t,n,e){let i=_i({source:t,computation:()=>[],equal:Kt}),r=s=>{let a=e(s);i.set(CE(a.error)),a.value!==void 0&&n(a.value),i.set(CE(a.error))},o=()=>{i.set([])};return{errors:i.asReadonly(),setRawValue:r,reset:o}}var Iv=class{field;constructor(n){this.field=n}control=this;get value(){return this.field().controlValue()}get valid(){return this.field().valid()}get invalid(){return this.field().invalid()}get pending(){return this.field().pending()}get disabled(){return this.field().disabled()}get enabled(){return!this.field().disabled()}get errors(){return bE(this.field().errors())}get pristine(){return!this.field().dirty()}get dirty(){return this.field().dirty()}get touched(){return this.field().touched()}get untouched(){return!this.field().touched()}get status(){if(this.field().disabled())return"DISABLED";if(this.field().valid())return"VALID";if(this.field().invalid())return"INVALID";if(this.field().pending())return"PENDING";throw new C(1910,!1)}valueAccessor=null;hasValidator(n){return n===Kn.required?this.field().required():!1}updateValueAndValidity(){}},Tv={disabled:"disabled",disabledReasons:"disabledReasons",dirty:"dirty",errors:"errors",hidden:"hidden",invalid:"invalid",max:"max",maxLength:"maxLength",min:"min",minLength:"minLength",name:"name",pattern:"pattern",pending:"pending",readonly:"readonly",required:"required",touched:"touched"},LF=(()=>{let t={};for(let n of Object.keys(Tv))t[Tv[n]]=n;return t})();function Mv(t,n){let e=LF[n];return t[e]?.()}var Nv=Object.values(Tv);function Yu(){return{}}function ro(t,n,e){return t[n]!==e?(t[n]=e,!0):!1}function VF(t,n,e){let i;if(TE(t)&&e.isBadInput(t))return{error:new qu};switch(t.type){case"checkbox":return{value:t.checked};case"number":case"range":case"datetime-local":if(i=$(n),typeof i=="number"||i===null)return{value:t.value===""?null:t.valueAsNumber};break;case"date":case"month":case"time":case"week":if(i=$(n),i===null||i instanceof Date)return{value:t.valueAsDate};if(typeof i=="number")return{value:t.valueAsNumber};break}if(t.tagName==="INPUT"&&t.type==="text"&&(i??=$(n),typeof i=="number"||i===null)){if(t.value==="")return{value:null};let r=Number(t.value);return Number.isNaN(r)?{error:new qu}:{value:r}}return{value:t.value}}function SE(t,n){switch(t.type){case"checkbox":t.checked=n;return;case"radio":t.checked=n===t.value;return;case"number":case"range":case"datetime-local":if(typeof n=="number"){DE(t,n);return}else if(n===null){t.value="";return}break;case"date":case"month":case"time":case"week":if(n===null||n instanceof Date){t.valueAsDate=n;return}else if(typeof n=="number"){DE(t,n);return}}if(t.tagName==="INPUT"&&t.type==="text"){if(typeof n=="number"){t.value=isNaN(n)?"":String(n);return}if(n===null){t.value="";return}}t.value=n}function DE(t,n){isNaN(n)?t.value="":t.valueAsNumber=n}function TE(t){return t.tagName==="INPUT"}function BF(t){return t.type==="date"||t.type==="datetime-local"||t.type==="month"||t.type==="time"||t.type==="week"}function jF(t,n){let e=t.getUTCFullYear(),i=String(t.getUTCMonth()+1).padStart(2,"0");if(n==="month")return`${e}-${i}`;let r=String(t.getUTCDate()).padStart(2,"0");return`${e}-${i}-${r}`}function ME(t,n,e){return n instanceof Date&&(t==="min"||t==="max")&&(e==="date"||e==="month")?jF(n,e):n}function UF(t,n){t.listenToCustomControlModel(i=>n.state().controlValue.set(i)),t.listenToCustomControlOutput("touch",()=>n.state().markAsTouched()),n.registerAsBinding(t.customControl);let e=Yu();return()=>{let i=n.state(),r=i.controlValue();ro(e,"controlValue",r)&&t.setCustomControlModelInput(r);for(let o of Nv){let s;if(o==="errors"?s=n.errors():s=Mv(i,o),ro(e,o,s)&&(t.setInputOnDirectives(o,s),n.elementAcceptsNativeProperty(o)&&!t.customControlHasInput(o))){let a=ME(o,s,n.nativeFormElement.type);fl(n.renderer,n.nativeFormElement,o,a)}}}}function HF(t){return typeof t=="object"&&t!==null}function zF(t,n){let e=Yu();n.controlValueAccessor.registerOnChange(r=>{e.controlValue=r,n.state().controlValue.set(r)}),n.controlValueAccessor.registerOnTouched(()=>n.state().markAsTouched());let i=n.injector.get(to,null,{optional:!0,self:!0});if(i){let r;for(let l of i)HF(l)&&l.registerOnValidatorChange&&(r??=H(0),l.registerOnValidatorChange(()=>{r.update(c=>c+1)}));let o=i.map(l=>typeof l=="function"?l:l.validate.bind(l)),s=Kn.compose(o),a=w(()=>{r?.();let l=s?s(n.interopNgControl.control):null;return yE(l,n.interopNgControl.control)});n.parseErrorsSource.set(a)}return n.registerAsBinding({reset:()=>{let r=n.state().value();e.controlValue=r,$(()=>n.controlValueAccessor.writeValue(r))}}),()=>{let r=n.state(),o=r.controlValue();ro(e,"controlValue",o)&&$(()=>n.controlValueAccessor.writeValue(o));for(let s of Nv){let a=Mv(r,s);if(ro(e,s,a)){let l=t.setInputOnDirectives(s,a);s==="disabled"&&n.controlValueAccessor.setDisabledState?$(()=>n.controlValueAccessor.setDisabledState(a)):!l&&n.elementAcceptsNativeProperty(s)&&fl(n.renderer,n.nativeFormElement,s,a)}}}}function $F(t,n,e){if(typeof MutationObserver!="function")return;let i=new MutationObserver(r=>{r.some(o=>WF(o))&&n()});i.observe(t,{attributes:!0,attributeFilter:["value"],characterData:!0,childList:!0,subtree:!0}),e.onDestroy(()=>i.disconnect())}function WF(t){if(t.type==="childList"||t.type==="characterData"){if(t.target instanceof Comment)return!1;for(let n of t.addedNodes)if(!(n instanceof Comment))return!0;for(let n of t.removedNodes)if(!(n instanceof Comment))return!0;return!1}return t.type==="attributes"&&t.target instanceof HTMLOptionElement}function GF(t,n,e,i){let r=!1,o=n.nativeFormElement,s=FF(()=>n.state().value(),l=>n.state().controlValue.set(l),l=>VF(o,n.state().value,i));e.set(s.errors),n.onReset=()=>{s.reset();let l=n.state().value();a.controlValue=l,SE(o,l)},t.listenToDom("input",()=>s.setRawValue(void 0)),t.listenToDom("blur",()=>n.state().markAsTouched()),TE(o)&&BF(o)&&i.watchValidity(n.destroyRef,o,()=>s.setRawValue(void 0)),n.registerAsBinding(),o.tagName==="SELECT"&&$F(o,()=>{r&&(o.value=n.state().controlValue())},n.destroyRef);let a=Yu();return()=>{let l=n.state();for(let u of Nv){let f=Mv(l,u);if(ro(a,u,f)&&(t.setInputOnDirectives(u,f),n.elementAcceptsNativeProperty(u))){let h=ME(u,f,o.type);fl(n.renderer,o,u,h)}}let c=l.controlValue();ro(a,"controlValue",c)&&SE(o,c),r=!0}}var NE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=se({token:t,factory:e=>qF.\u0275fac(e),providedIn:"root"})}return t})(),qF=(()=>{class t extends NE{document=d(L);cspNonce=d(ui,{optional:!0});injectedStyles=new WeakMap;watchValidity(e,i,r){let o=i.getRootNode();this.injectedStyles.has(o)||this.injectedStyles.set(o,this.createTransitionStyle(o));let s=a=>{let l=a;(l.animationName==="ng-valid"||l.animationName==="ng-invalid")&&r()};i.addEventListener("animationstart",s),e.onDestroy(()=>{i.removeEventListener("animationstart",s)})}isBadInput(e){return e.validity?.badInput??!1}createTransitionStyle(e){let i=this.document.createElement("style");return this.cspNonce&&(i.nonce=this.cspNonce),i.textContent=`
      @keyframes ng-valid {}
      @keyframes ng-invalid {}
      input:valid, textarea:valid {
        animation: ng-valid 0.001s;
      }
      input:invalid, textarea:invalid {
        animation: ng-invalid 0.001s;
      }
    `,e.nodeType===9?e.head?.appendChild(i):e.appendChild(i),i}ngOnDestroy(){this.injectedStyles.get(this.document)?.remove()}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Dt(t)))(r||t)}})();static \u0275prov=se({token:t,factory:t.\u0275fac})}return t})(),YF=Symbol(),ps=new g(""),er=(()=>{class t{field=it.required({alias:"formField"});state=w(()=>this.field()());renderer=d(we);destroyRef=d(He);injector=d(V);element=d(F).nativeElement;elementIsNativeFormElement=nv(this.element);elementAcceptsTextualValues=K0(this.element);_elementAcceptsMinMax;nativeFormElement=this.elementIsNativeFormElement?this.element:void 0;focuser=e=>this.element.focus(e);controlValueAccessors=d(eo,{optional:!0,self:!0});config=d(RF,{optional:!0});validityMonitor=d(NE);parseErrorsSource=H(void 0);_interopNgControl;get interopNgControl(){return this._interopNgControl??=new Iv(this.state)}parseErrors=w(()=>this.parseErrorsSource()?.().map(e=>P(p({},e),{fieldTree:$(this.state).fieldTree,formField:this}))??[],{equal:Kt});errors=w(()=>this.state().errors().filter(e=>!e.formField||e.formField===this),{equal:Kt});isFieldBinding=!1;resetter=()=>{};parseErrorsResetCallback;setParseErrors(e){this.parseErrorsSource.set(e)}set onReset(e){this.parseErrorsResetCallback=e}get onReset(){return this.parseErrorsResetCallback}get controlValueAccessor(){return!this.controlValueAccessors||this.controlValueAccessors.length===0?this.interopNgControl?.valueAccessor??void 0:iv(this.interopNgControl,this.controlValueAccessors)??void 0}installClassBindingEffect(){let e=Object.entries(this.config?.classes??{}).map(([r,o])=>[r,w(()=>o(this))]);if(e.length===0)return;let i=Yu();$o({write:()=>{for(let[r,o]of e){let s=o();ro(i,r,s)&&(s?this.renderer.addClass(this.element,r):this.renderer.removeClass(this.element,r))}}},{injector:this.injector})}focus(e){this.focuser(e)}reset(){this.resetter(),this.parseErrorsResetCallback?.(this.state().value())}registerAsBinding(e){if(this.isFieldBinding)throw new C(1913,!1);this.isFieldBinding=!0,this.installClassBindingEffect(),e?.focus&&(this.focuser=i=>e.focus(i)),e?.reset&&(this.resetter=()=>e.reset()),nt(i=>{let r=this.state();r.nodeState.formFieldBindings.update(o=>[...o,this]),i(()=>{r.nodeState.formFieldBindings.update(o=>o.filter(s=>s!==this))})},{injector:this.injector})}[YF];\u0275ngControlCreate(e){if(!e.hasPassThrough)if(this.controlValueAccessor)this.\u0275ngControlUpdate=zF(e,this);else if(e.customControl)this.\u0275ngControlUpdate=UF(e,this);else if(this.elementIsNativeFormElement)this.\u0275ngControlUpdate=GF(e,this,this.parseErrorsSource,this.validityMonitor);else throw new C(1914,!1)}\u0275ngControlUpdate;elementAcceptsNativeProperty(e){if(!this.elementIsNativeFormElement)return!1;switch(e){case"min":case"max":return this._elementAcceptsMinMax??=Z0(this.element);case"minLength":case"maxLength":return this.elementAcceptsTextualValues;case"disabled":case"required":case"readonly":case"name":return!0;default:return!1}}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","formField",""]],inputs:{field:[1,"formField","field"]},exportAs:["formField"],features:[Re([{provide:ps,useExisting:t},{provide:Zi,useFactory:()=>d(t).interopNgControl},{provide:Q0,useFactory:()=>d(ps,{self:!0})}]),Ud("formField")]})}return t})();var fn=class t{formatHsl(n,e,i){return`hsl(${n}, ${e*100}%, ${i*100}%)`}addHue(n,e){let i=n+e;return i-Math.floor(i/360)*360}hslToRgb(n,e,i){n/=360;let r,o,s;if(e===0)r=o=s=i;else{let a=(u,f,h)=>(h<0&&(h+=1),h>1&&(h-=1),h<.16666666666666666?u+(f-u)*6*h:h<.5?f:h<.6666666666666666?u+(f-u)*(.6666666666666666-h)*6:u),l=i<.5?i*(1+e):i+e-i*e,c=2*i-l;r=a(c,l,n+1/3),o=a(c,l,n),s=a(c,l,n-1/3)}return[Math.round(r*255),Math.round(o*255),Math.round(s*255)]}rgbToHex(n,e,i){return"#"+[n,e,i].map(r=>r.toString(16).padStart(2,"0")).join("")}exportJsonAsText(n,e="data.txt"){let i=JSON.stringify(n,null,2),r=new Blob([i],{type:"text/plain;charset=utf-8"}),o=document.createElement("a");o.href=URL.createObjectURL(r),o.download=e,o.click(),URL.revokeObjectURL(o.href)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})};var Ut=class t{utilsService=d(fn);inc=H(0);state=H({name:"",colors:[]});stateForm=io(this.state,n=>{Wu(n.colors,e=>{Wu(e.shades,i=>{IE(i,0),xE(i,1),EE(i,{when:({valueOf:r})=>r(e.lockShade)})})})});_=nt(()=>{this._updateColors(this.state()),localStorage.setItem("currentColor",JSON.stringify(this.state()))});constructor(){let n=localStorage.getItem("currentColor")||"";if(!n)this.reset();else{let e=JSON.parse(n);this.inc.set(0),this.setColor(e.name||"unknown",(e.colors||[]).map((i,r)=>this._createColor(r===0,i)))}}setColor=(n,e)=>{this.state.set({name:n,colors:[...e]})};reset=()=>{this.inc.set(0),this.setColor("New Palette",[this._createColor(!0)])};addShade=n=>{let e=this.state().colors.find((i,r)=>r===n);e&&this.state.set(P(p({},this.state()),{colors:[...this.state().colors.map((i,r)=>r!=n?i:P(p({},e),{shades:[...e?.shades,.5]}))]}))};addColor=()=>{this.state.set(P(p({},this.state()),{colors:[...this.state().colors,this._createColor(!1)]}))};removeShade=(n,e)=>{let i=this.state().colors.find((r,o)=>o===n);i&&this.state.set(P(p({},this.state()),{colors:[...this.state().colors.map((r,o)=>o!=n?r:P(p({},i),{shades:[...i?.shades.filter((s,a)=>a!=e)]}))]}))};removeColor=n=>{this.state().colors.find((i,r)=>r===n)&&this.state.set(P(p({},this.state()),{colors:[...this.state().colors.filter((i,r)=>r!=n)]}))};_updateColors(n){let e=n.colors,i=e[0];if(e.length>1)for(let r=1;r<e.length;r++){let o=e[r];o.lockHue?o.hue=this.utilsService.addHue(i.hue,o.hueDiff):o.hueDiff=o.hue-i.hue,o.lockSaturation&&(o.saturation=i.saturation),o.lockShade&&(o.shades=[...i.shades])}}_createColor(n=!1,e={}){let r=(()=>{let o=this.inc();return this.inc.set(o+1),o})();return{name:e.name??`Color ${r}`,hue:e.hue??15,hueDiff:e.hueDiff??0,lockHue:e.lockHue??!n,saturation:e.saturation??.52,lockSaturation:e.lockSaturation??!n,shades:e.shades??[.23,.43,.63,.83],lockShade:e.lockShade??!n}}static \u0275fac=function(e){return new(e||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})};var kv=class{_document;_textarea;constructor(n,e){this._document=e;let i=this._textarea=this._document.createElement("textarea"),r=i.style;r.position="fixed",r.top=r.opacity="0",r.left="-999em",i.setAttribute("aria-hidden","true"),i.value=n,i.readOnly=!0,(this._document.fullscreenElement||this._document.body).appendChild(i)}copy(){let n=this._textarea,e=!1;try{if(n){let i=this._document.activeElement;n.select(),n.setSelectionRange(0,n.value.length),e=this._document.execCommand("copy"),i&&i.focus()}}catch{}return e}destroy(){let n=this._textarea;n&&(n.remove(),this._textarea=void 0)}},ZF=(()=>{class t{_document=d(L);copy(e){let i=this.beginCopy(e),r=i.copy();return i.destroy(),r}beginCopy(e){return new kv(e,this._document)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),KF=new g("CDK_COPY_TO_CLIPBOARD_CONFIG"),kE=(()=>{class t{_clipboard=d(ZF);_ngZone=d(T);text="";attempts=1;copied=new W;_pending=new Set;_destroyed=!1;_currentTimeout;constructor(){let e=d(KF,{optional:!0});e&&e.attempts!=null&&(this.attempts=e.attempts)}copy(e=this.attempts){if(e=Math.min(e,50),e>1){let i=e,r=this._clipboard.beginCopy(this.text);this._pending.add(r);let o=()=>{let s=r.copy();!s&&--i&&!this._destroyed?this._currentTimeout=this._ngZone.runOutsideAngular(()=>setTimeout(o,1)):(this._currentTimeout=null,this._pending.delete(r),r.destroy(),this.copied.emit(s))};o()}else this.copied.emit(this._clipboard.copy(this.text))}ngOnDestroy(){this._currentTimeout&&clearTimeout(this._currentTimeout),this._pending.forEach(e=>e.destroy()),this._pending.clear(),this._destroyed=!0}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdkCopyToClipboard",""]],hostBindings:function(i,r){i&1&&z("click",function(){return r.copy()})},inputs:{text:[0,"cdkCopyToClipboard","text"],attempts:[0,"cdkCopyToClipboardAttempts","attempts"]},outputs:{copied:"cdkCopyToClipboardCopied"}})}return t})(),AE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({})}return t})();function Cl(t){return t.buttons===0||t.detail===0}function Sl(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var Av;function RE(){if(Av==null){let t=typeof document<"u"?document.head:null;Av=!!(t&&(t.createShadowRoot||t.attachShadow))}return Av}function Rv(t){if(RE()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Dl(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function vt(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var Ov;try{Ov=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Ov=!1}var me=(()=>{class t{_platformId=d(Vr);isBrowser=this._platformId?uD(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Ov)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var El;function OE(){if(El==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>El=!0}))}finally{El=El||!1}return El}function gs(t){return OE()?t:!!t.capture}function Di(t,n=0){return PE(t)?Number(t):arguments.length===2?n:0}function PE(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function Xt(t){return t instanceof F?t.nativeElement:t}var FE=new g("cdk-input-modality-detector-options"),LE={ignoreKeys:[18,17,224,91,16]},VE=650,Pv={passive:!0,capture:!0},BE=(()=>{class t{_platform=d(me);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Je(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=vt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<VE||(this._modality.next(Cl(e)?"keyboard":"mouse"),this._mostRecentTarget=vt(e))};_onTouchstart=e=>{if(Sl(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=vt(e)};constructor(){let e=d(T),i=d(L),r=d(FE,{optional:!0});if(this._options=p(p({},LE),r),this.modalityDetected=this._modality.pipe(Zs(1)),this.modalityChanged=this.modalityDetected.pipe(Ic()),this._platform.isBrowser){let o=d(Ze).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Pv),o.listen(i,"mousedown",this._onMousedown,Pv),o.listen(i,"touchstart",this._onTouchstart,Pv)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),wl=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(wl||{}),jE=new g("cdk-focus-monitor-default-options"),Zu=gs({passive:!0,capture:!0}),oo=(()=>{class t{_ngZone=d(T);_platform=d(me);_inputModalityDetector=d(BE);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(L);_stopInputModalityDetector=new D;constructor(){let e=d(jE,{optional:!0});this._detectionMode=e?.detectionMode||wl.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=vt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=Xt(e);if(!this._platform.isBrowser||r.nodeType!==1)return q();let o=Rv(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new D,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=Xt(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=Xt(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,l])=>this._originChanged(a,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===wl.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===wl.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?VE:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=vt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Zu),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Zu)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ce(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Zu),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Zu),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var Ku=new WeakMap,Qe=(()=>{class t{_appRef;_injector=d(V);_environmentInjector=d(xe);load(e){let i=this._appRef=this._appRef||this._injector.get(Gt),r=Ku.get(i);r||(r={loaders:new Set,refs:[]},Ku.set(i,r),i.onDestroy(()=>{Ku.get(i)?.refs.forEach(o=>o.destroy()),Ku.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Jd(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var vs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
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
`],encapsulation:2})}return t})(),Xu;function QF(){if(Xu===void 0&&(Xu=null,typeof window<"u")){let t=window;if(t.trustedTypes!==void 0)try{Xu=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n})}catch(n){console.error(n)}}return Xu}function so(t){return QF()?.createHTML(t)||t}function UE(t,n,e){let i=e.sanitize(ot.HTML,n);t.innerHTML=so(i||"")}function _s(t){return Array.isArray(t)?t:[t]}var HE=new Set,ao,bs=(()=>{class t{_platform=d(me);_nonce=d(ui,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):e1}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&JF(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function JF(t,n){if(!HE.has(t))try{ao||(ao=document.createElement("style"),n&&ao.setAttribute("nonce",n),ao.setAttribute("type","text/css"),document.head.appendChild(ao)),ao.sheet&&(ao.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),HE.add(t))}catch(e){console.error(e)}}function e1(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var Fv=(()=>{class t{_mediaMatcher=d(bs);_zone=d(T);_queries=new Map;_destroySubject=new D;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return zE(_s(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=zE(_s(e)).map(s=>this._registerQuery(s).observable),o=Ws(r);return o=Ri(o.pipe($e(1)),o.pipe(Zs(1),qs(0))),o.pipe(re(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:l,query:c})=>{a.matches=a.matches||l,a.breakpoints[c]=l}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ie(s=>{let a=l=>this._zone.run(()=>s.next(l));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(kt(i),re(({matches:s})=>({query:e,matches:s})),Ce(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function zE(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var t1=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var $E=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({providers:[t1]})}return t})();var Vv=(()=>{class t{_platform=d(me);isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return i1(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=n1(u1(e));if(i&&(WE(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=WE(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!c1(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return d1(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function n1(t){try{return t.frameElement}catch{return null}}function i1(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function r1(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function o1(t){return a1(t)&&t.type=="hidden"}function s1(t){return l1(t)&&t.hasAttribute("href")}function a1(t){return t.nodeName.toLowerCase()=="input"}function l1(t){return t.nodeName.toLowerCase()=="a"}function GE(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function WE(t){if(!GE(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function c1(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function d1(t){return o1(t)?!1:r1(t)||s1(t)||t.hasAttribute("contenteditable")||GE(t)}function u1(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var Lv=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,s){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){at(n,{injector:this._injector})}},Bv=(()=>{class t{_checker=d(Vv);_ngZone=d(T);_document=d(L);_injector=d(V);constructor(){d(Qe).load(vs)}create(e,i=!1){return new Lv(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var qE=new g("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),YE=new g("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),f1=0,xl=(()=>{class t{_ngZone=d(T);_defaultOptions=d(YE,{optional:!0});_liveElement;_document=d(L);_sanitizer=d(Wa);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(qE,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:UE(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${f1++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var h1=200,Qu=class{_letterKeyStream=new D;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new D;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:h1;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(rt(e=>this._pressedLetters.push(e)),qs(n),be(()=>this._pressedLetters.length>0),re(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function It(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Ju=class{_items;_activeItemIndex=H(-1);_activeItem=H(null);_wrap=!1;_typeaheadSubscription=ge.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Hr?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Wt(n)&&(this._effectRef=nt(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new D;change=new D;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Qu(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||It(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Wt(this._items)?this._items():this._items instanceof Hr?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Il=class extends Ju{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var QE=new Map,Ue=class t{_appId=d(_n);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=QE.get(n);return i===void 0?i=0:i++,QE.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})};var ew=" ";function p1(t,n,e){let i=tf(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(ew)))}function g1(t,n,e){let i=tf(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(ew)):t.removeAttribute(n)}function tf(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var tw="cdk-describedby-message",ef="cdk-describedby-host",zv=0,nw=(()=>{class t{_platform=d(me);_document=d(L);_messageRegistry=new Map;_messagesContainer=null;_id=`${zv++}`;constructor(){d(Qe).load(vs),this._id=d(_n)+"-"+zv++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=Hv(i,r);typeof i!="string"?(JE(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=Hv(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let s=this._messageRegistry.get(o);s&&s.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${ef}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(ef);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");JE(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(Hv(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=tf(e,"aria-describedby").filter(r=>r.indexOf(tw)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);p1(e,"aria-describedby",r.messageElement.id),e.setAttribute(ef,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,g1(e,"aria-describedby",r.messageElement.id),e.removeAttribute(ef)}_isElementDescribedByMessage(e,i){let r=tf(e,"aria-describedby"),o=this._messageRegistry.get(i),s=o&&o.messageElement.id;return!!s&&r.indexOf(s)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function Hv(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function JE(t,n){t.id||(t.id=`${tw}-${n}-${zv++}`)}var iw={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};function $v(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}function Ye(t){return t==null?"":typeof t=="string"?t:`${t}px`}var v1=new g("cdk-dir-doc",{providedIn:"root",factory:()=>d(L)}),_1=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function rw(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?_1.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var ut=(()=>{class t{get value(){return this.valueSignal()}valueSignal=H("ltr");change=new W;constructor(){let e=d(v1,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(rw(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var Tn=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(Tn||{}),nf,lo;function rf(){if(lo==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return lo=!1,lo;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)lo=!0;else{let t=Element.prototype.scrollTo;t?lo=!/\{\s*\[native code\]\s*\}/.test(t.toString()):lo=!1}}return lo}function ys(){if(typeof document!="object"||!document)return Tn.NORMAL;if(nf==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),nf=Tn.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,nf=t.scrollLeft===0?Tn.NEGATED:Tn.INVERTED),t.remove()}return nf}var Me=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({})}return t})();var b1=20,co=(()=>{class t{_ngZone=d(T);_platform=d(me);_renderer=d(Ze).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new D;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=b1){return this._platform.isBrowser?new ie(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(xc(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):q()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(be(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=Xt(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),Wv=(()=>{class t{elementRef=d(F);scrollDispatcher=d(co);ngZone=d(T);dir=d(ut,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new D;_renderer=d(we);_cleanupScroll;_elementScrolled=new D;ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&ys()!=Tn.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),ys()==Tn.INVERTED?e.left=e.right:ys()==Tn.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;rf()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let s=this.dir&&this.dir.value=="rtl";return e=="start"?e=s?r:i:e=="end"&&(e=s?i:r),s&&ys()==Tn.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:s&&ys()==Tn.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),y1=20,tr=(()=>{class t{_platform=d(me);_listeners;_viewportSize=null;_change=new D;_document=d(L);constructor(){let e=d(T),i=d(Ze).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=y1){return e>0?this._change.pipe(xc(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var Tl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({})}return t})(),Gv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[Me,Tl,Me,Tl]})}return t})();var Ml=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Mn=class extends Ml{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,s){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},Ei=class extends Ml{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},qv=class extends Ml{element;constructor(n){super(),this.element=n instanceof F?n.nativeElement:n}},nr=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Mn)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Ei)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof qv)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},of=class extends nr{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(qn,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||V.NULL,o=r.get(xe,i.injector);e=Jd(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var ir=(()=>{class t extends nr{_moduleRef=d(qn,{optional:!0});_document=d(L);_viewContainerRef=d(Rt);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new W;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Dt(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[Ae]})}return t})(),Nl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({})}return t})();var ow=rf();function Ds(t){return new sf(t.get(tr),t.get(L))}var sf=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Ye(-this._previousScrollPosition.left),n.style.top=Ye(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),ow&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),ow&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function fw(t,n){return new af(t.get(co),t.get(T),t.get(tr),n)}var af=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(be(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var kl=class{enable(){}disable(){}attach(){}};function Yv(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function sw(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function ho(t,n){return new lf(t.get(co),t.get(tr),t.get(T),n)}var lf=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Yv(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},hw=(()=>{class t{_injector=d(V);noop=()=>new kl;close=e=>fw(this._injector,e);block=()=>Ds(this._injector);reposition=e=>ho(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),wi=class{positionStrategy;scrollStrategy=new kl;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var cf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var mw=(()=>{class t{_attachedOverlays=[];_document=d(L);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),pw=(()=>{class t extends mw{_ngZone=d(T);_renderer=d(Ze).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),gw=(()=>{class t extends mw{_platform=d(me);_ngZone=d(T);_renderer=d(Ze).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=vt(e)};_clickListener=e=>{let i=vt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],l=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,l))){if(aw(a.overlayElement,i)||aw(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function aw(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var vw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
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
`],encapsulation:2})}return t})(),ff=(()=>{class t{_platform=d(me);_containerElement;_document=d(L);_styleLoader=d(Qe);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||$v()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),$v()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(vw)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),Zv=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Kv(t){return t&&t.nodeType===1}var Cs=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new D;_attachments=new D;_detachments=new D;_positionStrategy;_scrollStrategy;_locationChanges=ge.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new D;_outsidePointerEvents=new D;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,l,c,u=!1,f,h){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=f,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=at(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=p(p({},this._config),n),this._updateElementSize()}setDirection(n){this._config=P(p({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Ye(this._config.width),n.height=Ye(this._config.height),n.minWidth=Ye(this._config.minWidth),n.minHeight=Ye(this._config.minHeight),n.maxWidth=Ye(this._config.maxWidth),n.maxHeight=Ye(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Kv(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Zv(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=_s(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=at(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},lw="cdk-overlay-connected-position-bounding-box",S1=/([A-Za-z%]+)$/;function Al(t,n){return new df(n,t.get(tr),t.get(L),t.get(me),t.get(ff))}var df=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new D;_resizeSubscription=ge.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(lw),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let l=this._getOriginPoint(n,r,a),c=this._getOverlayPoint(l,e,a),u=this._getOverlayFit(c,e,i,a);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:a,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,a)});continue}(!s||s.overlayFit.visibleArea<u.visibleArea)&&(s={overlayFit:u,overlayPoint:c,originPoint:l,position:a,overlayRect:e})}if(o.length){let a=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,a=c)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&uo(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(lw),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof F?this._origin.nativeElement:Kv(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=dw(e),{x:s,y:a}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(s+=l),c&&(a+=c);let u=0-s,f=s+o.width-i.width,h=0-a,m=a+o.height-i.height,v=this._subtractOverflows(o.width,u,f),S=this._subtractOverflows(o.height,h,m),M=v*S;return{visibleArea:M,isCompletelyWithinViewport:o.width*o.height===M,fitsInViewportVertically:S===o.height,fitsInViewportHorizontally:v==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=cw(this._overlayRef.getConfig().minHeight),a=cw(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||s!=null&&s<=r,c=n.fitsInViewportHorizontally||a!=null&&a<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=dw(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,f=0;return r.width<=o.width?u=c||-s:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=l||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:f},{x:n.x+u,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!D1(this._lastScrollVisibility,i)){let r=new cf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let m=Math.min(i.bottom-n.y+i.top,n.y),v=this._lastBoundingBoxSize.height;o=m*2,s=n.y-m,o>v&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-v/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,f,h;if(c)h=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)f=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let m=Math.min(i.right-n.x+i.left,n.x),v=this._lastBoundingBoxSize.width;u=m*2,f=n.x-m,u>v&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-v/2)}return{top:s,left:f,bottom:a,right:h,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Ye(i.width),r.height=Ye(i.height),r.top=Ye(i.top)||"auto",r.bottom=Ye(i.bottom)||"auto",r.left=Ye(i.left)||"auto",r.right=Ye(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Ye(o)),s&&(r.maxWidth=Ye(s))}this._lastBoundingBoxSize=i,uo(this._boundingBox.style,r)}_resetBoundingBoxStyles(){uo(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){uo(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();uo(i,this._getExactOverlayY(e,n,u)),uo(i,this._getExactOverlayX(e,n,u))}else i.position="static";let a="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(a+=`translateX(${l}px) `),c&&(a+=`translateY(${c}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Ye(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Ye(s.maxWidth):o&&(i.maxWidth="")),uo(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Ye(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Ye(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:sw(n,i),isOriginOutsideView:Yv(n,i),isOverlayClipped:sw(e,i),isOverlayOutsideView:Yv(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&_s(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof F)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function uo(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function cw(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(S1);return!e||e==="px"?parseFloat(n):null}return t||null}function dw(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function D1(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var uw="cdk-global-overlay-wrapper";function rr(t){return new uf}var uf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(uw),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,l=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),c=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),u=this._xPosition,f=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",m="",v="",S="";l?S="flex-start":u==="center"?(S="center",h?v=f:m=f):h?u==="left"||u==="end"?(S="flex-end",m=f):(u==="right"||u==="start")&&(S="flex-start",v=f):u==="left"||u==="start"?(S="flex-start",m=f):(u==="right"||u==="end")&&(S="flex-end",v=f),n.position=this._cssPosition,n.marginLeft=l?"0":m,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":v,e.justifyContent=S,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(uw),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},_w=(()=>{class t{_injector=d(V);global(){return rr()}flexibleConnectedTo(e){return Al(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),Rl=new g("OVERLAY_DEFAULT_CONFIG");function xi(t,n){t.get(Qe).load(vw);let e=t.get(ff),i=t.get(L),r=t.get(Ue),o=t.get(Gt),s=t.get(ut),a=t.get(we,null,{optional:!0})||t.get(Ze).createRenderer(null,null),l=new wi(n),c=t.get(Rl,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||s.value,!i.body||!("showPopover"in i.body)?l.usePopover=!1:l.usePopover=n?.usePopover??c;let u=i.createElement("div"),f=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),f.appendChild(u),l.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let h=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return Kv(h)?h.after(f):h?.type==="parent"?h.element.appendChild(f):e.getContainerElement().appendChild(f),new Cs(new of(u,o,t),f,u,l,t.get(T),t.get(pw),i,t.get(Wi),t.get(gw),n?.disableAnimations??t.get(ma,null,{optional:!0})==="NoopAnimations",t.get(xe),a)}var bw=(()=>{class t{scrollStrategies=d(hw);_positionBuilder=d(_w);_injector=d(V);create(e){return xi(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),E1=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],w1=new g("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(V);return()=>ho(t)}}),Ss=(()=>{class t{elementRef=d(F);static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),yw=new g("cdk-connected-overlay-default-config"),hf=(()=>{class t{_dir=d(ut,{optional:!0});_injector=d(V);_overlayRef;_templatePortal;_backdropSubscription=ge.EMPTY;_attachSubscription=ge.EMPTY;_detachSubscription=ge.EMPTY;_positionSubscription=ge.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(w1);_ngZone=d(T);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new W;positionChange=new W;attach=new W;detach=new W;overlayKeydown=new W;overlayOutsideClick=new W;constructor(){let e=d(jt),i=d(Rt),r=d(yw,{optional:!0}),o=d(Rl,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Ei(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=E1);let e=this._overlayRef=xi(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!It(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=vt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new wi({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=Al(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Ss?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Ss?this.origin.elementRef.nativeElement:this.origin instanceof F?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(fh(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",ne],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",ne],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",ne],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",ne],push:[2,"cdkConnectedOverlayPush","push",ne],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",ne],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",ne],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[qe]})}return t})(),Es=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({providers:[bw],imports:[Me,Nl,Gv,Gv]})}return t})();var ws,Cw=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Xv(){if(ws)return ws;if(typeof document!="object"||!document)return ws=new Set(Cw),ws;let t=document.createElement("input");return ws=new Set(Cw.filter(n=>(t.setAttribute("type",n),t.type===n))),ws}var x1=new g("MATERIAL_ANIMATIONS"),Sw=null;function I1(){return d(x1,{optional:!0})?.animationsDisabled||d(ma,{optional:!0})==="NoopAnimations"?"di-disabled":(Sw??=d(bs).matchMedia("(prefers-reduced-motion)").matches,Sw?"reduced-motion":"enabled")}function Oe(){return I1()!=="enabled"}function Ii(t){return t!=null&&`${t}`!="false"}var Lt=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(Lt||{}),Qv=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Lt.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},Dw=gs({passive:!0,capture:!0}),Jv=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,Dw)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,Dw)))}_delegateEventHandler=n=>{let e=vt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Ol={enterDuration:225,exitDuration:150},T1=800,Ew=gs({passive:!0,capture:!0}),ww=["mousedown","touchstart"],xw=["mouseup","mouseleave","touchend","touchcancel"],M1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
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
`],encapsulation:2})}return t})(),Pl=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Jv;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=Xt(i)),o&&o.get(Qe).load(M1)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=p(p({},Ol),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||N1(n,e,r),a=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${a-s}px`,u.style.top=`${l-s}px`,u.style.height=`${s*2}px`,u.style.width=`${s*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let f=window.getComputedStyle(u),h=f.transitionProperty,m=f.transitionDuration,v=h==="none"||m==="0s"||m==="0s, 0s"||r.width===0&&r.height===0,S=new Qv(this,u,i,v);u.style.transform="scale3d(1, 1, 1)",S.state=Lt.FADING_IN,i.persistent||(this._mostRecentTransientRipple=S);let M=null;return!v&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let O=()=>{M&&(M.fallbackTimer=null),clearTimeout(ye),this._finishRippleTransition(S)},pe=()=>this._destroyRipple(S),ye=setTimeout(pe,c+100);u.addEventListener("transitionend",O),u.addEventListener("transitioncancel",pe),M={onTransitionEnd:O,onTransitionCancel:pe,fallbackTimer:ye}}),this._activeRipples.set(S,M),(v||!c)&&this._finishRippleTransition(S),S}fadeOutRipple(n){if(n.state===Lt.FADING_OUT||n.state===Lt.HIDDEN)return;let e=n.element,i=p(p({},Ol),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=Lt.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=Xt(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,ww.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{xw.forEach(e=>{this._triggerElement.addEventListener(e,this,Ew)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Lt.FADING_IN?this._startFadeOutTransition(n):n.state===Lt.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=Lt.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Lt.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=Cl(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+T1;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Sl(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===Lt.VISIBLE||n.config.terminateOnPointerUp&&n.state===Lt.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(ww.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(xw.forEach(e=>n.removeEventListener(e,this,Ew)),this._pointerUpEventsRegistered=!1))}};function N1(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Fl=new g("mat-ripple-global-options"),mo=(()=>{class t{_elementRef=d(F);_animationsDisabled=Oe();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(T),i=d(me),r=d(Fl,{optional:!0}),o=d(V);this._globalOptions=r||{},this._rippleRenderer=new Pl(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:p(p(p({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,p(p({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,p(p({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&ee("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var k1={capture:!0},A1=["focus","mousedown","mouseenter","touchstart"],e_="mat-ripple-loader-uninitialized",t_="mat-ripple-loader-class-name",Iw="mat-ripple-loader-centered",mf="mat-ripple-loader-disabled",Tw=(()=>{class t{_document=d(L);_animationsDisabled=Oe();_globalRippleOptions=d(Fl,{optional:!0});_platform=d(me);_ngZone=d(T);_injector=d(V);_eventCleanups;_hosts=new Map;constructor(){let e=d(Ze).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>A1.map(i=>e.listen(this._document,i,this._onInteraction,k1)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(e_,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(t_))&&e.setAttribute(t_,i.className||""),i.centered&&e.setAttribute(Iw,""),i.disabled&&e.setAttribute(mf,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(mf,""):e.removeAttribute(mf)}_onInteraction=e=>{let i=vt(e);if(i instanceof HTMLElement){let r=i.closest(`[${e_}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(t_)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Ol.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Ol.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(mf),rippleConfig:{centered:e.hasAttribute(Iw),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},l=new Pl(a,this._ngZone,i,this._platform,this._injector),c=!a.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:l,hasSetUpEvents:c}),e.removeAttribute(e_)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var or=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
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
`],encapsulation:2})}return t})();var R1=["*",[["","progressIndicator",""]]],O1=["*","[progressIndicator]"];function P1(t,n){t&1&&(mt(0,"div",1),le(1,1),Et())}var F1=new g("MAT_BUTTON_CONFIG");function Mw(t){return t==null?void 0:xt(t)}var n_=(()=>{class t{_elementRef=d(F);_ngZone=d(T);_animationsDisabled=Oe();_config=d(F1,{optional:!0});_focusMonitor=d(oo);_cleanupClick;_renderer=d(we);_rippleLoader=d(Tw);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=it(!1,{transform:ne});constructor(){d(Qe).load(or);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(_e("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),pt(r.color?"mat-"+r.color:""),ee("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",ne],disabled:[2,"disabled","disabled",ne],ariaDisabled:[2,"aria-disabled","ariaDisabled",ne],disabledInteractive:[2,"disabledInteractive","disabledInteractive",ne],tabIndex:[2,"tabIndex","tabIndex",Mw],_tabindex:[2,"tabindex","_tabindex",Mw],showProgress:[1,"showProgress"]}})}return t})(),Ll=(()=>{class t extends n_{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[Ae],ngContentSelectors:O1,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Te(R1),sn(0,"span",0),le(1),Y(2,P1,2,0,"div",1),sn(3,"span",2)(4,"span",3)),i&2&&(y(2),Z(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
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
`],encapsulation:2})}return t})();var xs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[Me]})}return t})();var L1=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],V1=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function B1(t,n){t&1&&(mt(0,"div",2),le(1,3),Et())}var Nw=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),hn=(()=>{class t extends n_{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=j1(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?Nw.get(this._appearance):null,o=Nw.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Ae],ngContentSelectors:V1,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Te(L1),sn(0,"span",0),le(1),mt(2,"span",1),le(3,1),Et(),le(4,2),Y(5,B1,2,0,"div",2),sn(6,"span",3)(7,"span",4)),i&2&&(ee("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),y(5),Z(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
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
`],encapsulation:2})}return t})();function j1(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Vl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[xs,Me]})}return t})();function U1(t,n){if(t&1){let e=Pt();_(0,"div",1)(1,"button",2),z("click",function(){Ct(e);let r=G();return St(r.action())}),R(2),b()()}if(t&2){let e=G();y(2),Yt(" ",e.data.action," ")}}var H1=["label"];function z1(t,n){}var $1=Math.pow(2,31)-1,Bl=class{_overlayRef;instance;containerInstance;_afterDismissed=new D;_afterOpened=new D;_onAction=new D;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,$1))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},Aw=new g("MatSnackBarData"),Is=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},W1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),G1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),q1=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),Y1=(()=>{class t{snackBarRef=d(Bl);data=d(Aw);action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(_(0,"div",0),R(1),b(),Y(2,U1,3,1,"div",1)),i&2&&(y(),Yt(" ",r.data.message,`
`),y(),Z(r.hasAction?2:-1))},dependencies:[hn,W1,G1,q1],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2})}return t})(),i_="_mat-snack-bar-enter",r_="_mat-snack-bar-exit",Z1=(()=>{class t extends nr{_ngZone=d(T);_elementRef=d(F);_changeDetectorRef=d(Le);_platform=d(me);_animationsDisabled=Oe();snackBarConfig=d(Is);_document=d(L);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(V);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new D;_onExit=new D;_onEnter=new D;_animationState="void";_live;_label;_role;_liveElementId=d(Ue).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===r_?this._completeExit():e===i_&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?at(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(i_)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(i_)},200)))}exit(){return this._destroyed?q(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?at(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(r_)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(r_),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&Fe(ir,7)(H1,7),i&2){let o;Q(o=J())&&(r._portalOutlet=o.first),Q(o=J())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&z("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&ee("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[Ae],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(_(0,"div",1)(1,"div",2,0)(3,"div",3),Ot(4,z1,0,0,"ng-template",4),b(),A(5,"div"),b()()),i&2&&(y(5),_e("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[ir],styles:[`@keyframes _mat-snack-bar-enter {
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
`],encapsulation:2,changeDetection:1})}return t})(),K1=new g("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Is}),Ts=(()=>{class t{_live=d(xl);_injector=d(V);_breakpointObserver=d(Fv);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(K1);_animationsDisabled=Oe();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=Y1;snackBarContainerComponent=Z1;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=p(p({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=V.create({parent:r||this._injector,providers:[{provide:Is,useValue:i}]}),s=new Mn(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=p(p(p({},new Is),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new Bl(s,o);if(e instanceof jt){let l=new Ei(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(l)}else{let l=this._createInjector(r,a),c=new Mn(e,void 0,l),u=s.attachComponentPortal(c);a.instance=u.instance}return this._breakpointObserver.observe(iw.HandsetPortrait).pipe(Ce(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new wi;i.direction=e.direction;let r=rr(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,xi(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return V.create({parent:r||this._injector,providers:[{provide:Bl,useValue:i},{provide:Aw,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var X1=["tooltip"],Q1=20;var J1=new g("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(V);return()=>ho(t,{scrollThrottle:Q1})}}),eL=new g("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var Rw="tooltip-panel",tL={passive:!0},nL=8,iL=8,rL=24,oL=200,o_=(()=>{class t{_elementRef=d(F);_ngZone=d(T);_platform=d(me);_ariaDescriber=d(nw);_focusMonitor=d(oo);_dir=d(ut);_injector=d(V);_viewContainerRef=d(Rt);_mediaMatcher=d(bs);_document=d(L);_renderer=d(we);_animationsDisabled=Oe();_defaultOptions=d(eL,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=Ow;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=Ii(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=Ii(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=Di(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=Di(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new D;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=nL}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(Ce(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new Mn(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(Ce(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let s=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&s._origin instanceof F)return this._overlayRef;this._detach()}let i=this._injector.get(co).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${Rw}`,o=Al(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(Ce(this._destroyed)).subscribe(s=>{this._updateCurrentPositionClass(s.connectionPair),this._tooltipInstance&&s.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=xi(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(J1)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(Ce(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(Ce(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(Ce(this._destroyed)).subscribe(s=>{s.preventDefault(),s.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(Ce(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(p(p({},r.main),o.main)),this._addOffset(p(p({},r.fallback),o.fallback))])}_addOffset(e){let i=iL,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:s}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:s}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:s}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:s}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),at(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,s;if(i==="center"?this._dir&&this._dir.value==="rtl"?s=r==="end"?"left":"right":s=r==="start"?"left":"right":s=i==="bottom"&&o==="top"?"above":"below",s!==this._currentPosition){let a=this._overlayRef;if(a){let l=`${this._cssClassPrefix}-${Rw}-`;a.removePanelClass(l+this._currentPosition),a.addPanelClass(l+s)}this._currentPosition=s}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,tL))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||at({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!It(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&ee("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),Ow=(()=>{class t{_changeDetectorRef=d(Le);_elementRef=d(F);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Oe();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new D;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>rL&&e.width>=oL}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let s=getComputedStyle(i);(s.getPropertyValue("animation-duration")==="0s"||s.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&Fe(X1,7),i&2){let o;Q(o=J())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&z("mouseleave",function(s){return r._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(mt(0,"div",1,0),zd("animationend",function(s){return r._handleAnimationEnd(s)}),mt(2,"div",2),R(3),Et()()),i&2&&(pt(r.tooltipClass),ee("mdc-tooltip--multiline",r._isMultiline),y(3),wt(r.message))},styles:[`.mat-mdc-tooltip {
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
`],encapsulation:2})}return t})();var Ms=class t{utilsService=d(fn);snackBar=d(Ts);hue=it.required();saturation=it.required();light=it.required();color=w(()=>this.utilsService.formatHsl(this.hue(),this.saturation(),this.light()));hexColor=w(()=>{let n=this.utilsService.hslToRgb(this.hue(),this.saturation(),this.light());return this.utilsService.rgbToHex(n[0],n[1],n[2])});onCopied(){this.snackBar.open("Color copied to clipboard","Close",{duration:5e3})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-copy"]],hostVars:2,hostBindings:function(e,i){e&2&&an("background-color",i.color())},inputs:{hue:[1,"hue"],saturation:[1,"saturation"],light:[1,"light"]},decls:1,vars:2,consts:[[3,"cdkCopyToClipboardCopied","cdkCopyToClipboard","matTooltip"]],template:function(e,i){e&1&&(_(0,"button",0),z("cdkCopyToClipboardCopied",function(){return i.onCopied()}),b()),e&2&&k("cdkCopyToClipboard",i.hexColor())("matTooltip",i.hexColor())},dependencies:[AE,kE,o_],styles:["[_nghost-%COMP%]{height:5vw;width:5vw;display:block}button[_ngcontent-%COMP%]{height:100%;width:100%;background:none;border:0;cursor:pointer}button[_ngcontent-%COMP%]:hover + .tooltip[_ngcontent-%COMP%]{opacity:1}.tooltip[_ngcontent-%COMP%]{pointer-events:none;opacity:0;transition:opacity .4s}"]})};var pf=class t{utilsService=d(fn);hue=it.required();saturation=it.required();light=it.required();position=w(()=>{let n=Math.PI/180*this.hue(),e=Math.cos(n),i=Math.sin(n),r=this.light(),o=0;return[r*e-o*i,r*i+o*e]});left=w(()=>{var n=(this.position()[0]+1)*50;return n+"%"});top=w(()=>{var n=(this.position()[1]+1)*50;return n+"%"});static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-pointer"]],hostVars:4,hostBindings:function(e,i){e&2&&an("left",i.left())("top",i.top())},inputs:{hue:[1,"hue"],saturation:[1,"saturation"],light:[1,"light"]},decls:1,vars:3,consts:[[3,"hue","saturation","light"]],template:function(e,i){e&1&&A(0,"pbu-color-copy",0),e&2&&k("hue",i.hue())("saturation",i.saturation())("light",i.light())},dependencies:[Ms],styles:["[_nghost-%COMP%]{border-radius:50%;width:30px;height:30px;display:block;border:2px solid #fff;outline:1px solid #cacaca;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%)}pbu-color-copy[_ngcontent-%COMP%]{height:100%;width:100%;border-radius:50%}"]})};var sL=["colorWheel"];function aL(t,n){if(t&1&&A(0,"pbu-color-pointer",1),t&2){let e=n.$implicit,i=G().$implicit;k("hue",i.hue)("saturation",i.saturation)("light",e)}}function lL(t,n){if(t&1&&ct(0,aL,1,3,"pbu-color-pointer",1,Sn),t&2){let e=n.$implicit;dt(e.shades)}}var Pw=.6,gf=class t{canvasBinding;ctx;colorState=d(Ut);hostElement=d(F);stateForm=this.colorState.stateForm;colors=w(()=>this.colorState.state().colors);radius=H(0);cx=w(()=>this.radius());cy=w(()=>this.radius());constructor(){nt(()=>{this.draw(Pw)})}onWindowResize(){this.resizeCanvas()}ngAfterViewInit(){let n=this.canvasBinding.nativeElement;this.ctx=n.getContext("2d"),this.resizeCanvas()}resizeCanvas(){let n=this.canvasBinding.nativeElement,e=this.hostElement.nativeElement;n.width=e.clientWidth,n.height=e.clientHeight,this.radius.set(n.width/2),this.draw(Pw)}draw(n){if(this.ctx!=null)for(let e=0;e<360;e++){let i=(e-1)*Math.PI/180,r=(e+1)*Math.PI/180;this.ctx.beginPath(),this.ctx.moveTo(this.cx(),this.cy()),this.ctx.arc(this.cx(),this.cy(),this.radius(),i,r),this.ctx.closePath();let o=this.ctx.createRadialGradient(this.cx(),this.cy(),0,this.cx(),this.cy(),this.radius());o.addColorStop(1,`hsl(${e}, ${Math.round(n*100)}%, 50%)`),o.addColorStop(0,`hsl(${e}, ${Math.round(n*100)}%, 50%)`),this.ctx.fillStyle=o,this.ctx.fill()}}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-wheel"]],viewQuery:function(e,i){if(e&1&&Fe(sL,5),e&2){let r;Q(r=J())&&(i.canvasBinding=r.first)}},hostBindings:function(e,i){e&1&&z("resize",function(){return i.onWindowResize()},pp)},decls:4,vars:0,consts:[["colorWheel",""],[3,"hue","saturation","light"]],template:function(e,i){e&1&&(A(0,"canvas",null,0),ct(2,lL,2,0,null,null,Sn)),e&2&&(y(2),dt(i.colors()))},dependencies:[pf],styles:["[_nghost-%COMP%]{position:relative;width:100%;max-width:70vh;aspect-ratio:1}canvas[_ngcontent-%COMP%]{width:100%;height:100%}"]})};var cL=["knob"],dL=["valueIndicatorContainer"];function uL(t,n){if(t&1&&(_(0,"div",2,1)(2,"div",5)(3,"span",6),R(4),b()()()),t&2){let e=G();y(4),wt(e.valueIndicatorText)}}var fL=["trackActive"],hL=["*"];function mL(t,n){if(t&1&&A(0,"div"),t&2){let e=n.$implicit,i=n.$index,r=G(3);pt(e===0?"mdc-slider__tick-mark--active":"mdc-slider__tick-mark--inactive"),an("transform",r._calcTickMarkTransform(i))}}function pL(t,n){if(t&1&&ct(0,mL,1,4,"div",8,Sn),t&2){let e=G(2);dt(e._tickMarks)}}function gL(t,n){if(t&1&&(_(0,"div",6,1),Y(2,pL,2,0),b()),t&2){let e=G();y(2),Z(e._cachedWidth?2:-1)}}function vL(t,n){if(t&1&&A(0,"mat-slider-visual-thumb",7),t&2){let e=G();k("discrete",e.discrete)("thumbPosition",1)("valueIndicatorText",e.startValueIndicatorText)}}var oe=(function(t){return t[t.START=1]="START",t[t.END=2]="END",t})(oe||{}),Ns=(function(t){return t[t.ACTIVE=0]="ACTIVE",t[t.INACTIVE=1]="INACTIVE",t})(Ns||{}),s_=new g("_MatSlider"),Fw=new g("_MatSliderThumb"),_L=new g("_MatSliderRangeThumb"),Lw=new g("_MatSliderVisualThumb");var bL=(()=>{class t{_cdr=d(Le);_ngZone=d(T);_slider=d(s_);_renderer=d(we);_listenerCleanups;discrete=!1;thumbPosition;valueIndicatorText;_ripple;_knob;_valueIndicatorContainer;_sliderInput;_sliderInputEl;_hoverRippleRef;_focusRippleRef;_activeRippleRef;_isHovered=!1;_isActive=!1;_isValueIndicatorVisible=!1;_hostElement=d(F).nativeElement;_platform=d(me);ngAfterViewInit(){let e=this._slider._getInput(this.thumbPosition);e&&(this._ripple.radius=24,this._sliderInput=e,this._sliderInputEl=this._sliderInput._hostElement,this._ngZone.runOutsideAngular(()=>{let i=this._sliderInputEl,r=this._renderer;this._listenerCleanups=[r.listen(i,"pointermove",this._onPointerMove),r.listen(i,"pointerdown",this._onDragStart),r.listen(i,"pointerup",this._onDragEnd),r.listen(i,"pointerleave",this._onMouseLeave),r.listen(i,"focus",this._onFocus),r.listen(i,"blur",this._onBlur)]}))}ngOnDestroy(){this._listenerCleanups?.forEach(e=>e())}_onPointerMove=e=>{if(this._sliderInput._isFocused)return;let i=this._hostElement.getBoundingClientRect(),r=this._slider._isCursorOnSliderThumb(e,i);this._isHovered=r,r?this._showHoverRipple():this._hideRipple(this._hoverRippleRef)};_onMouseLeave=()=>{this._isHovered=!1,this._hideRipple(this._hoverRippleRef)};_onFocus=()=>{this._hideRipple(this._hoverRippleRef),this._showFocusRipple(),this._hostElement.classList.add("mdc-slider__thumb--focused")};_onBlur=()=>{this._isActive||this._hideRipple(this._focusRippleRef),this._isHovered&&this._showHoverRipple(),this._hostElement.classList.remove("mdc-slider__thumb--focused")};_onDragStart=e=>{e.button===0&&(this._isActive=!0,this._showActiveRipple())};_onDragEnd=()=>{this._isActive=!1,this._hideRipple(this._activeRippleRef),this._sliderInput._isFocused||this._hideRipple(this._focusRippleRef),this._platform.SAFARI&&this._showHoverRipple()};_showHoverRipple(){this._isShowingRipple(this._hoverRippleRef)||(this._hoverRippleRef=this._showRipple({enterDuration:0,exitDuration:0}),this._hoverRippleRef?.element.classList.add("mat-mdc-slider-hover-ripple"))}_showFocusRipple(){this._isShowingRipple(this._focusRippleRef)||(this._focusRippleRef=this._showRipple({enterDuration:0,exitDuration:0},!0),this._focusRippleRef?.element.classList.add("mat-mdc-slider-focus-ripple"))}_showActiveRipple(){this._isShowingRipple(this._activeRippleRef)||(this._activeRippleRef=this._showRipple({enterDuration:225,exitDuration:400}),this._activeRippleRef?.element.classList.add("mat-mdc-slider-active-ripple"))}_isShowingRipple(e){return e?.state===Lt.FADING_IN||e?.state===Lt.VISIBLE}_showRipple(e,i){if(!this._slider.disabled&&(this._showValueIndicator(),this._slider._isRange&&this._slider._getThumb(this.thumbPosition===oe.START?oe.END:oe.START)._showValueIndicator(),!(this._slider._globalRippleOptions?.disabled&&!i)))return this._ripple.launch({animation:this._slider._noopAnimations?{enterDuration:0,exitDuration:0}:e,centered:!0,persistent:!0})}_hideRipple(e){if(e?.fadeOut(),this._isShowingAnyRipple())return;this._slider._isRange||this._hideValueIndicator();let i=this._getSibling();i._isShowingAnyRipple()||(this._hideValueIndicator(),i._hideValueIndicator())}_showValueIndicator(){this._hostElement.classList.add("mdc-slider__thumb--with-indicator")}_hideValueIndicator(){this._hostElement.classList.remove("mdc-slider__thumb--with-indicator")}_getSibling(){return this._slider._getThumb(this.thumbPosition===oe.START?oe.END:oe.START)}_getValueIndicatorContainer(){return this._valueIndicatorContainer?.nativeElement}_getKnob(){return this._knob.nativeElement}_isShowingAnyRipple(){return this._isShowingRipple(this._hoverRippleRef)||this._isShowingRipple(this._focusRippleRef)||this._isShowingRipple(this._activeRippleRef)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-slider-visual-thumb"]],viewQuery:function(i,r){if(i&1&&Fe(mo,5)(cL,5)(dL,5),i&2){let o;Q(o=J())&&(r._ripple=o.first),Q(o=J())&&(r._knob=o.first),Q(o=J())&&(r._valueIndicatorContainer=o.first)}},hostAttrs:[1,"mdc-slider__thumb","mat-mdc-slider-visual-thumb"],inputs:{discrete:"discrete",thumbPosition:"thumbPosition",valueIndicatorText:"valueIndicatorText"},features:[Re([{provide:Lw,useExisting:t}])],decls:4,vars:2,consts:[["knob",""],["valueIndicatorContainer",""],[1,"mdc-slider__value-indicator-container"],[1,"mdc-slider__thumb-knob"],["matRipple","",1,"mat-focus-indicator",3,"matRippleDisabled"],[1,"mdc-slider__value-indicator"],[1,"mdc-slider__value-indicator-text"]],template:function(i,r){i&1&&(Y(0,uL,5,1,"div",2),A(1,"div",3,0)(3,"div",4)),i&2&&(Z(r.discrete?0:-1),y(3),k("matRippleDisabled",!0))},dependencies:[mo],styles:[`.mat-mdc-slider-visual-thumb .mat-ripple {
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
`],encapsulation:2})}return t})(),Vw=(()=>{class t{_ngZone=d(T);_cdr=d(Le);_elementRef=d(F);_dir=d(ut,{optional:!0});_globalRippleOptions=d(Fl,{optional:!0});_trackActive;_thumbs;_input;_inputs;get disabled(){return this._disabled}set disabled(e){this._disabled=e;let i=this._getInput(oe.END),r=this._getInput(oe.START);i&&(i.disabled=this._disabled),r&&(r.disabled=this._disabled)}_disabled=!1;get discrete(){return this._discrete}set discrete(e){this._discrete=e,this._updateValueIndicatorUIs()}_discrete=!1;get showTickMarks(){return this._showTickMarks}set showTickMarks(e){this._showTickMarks=e,this._hasViewInitialized&&(this._updateTickMarkUI(),this._updateTickMarkTrackUI())}_showTickMarks=!1;get min(){return this._min}set min(e){let i=e==null||isNaN(e)?this._min:e;this._min!==i&&this._updateMin(i)}_min=0;color;disableRipple=!1;_updateMin(e){let i=this._min;this._min=e,this._isRange?this._updateMinRange({old:i,new:e}):this._updateMinNonRange(e),this._onMinMaxOrStepChange()}_updateMinRange(e){let i=this._getInput(oe.END),r=this._getInput(oe.START),o=i.value,s=r.value;r.min=e.new,i.min=Math.max(e.new,r.value),r.max=Math.min(i.max,i.value),r._updateWidthInactive(),i._updateWidthInactive(),e.new<e.old?this._onTranslateXChangeBySideEffect(i,r):this._onTranslateXChangeBySideEffect(r,i),o!==i.value&&this._onValueChange(i),s!==r.value&&this._onValueChange(r)}_updateMinNonRange(e){let i=this._getInput(oe.END);if(i){let r=i.value;i.min=e,i._updateThumbUIByValue(),this._updateTrackUI(i),r!==i.value&&this._onValueChange(i)}}get max(){return this._max}set max(e){let i=e==null||isNaN(e)?this._max:e;this._max!==i&&this._updateMax(i)}_max=100;_updateMax(e){let i=this._max;this._max=e,this._isRange?this._updateMaxRange({old:i,new:e}):this._updateMaxNonRange(e),this._onMinMaxOrStepChange()}_updateMaxRange(e){let i=this._getInput(oe.END),r=this._getInput(oe.START),o=i.value,s=r.value;i.max=e.new,r.max=Math.min(e.new,i.value),i.min=r.value,i._updateWidthInactive(),r._updateWidthInactive(),e.new>e.old?this._onTranslateXChangeBySideEffect(r,i):this._onTranslateXChangeBySideEffect(i,r),o!==i.value&&this._onValueChange(i),s!==r.value&&this._onValueChange(r)}_updateMaxNonRange(e){let i=this._getInput(oe.END);if(i){let r=i.value;i.max=e,i._updateThumbUIByValue(),this._updateTrackUI(i),r!==i.value&&this._onValueChange(i)}}get step(){return this._step}set step(e){let i=isNaN(e)?this._step:e;this._step!==i&&this._updateStep(i)}_step=1;_updateStep(e){this._step=e,this._isRange?this._updateStepRange():this._updateStepNonRange(),this._onMinMaxOrStepChange()}_updateStepRange(){let e=this._getInput(oe.END),i=this._getInput(oe.START),r=e.value,o=i.value,s=i.value;e.min=this._min,i.max=this._max,e.step=this._step,i.step=this._step,this._platform.SAFARI&&(e.value=e.value,i.value=i.value),e.min=Math.max(this._min,i.value),i.max=Math.min(this._max,e.value),i._updateWidthInactive(),e._updateWidthInactive(),e.value<s?this._onTranslateXChangeBySideEffect(i,e):this._onTranslateXChangeBySideEffect(e,i),r!==e.value&&this._onValueChange(e),o!==i.value&&this._onValueChange(i)}_updateStepNonRange(){let e=this._getInput(oe.END);if(e){let i=e.value;e.step=this._step,this._platform.SAFARI&&(e.value=e.value),e._updateThumbUIByValue(),i!==e.value&&this._onValueChange(e)}}displayWith=e=>`${e}`;_tickMarks;_noopAnimations=Oe();_resizeObserver=null;_cachedWidth;_cachedLeft;_rippleRadius=24;startValueIndicatorText="";endValueIndicatorText="";_endThumbTransform;_startThumbTransform;_isRange=!1;_isRtl=w(()=>this._dir?.valueSignal()==="rtl");_hasViewInitialized=!1;_tickMarkTrackWidth=0;_hasAnimation=!1;_resizeTimer=null;_platform=d(me);constructor(){d(Qe).load(or);let e=this._isRtl();$o(()=>{let i=this._isRtl();i!==e&&(e=i,this._isRange?this._onDirChangeRange():this._onDirChangeNonRange(),this._updateTickMarkUI())})}_knobRadius=8;_inputPadding;ngAfterViewInit(){this._platform.isBrowser&&this._updateDimensions();let e=this._getInput(oe.END),i=this._getInput(oe.START);this._isRange=!!e&&!!i,this._cdr.detectChanges();let r=this._getThumb(oe.END);this._rippleRadius=r._ripple.radius,this._inputPadding=this._rippleRadius-this._knobRadius,this._isRange?this._initUIRange(e,i):this._initUINonRange(e),this._updateTrackUI(e),this._updateTickMarkUI(),this._updateTickMarkTrackUI(),this._observeHostResize(),this._cdr.detectChanges()}_initUINonRange(e){e.initProps(),e.initUI(),this._updateValueIndicatorUI(e),this._hasViewInitialized=!0,e._updateThumbUIByValue()}_initUIRange(e,i){e.initProps(),e.initUI(),i.initProps(),i.initUI(),e._updateMinMax(),i._updateMinMax(),e._updateStaticStyles(),i._updateStaticStyles(),this._updateValueIndicatorUIs(),this._hasViewInitialized=!0,e._updateThumbUIByValue(),i._updateThumbUIByValue()}ngOnDestroy(){this._resizeObserver?.disconnect(),this._resizeObserver=null}_onDirChangeRange(){let e=this._getInput(oe.END),i=this._getInput(oe.START);e._setIsLeftThumb(),i._setIsLeftThumb(),e.translateX=e._calcTranslateXByValue(),i.translateX=i._calcTranslateXByValue(),e._updateStaticStyles(),i._updateStaticStyles(),e._updateWidthInactive(),i._updateWidthInactive(),e._updateThumbUIByValue(),i._updateThumbUIByValue()}_onDirChangeNonRange(){this._getInput(oe.END)._updateThumbUIByValue()}_observeHostResize(){typeof ResizeObserver>"u"||!ResizeObserver||this._ngZone.runOutsideAngular(()=>{this._resizeObserver=new ResizeObserver(()=>{this._isActive()||(this._resizeTimer&&clearTimeout(this._resizeTimer),this._onResize())}),this._resizeObserver.observe(this._elementRef.nativeElement)})}_isActive(){return this._getThumb(oe.START)._isActive||this._getThumb(oe.END)._isActive}_getValue(e=oe.END){let i=this._getInput(e);return i?i.value:this.min}_skipUpdate(){return!!(this._getInput(oe.START)?._skipUIUpdate||this._getInput(oe.END)?._skipUIUpdate)}_updateDimensions(){this._cachedWidth=this._elementRef.nativeElement.offsetWidth,this._cachedLeft=this._elementRef.nativeElement.getBoundingClientRect().left}_setTrackActiveStyles(e){let i=this._trackActive.nativeElement.style;i.left=e.left,i.right=e.right,i.transformOrigin=e.transformOrigin,i.transform=e.transform}_calcTickMarkTransform(e){let i=e*(this._tickMarkTrackWidth/(this._tickMarks.length-1));return`translateX(${this._isRtl()?this._cachedWidth-6-i:i}px)`}_onTranslateXChange(e){this._hasViewInitialized&&(this._updateThumbUI(e),this._updateTrackUI(e),this._updateOverlappingThumbUI(e))}_onTranslateXChangeBySideEffect(e,i){this._hasViewInitialized&&(e._updateThumbUIByValue(),i._updateThumbUIByValue())}_onValueChange(e){this._hasViewInitialized&&(this._updateValueIndicatorUI(e),this._updateTickMarkUI(),this._cdr.detectChanges())}_onMinMaxOrStepChange(){this._hasViewInitialized&&(this._updateTickMarkUI(),this._updateTickMarkTrackUI(),this._cdr.markForCheck())}_onResize(){if(this._hasViewInitialized){if(this._updateDimensions(),this._isRange){let e=this._getInput(oe.END),i=this._getInput(oe.START);e._updateThumbUIByValue(),i._updateThumbUIByValue(),e._updateStaticStyles(),i._updateStaticStyles(),e._updateMinMax(),i._updateMinMax(),e._updateWidthInactive(),i._updateWidthInactive()}else{let e=this._getInput(oe.END);e&&e._updateThumbUIByValue()}this._updateTickMarkUI(),this._updateTickMarkTrackUI(),this._cdr.detectChanges()}}_thumbsOverlap=!1;_areThumbsOverlapping(){let e=this._getInput(oe.START),i=this._getInput(oe.END);return!e||!i?!1:i.translateX-e.translateX<20}_updateOverlappingThumbClassNames(e){let i=e.getSibling(),r=this._getThumb(e.thumbPosition);this._getThumb(i.thumbPosition)._hostElement.classList.remove("mdc-slider__thumb--top"),r._hostElement.classList.toggle("mdc-slider__thumb--top",this._thumbsOverlap)}_updateOverlappingThumbUI(e){!this._isRange||this._skipUpdate()||this._thumbsOverlap!==this._areThumbsOverlapping()&&(this._thumbsOverlap=!this._thumbsOverlap,this._updateOverlappingThumbClassNames(e))}_updateThumbUI(e){if(this._skipUpdate())return;let i=this._getThumb(e.thumbPosition===oe.END?oe.END:oe.START);i._hostElement.style.transform=`translateX(${e.translateX}px)`}_updateValueIndicatorUI(e){if(this._skipUpdate())return;let i=this.displayWith(e.value);if(this._hasViewInitialized?e._valuetext.set(i):e._hostElement.setAttribute("aria-valuetext",i),this.discrete){e.thumbPosition===oe.START?this.startValueIndicatorText=i:this.endValueIndicatorText=i;let r=this._getThumb(e.thumbPosition);i.length<3?r._hostElement.classList.add("mdc-slider__thumb--short-value"):r._hostElement.classList.remove("mdc-slider__thumb--short-value")}}_updateValueIndicatorUIs(){let e=this._getInput(oe.END),i=this._getInput(oe.START);e&&this._updateValueIndicatorUI(e),i&&this._updateValueIndicatorUI(i)}_updateTickMarkTrackUI(){if(!this.showTickMarks||this._skipUpdate())return;let e=this._step&&this._step>0?this._step:1,r=(Math.floor(this.max/e)*e-this.min)/(this.max-this.min);this._tickMarkTrackWidth=(this._cachedWidth-6)*r}_updateTrackUI(e){this._skipUpdate()||(this._isRange?this._updateTrackUIRange(e):this._updateTrackUINonRange(e))}_updateTrackUIRange(e){let i=e.getSibling();if(!i||!this._cachedWidth)return;let r=Math.abs(i.translateX-e.translateX)/this._cachedWidth;e._isLeftThumb&&this._cachedWidth?this._setTrackActiveStyles({left:"auto",right:`${this._cachedWidth-i.translateX}px`,transformOrigin:"right",transform:`scaleX(${r})`}):this._setTrackActiveStyles({left:`${i.translateX}px`,right:"auto",transformOrigin:"left",transform:`scaleX(${r})`})}_updateTrackUINonRange(e){this._isRtl()?this._setTrackActiveStyles({left:"auto",right:"0px",transformOrigin:"right",transform:`scaleX(${1-e.fillPercentage})`}):this._setTrackActiveStyles({left:"0px",right:"auto",transformOrigin:"left",transform:`scaleX(${e.fillPercentage})`})}_updateTickMarkUI(){if(!this.showTickMarks||this.step===void 0||this.min===void 0||this.max===void 0)return;let e=this.step>0?this.step:1;this._isRange?this._updateTickMarkUIRange(e):this._updateTickMarkUINonRange(e)}_updateTickMarkUINonRange(e){let i=this._getValue(),r=Math.max(Math.round((i-this.min)/e),0)+1,o=Math.max(Math.round((this.max-i)/e),0)-1;this._isRtl()?r++:o++,this._tickMarks=Array(r).fill(Ns.ACTIVE).concat(Array(o).fill(Ns.INACTIVE))}_updateTickMarkUIRange(e){let i=this._getValue(),r=this._getValue(oe.START),o=Math.max(Math.round((r-this.min)/e),0),s=Math.max(Math.round((i-r)/e)+1,0),a=Math.max(Math.round((this.max-i)/e),0);this._tickMarks=Array(o).fill(Ns.INACTIVE).concat(Array(s).fill(Ns.ACTIVE),Array(a).fill(Ns.INACTIVE))}_getInput(e){if(e===oe.END&&this._input)return this._input;if(this._inputs?.length)return e===oe.START?this._inputs.first:this._inputs.last}_getThumb(e){return e===oe.END?this._thumbs?.last:this._thumbs?.first}_setTransition(e){this._hasAnimation=!this._platform.IOS&&e&&!this._noopAnimations,this._elementRef.nativeElement.classList.toggle("mat-mdc-slider-with-animation",this._hasAnimation)}_isCursorOnSliderThumb(e,i){let r=i.width/2,o=i.x+r,s=i.y+r,a=e.clientX-o,l=e.clientY-s;return Math.pow(a,2)+Math.pow(l,2)<Math.pow(r,2)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-slider"]],contentQueries:function(i,r,o){if(i&1&&Dn(o,Fw,5)(o,_L,4),i&2){let s;Q(s=J())&&(r._input=s.first),Q(s=J())&&(r._inputs=s)}},viewQuery:function(i,r){if(i&1&&Fe(fL,5)(Lw,5),i&2){let o;Q(o=J())&&(r._trackActive=o.first),Q(o=J())&&(r._thumbs=o)}},hostAttrs:[1,"mat-mdc-slider","mdc-slider"],hostVars:12,hostBindings:function(i,r){i&2&&(pt("mat-"+(r.color||"primary")),ee("mdc-slider--range",r._isRange)("mdc-slider--disabled",r.disabled)("mdc-slider--discrete",r.discrete)("mdc-slider--tick-marks",r.showTickMarks)("_mat-animation-noopable",r._noopAnimations))},inputs:{disabled:[2,"disabled","disabled",ne],discrete:[2,"discrete","discrete",ne],showTickMarks:[2,"showTickMarks","showTickMarks",ne],min:[2,"min","min",xt],color:"color",disableRipple:[2,"disableRipple","disableRipple",ne],max:[2,"max","max",xt],step:[2,"step","step",xt],displayWith:"displayWith"},exportAs:["matSlider"],features:[Re([{provide:s_,useExisting:t}])],ngContentSelectors:hL,decls:9,vars:5,consts:[["trackActive",""],["tickMarkContainer",""],[1,"mdc-slider__track"],[1,"mdc-slider__track--inactive"],[1,"mdc-slider__track--active"],[1,"mdc-slider__track--active_fill"],[1,"mdc-slider__tick-marks"],[3,"discrete","thumbPosition","valueIndicatorText"],[3,"class","transform"]],template:function(i,r){i&1&&(Te(),le(0),_(1,"div",2),A(2,"div",3),_(3,"div",4),A(4,"div",5,0),b(),Y(6,gL,3,1,"div",6),b(),Y(7,vL,1,3,"mat-slider-visual-thumb",7),A(8,"mat-slider-visual-thumb",7)),i&2&&(y(6),Z(r.showTickMarks?6:-1),y(),Z(r._isRange?7:-1),y(),k("discrete",r.discrete)("thumbPosition",2)("valueIndicatorText",r.endValueIndicatorText))},dependencies:[bL],styles:[`.mdc-slider__track {
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
`],encapsulation:2})}return t})();var yL={provide:eo,useExisting:Bt(()=>a_),multi:!0};var a_=(()=>{class t{_ngZone=d(T);_elementRef=d(F);_cdr=d(Le);_slider=d(s_);_platform=d(me);_listenerCleanups;get value(){return xt(this._hostElement.value,0)}set value(e){e===null&&(e=this._getDefaultValue()),e=isNaN(e)?0:e;let i=e+"";if(!this._hasSetInitialValue){this._initialValue=i;return}this._isActive||this._setValue(i)}_setValue(e){this._hostElement.value=e,this._updateThumbUIByValue(),this._slider._onValueChange(this),this._cdr.detectChanges(),this._slider._cdr.markForCheck()}valueChange=new W;dragStart=new W;dragEnd=new W;get translateX(){return this._slider.min>=this._slider.max?(this._translateX=this._tickMarkOffset,this._translateX):(this._translateX===void 0&&(this._translateX=this._calcTranslateXByValue()),this._translateX)}set translateX(e){this._translateX=e}_translateX;thumbPosition=oe.END;get min(){return xt(this._hostElement.min,0)}set min(e){this._hostElement.min=e+"",this._cdr.detectChanges()}get max(){return xt(this._hostElement.max,0)}set max(e){this._hostElement.max=e+"",this._cdr.detectChanges()}get step(){return xt(this._hostElement.step,0)}set step(e){this._hostElement.step=e+"",this._cdr.detectChanges()}get disabled(){return ne(this._hostElement.disabled)}set disabled(e){this._hostElement.disabled=e,this._cdr.detectChanges(),this._slider.disabled!==this.disabled&&(this._slider.disabled=this.disabled)}get percentage(){return this._slider.min>=this._slider.max?this._slider._isRtl()?1:0:(this.value-this._slider.min)/(this._slider.max-this._slider.min)}get fillPercentage(){return this._slider._cachedWidth?this._translateX===0?0:this.translateX/this._slider._cachedWidth:this._slider._isRtl()?1:0}_hostElement=this._elementRef.nativeElement;_valuetext=H("");_knobRadius=8;_tickMarkOffset=3;_isActive=!1;_isFocused=!1;_setIsFocused(e){this._isFocused=e}_hasSetInitialValue=!1;_initialValue;_formControl;_destroyed=new D;_skipUIUpdate=!1;_onChangeFn;_onTouchedFn=()=>{};_isControlInitialized=!1;constructor(){let e=d(we);this._ngZone.runOutsideAngular(()=>{this._listenerCleanups=[e.listen(this._hostElement,"pointerdown",this._onPointerDown.bind(this)),e.listen(this._hostElement,"pointermove",this._onPointerMove.bind(this)),e.listen(this._hostElement,"pointerup",this._onPointerUp.bind(this))]})}ngOnDestroy(){this._listenerCleanups.forEach(e=>e()),this._destroyed.next(),this._destroyed.complete(),this.dragStart.complete(),this.dragEnd.complete()}initProps(){this._updateWidthInactive(),this.disabled!==this._slider.disabled&&(this._slider.disabled=!0),this.step=this._slider.step,this.min=this._slider.min,this.max=this._slider.max,this._initValue()}initUI(){this._updateThumbUIByValue()}_initValue(){this._hasSetInitialValue=!0,this._initialValue===void 0?this.value=this._getDefaultValue():(this._hostElement.value=this._initialValue,this._updateThumbUIByValue(),this._slider._onValueChange(this),this._cdr.detectChanges())}_getDefaultValue(){return this.min}_onBlur(){this._setIsFocused(!1),this._onTouchedFn()}_onFocus(){this._slider._setTransition(!1),this._slider._updateTrackUI(this),this._setIsFocused(!0)}_onChange(){this.valueChange.emit(this.value),this._isActive&&this._updateThumbUIByValue({withAnimation:!0})}_onInput(){this._onChangeFn?.(this.value),(this._slider.step||!this._isActive)&&this._updateThumbUIByValue({withAnimation:!0}),this._slider._onValueChange(this)}_onNgControlValueChange(){(!this._isActive||!this._isFocused)&&(this._slider._onValueChange(this),this._updateThumbUIByValue()),this._slider.disabled=this._formControl.disabled}_onPointerDown(e){if(!(this.disabled||e.button!==0)){if(this._platform.IOS){let i=this._slider._isCursorOnSliderThumb(e,this._slider._getThumb(this.thumbPosition)._hostElement.getBoundingClientRect());this._isActive=i,this._updateWidthActive(),this._slider._updateDimensions();return}this._isActive=!0,this._setIsFocused(!0),this._updateWidthActive(),this._slider._updateDimensions(),this._slider.step||this._updateThumbUIByPointerEvent(e,{withAnimation:!0}),this.disabled||(this._handleValueCorrection(e),this.dragStart.emit({source:this,parent:this._slider,value:this.value}))}}_handleValueCorrection(e){this._skipUIUpdate=!0,setTimeout(()=>{this._skipUIUpdate=!1,this._fixValue(e)},0)}_fixValue(e){let i=e.clientX-this._slider._cachedLeft,r=this._slider._cachedWidth,o=this._slider.step===0?1:this._slider.step,s=Math.floor((this._slider.max-this._slider.min)/o),a=this._slider._isRtl()?1-i/r:i/r,c=Math.round(a*s)/s*(this._slider.max-this._slider.min)+this._slider.min,u=Math.round(c/o)*o,f=this.value;if(u===f){this._slider._onValueChange(this),this._slider.step>0?this._updateThumbUIByValue():this._updateThumbUIByPointerEvent(e,{withAnimation:this._slider._hasAnimation});return}this.value=u,this.valueChange.emit(this.value),this._onChangeFn?.(this.value),this._slider._onValueChange(this),this._slider.step>0?this._updateThumbUIByValue():this._updateThumbUIByPointerEvent(e,{withAnimation:this._slider._hasAnimation})}_onPointerMove(e){!this._slider.step&&this._isActive&&this._updateThumbUIByPointerEvent(e)}_onPointerUp(){this._isActive&&(this._isActive=!1,this._platform.SAFARI&&this._setIsFocused(!1),this.dragEnd.emit({source:this,parent:this._slider,value:this.value}),setTimeout(()=>this._updateWidthInactive(),this._platform.IOS?10:0))}_clamp(e){let i=this._tickMarkOffset,r=this._slider._cachedWidth-this._tickMarkOffset;return Math.max(Math.min(e,r),i)}_calcTranslateXByValue(){return this._slider._isRtl()?(1-this.percentage)*(this._slider._cachedWidth-this._tickMarkOffset*2)+this._tickMarkOffset:this.percentage*(this._slider._cachedWidth-this._tickMarkOffset*2)+this._tickMarkOffset}_calcTranslateXByPointerEvent(e){return e.clientX-this._slider._cachedLeft}_updateWidthActive(){}_updateWidthInactive(){this._hostElement.style.padding=`0 ${this._slider._inputPadding}px`,this._hostElement.style.width=`calc(100% + ${this._slider._inputPadding-this._tickMarkOffset*2}px)`,this._hostElement.style.left=`-${this._slider._rippleRadius-this._tickMarkOffset}px`}_updateThumbUIByValue(e){this.translateX=this._clamp(this._calcTranslateXByValue()),this._updateThumbUI(e)}_updateThumbUIByPointerEvent(e,i){this.translateX=this._clamp(this._calcTranslateXByPointerEvent(e)),this._updateThumbUI(i)}_updateThumbUI(e){this._slider._setTransition(!!e?.withAnimation),this._slider._onTranslateXChange(this)}writeValue(e){(this._isControlInitialized||e!==null)&&(this.value=e)}registerOnChange(e){this._onChangeFn=e,this._isControlInitialized=!0}registerOnTouched(e){this._onTouchedFn=e}setDisabledState(e){this.disabled=e}focus(){this._hostElement.focus()}blur(){this._hostElement.blur()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["input","matSliderThumb",""]],hostAttrs:["type","range",1,"mdc-slider__input"],hostVars:1,hostBindings:function(i,r){i&1&&z("change",function(){return r._onChange()})("input",function(){return r._onInput()})("blur",function(){return r._onBlur()})("focus",function(){return r._onFocus()}),i&2&&_e("aria-valuetext",r._valuetext())},inputs:{value:[2,"value","value",xt]},outputs:{valueChange:"valueChange",dragStart:"dragStart",dragEnd:"dragEnd"},exportAs:["matSliderThumb"],features:[Re([yL,{provide:Fw,useExisting:t}])]})}return t})();var Bw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[xs,Me]})}return t})();var SL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
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
`],encapsulation:2})}return t})(),DL={passive:!0},jw=(()=>{class t{_platform=d(me);_ngZone=d(T);_renderer=d(Ze).createRenderer(null,null);_styleLoader=d(Qe);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return ze;this._styleLoader.load(SL);let i=Xt(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new D,s="cdk-text-field-autofilled",a=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,DL)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=Xt(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var Uw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({})}return t})();var Hw=new g("MAT_INPUT_VALUE_ACCESSOR");var l_=class{_box;_destroyed=new D;_resizeSubject=new D;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ie(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(be(e=>e.some(i=>i.target===n)),Nc({bufferSize:1,refCount:!0}),Ce(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},zw=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(T);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new l_(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var EL=["notch"],wL=["*"],$w=["iconPrefixContainer"],Ww=["textPrefixContainer"],Gw=["iconSuffixContainer"],qw=["textSuffixContainer"],xL=["textField"],IL=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],TL=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function ML(t,n){t&1&&A(0,"span",21)}function NL(t,n){if(t&1&&(_(0,"label",20),le(1,1),Y(2,ML,1,0,"span",21),b()),t&2){let e=G(2);k("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),_e("for",e._control.disableAutomaticLabeling?null:e._control.id),y(2),Z(!e.hideRequiredMarker&&e._control.required?2:-1)}}function kL(t,n){if(t&1&&Y(0,NL,3,5,"label",20),t&2){let e=G();Z(e._hasFloatingLabel()?0:-1)}}function AL(t,n){t&1&&A(0,"div",7)}function RL(t,n){}function OL(t,n){if(t&1&&Ot(0,RL,0,0,"ng-template",13),t&2){G(2);let e=vi(1);k("ngTemplateOutlet",e)}}function PL(t,n){if(t&1&&(_(0,"div",9),Y(1,OL,1,1,null,13),b()),t&2){let e=G();k("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),y(),Z(e._forceDisplayInfixLabel()?-1:1)}}function FL(t,n){t&1&&(_(0,"div",10,2),le(2,2),b())}function LL(t,n){t&1&&(_(0,"div",11,3),le(2,3),b())}function VL(t,n){}function BL(t,n){if(t&1&&Ot(0,VL,0,0,"ng-template",13),t&2){G();let e=vi(1);k("ngTemplateOutlet",e)}}function jL(t,n){t&1&&(_(0,"div",14,4),le(2,4),b())}function UL(t,n){t&1&&(_(0,"div",15,5),le(2,5),b())}function HL(t,n){t&1&&A(0,"div",16)}function zL(t,n){t&1&&(_(0,"div",18),le(1,6),b())}function $L(t,n){if(t&1&&(_(0,"mat-hint",22),R(1),b()),t&2){let e=G(2);k("id",e._hintLabelId),y(),wt(e.hintLabel)}}function WL(t,n){if(t&1&&(_(0,"div",19),Y(1,$L,2,2,"mat-hint",22),le(2,7),A(3,"div",23),le(4,8),b()),t&2){let e=G();y(),Z(e.hintLabel?1:-1)}}var Ti=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-label"]]})}return t})(),GL=new g("MatError");var jl=(()=>{class t{align="start";id=d(Ue).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(qt("id",r.id),_e("align",null),ee("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),qL=new g("MatPrefix");var ex=new g("MatSuffix"),Ul=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[Re([{provide:ex,useExisting:t}])]})}return t})(),tx=new g("FloatingLabelParent"),Yw=(()=>{class t{_elementRef=d(F);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(zw);_ngZone=d(T);_parent=d(tx);_resizeSubscription=new ge;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return YL(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&ee("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function YL(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var Zw="mdc-line-ripple--active",vf="mdc-line-ripple--deactivating",Kw=(()=>{class t{_elementRef=d(F);_cleanupTransitionEnd;constructor(){let e=d(T),i=d(we);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(vf),e.add(Zw)}deactivate(){this._elementRef.nativeElement.classList.add(vf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(vf);e.propertyName==="opacity"&&r&&i.remove(Zw,vf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),Xw=(()=>{class t{_elementRef=d(F);_ngZone=d(T);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Fe(EL,5),i&2){let o;Q(o=J())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&ee("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:wL,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Te(),sn(0,"div",1),mt(1,"div",2,0),le(3),Et(),sn(4,"div",3))},encapsulation:2})}return t})(),Hl=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t})}return t})();var zl=new g("MatFormField"),ZL=new g("MAT_FORM_FIELD_DEFAULT_OPTIONS"),Qw="fill",KL="auto",Jw="fixed",XL="translateY(-50%)",Xn=(()=>{class t{_elementRef=d(F);_changeDetectorRef=d(Le);_platform=d(me);_idGenerator=d(Ue);_ngZone=d(T);_defaults=d(ZL,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=Pa("iconPrefixContainer");_textPrefixContainerSignal=Pa("textPrefixContainer");_iconSuffixContainerSignal=Pa("iconSuffixContainer");_textSuffixContainerSignal=Pa("textSuffixContainer");_prefixSuffixContainers=w(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=iD(Ti);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Ii(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||KL}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||Qw;this._appearanceSignal.set(i)}_appearanceSignal=H(Qw);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||Jw}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||Jw}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new D;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Oe();constructor(){let e=this._defaults,i=d(ut);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),nt(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=w(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(kt([void 0,void 0]),re(()=>[i.errorState,i.userAriaDescribedBy]),Mc(),be(([[o,s],[a,l]])=>o!==a||s!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Ce(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),ii(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){$o({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=w(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,m=`calc(${u} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,v=`var(--mat-mdc-form-field-label-transform, ${XL} translateX(${m}))`,S=s+a+l+c;return[v,S]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&($d(o,r._labelChild,Ti,5),Dn(o,Hl,5)(o,qL,5)(o,ex,5)(o,GL,5)(o,jl,5)),i&2){Gd();let s;Q(s=J())&&(r._formFieldControl=s.first),Q(s=J())&&(r._prefixChildren=s),Q(s=J())&&(r._suffixChildren=s),Q(s=J())&&(r._errorChildren=s),Q(s=J())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Wd(r._iconPrefixContainerSignal,$w,5)(r._textPrefixContainerSignal,Ww,5)(r._iconSuffixContainerSignal,Gw,5)(r._textSuffixContainerSignal,qw,5),Fe(xL,5)($w,5)(Ww,5)(Gw,5)(qw,5)(Yw,5)(Xw,5)(Kw,5)),i&2){Gd(4);let o;Q(o=J())&&(r._textField=o.first),Q(o=J())&&(r._iconPrefixContainer=o.first),Q(o=J())&&(r._textPrefixContainer=o.first),Q(o=J())&&(r._iconSuffixContainer=o.first),Q(o=J())&&(r._textSuffixContainer=o.first),Q(o=J())&&(r._floatingLabel=o.first),Q(o=J())&&(r._notchedOutline=o.first),Q(o=J())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&ee("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Re([{provide:zl,useExisting:t},{provide:tx,useExisting:t}])],ngContentSelectors:TL,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Te(IL),Ot(0,kL,1,1,"ng-template",null,0,Up),_(2,"div",6,1),z("click",function(s){return r._control.onContainerClick(s)}),Y(4,AL,1,0,"div",7),_(5,"div",8),Y(6,PL,2,2,"div",9),Y(7,FL,3,0,"div",10),Y(8,LL,3,0,"div",11),_(9,"div",12),Y(10,BL,1,1,null,13),le(11),b(),Y(12,jL,3,0,"div",14),Y(13,UL,3,0,"div",15),b(),Y(14,HL,1,0,"div",16),b(),_(15,"div",17),Y(16,zL,2,0,"div",18)(17,WL,5,1,"div",19),b()),i&2){let o;y(2),ee("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),y(2),Z(!r._hasOutline()&&!r._control.disabled?4:-1),y(2),Z(r._hasOutline()?6:-1),y(),Z(r._hasIconPrefix?7:-1),y(),Z(r._hasTextPrefix?8:-1),y(2),Z(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),y(2),Z(r._hasTextSuffix?12:-1),y(),Z(r._hasIconSuffix?13:-1),y(),Z(r._hasOutline()?-1:14),y(),ee("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();y(),Z((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[Yw,Xw,Zp,Kw,jl],styles:[`.mdc-text-field {
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
`],encapsulation:2})}return t})();var _f=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}isSignalErrorState(e){if(!e)return!1;let i=e().invalid(),r=e().touched();return i&&r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();var ks=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,i,r,o){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o,e?Wt(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,e)??!1}};var $l=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[$E,Xn,Me]})}return t})();var QL=["button","checkbox","file","hidden","image","radio","range","reset","submit"],JL=new g("MAT_INPUT_CONFIG"),bf=(()=>{class t{_elementRef=d(F);_platform=d(me);ngControl=d(Zi,{optional:!0,self:!0});_autofillMonitor=d(jw);_ngZone=d(T);_formField=d(zl,{optional:!0});_renderer=d(we);_uid=d(Ue).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(JL,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new D;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=Ii(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Kn.required)??!1}set required(e){this._required=Ii(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Xv().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=Ii(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Xv().has(e));constructor(){let e=d(hl,{optional:!0}),i=d(ml,{optional:!0}),r=d(_f),o=d(Hw,{optional:!0,self:!0}),s=d(ps,{optional:!0,self:!0}),a=this._elementRef.nativeElement,l=a.nodeName.toLowerCase();o?Wt(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=a,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(a,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new ks(r,s||this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=l==="select",this._isTextarea=l==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=a.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&nt(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){QL.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&z("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(qt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),_e("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),ee("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",ne]},exportAs:["matInput"],features:[Re([{provide:Hl,useExisting:t}]),qe]})}return t})(),yf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[$l,$l,Uw,Me]})}return t})();function ox(t){return Error(`Unable to find icon with the name "${t}"`)}function eV(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function sx(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function ax(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Mi=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},cx=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Mi(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(ot.HTML,r);if(!s)throw ax(r);let a=so(s);return this._addSvgIconConfig(e,i,new Mi("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Mi(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(ot.HTML,i);if(!o)throw ax(i);let s=so(o);return this._addSvgIconSetConfig(e,new Mi("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(ot.RESOURCE_URL,e);if(!i)throw sx(e);let r=this._cachedIconsByUrl.get(i);return r?q(Cf(r)):this._loadSvgIconFromConfig(new Mi(e,null)).pipe(rt(o=>this._cachedIconsByUrl.set(i,o)),re(o=>Cf(o)))}getNamedSvgIcon(e,i=""){let r=lx(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):zs(ox(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?q(Cf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(re(i=>Cf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return q(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Cr(a=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(ot.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(c)),q(null)})));return Gs(o).pipe(re(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw ox(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(rt(i=>e.svgText=i),re(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?q(null):this._fetchIcon(e).pipe(rt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(so("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(so("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw eV();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(ot.RESOURCE_URL,i);if(!s)throw sx(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let l=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(re(c=>so(c)),Sr(()=>this._inProgressUrlFetches.delete(s)),Ys());return this._inProgressUrlFetches.set(s,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(lx(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return tV(o)?new Mi(o.url,null,o.options):new Mi(o,null)}}static \u0275fac=function(i){return new(i||t)(j($a,8),j(Wa),j(L,8),j(bt))};static \u0275prov=se({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Cf(t){return t.cloneNode(!0)}function lx(t,n){return t+":"+n}function tV(t){return!!(t.url&&t.options)}var nV=["*"],iV=new g("MAT_ICON_DEFAULT_OPTIONS"),rV=new g("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(L),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),dx=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],oV=dx.map(t=>`[${t}]`).join(", "),sV=/^url\(['"]?#(.*?)['"]?\)$/,Sf=(()=>{class t{_elementRef=d(F);_iconRegistry=d(cx);_location=d(rV);_errorHandler=d(bt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=ge.EMPTY;constructor(){let e=d(new bi("aria-hidden"),{optional:!0}),i=d(iV,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(oV),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)dx.forEach(s=>{let a=i[o],l=a.getAttribute(s),c=l?l.match(sV):null;if(c){let u=r.get(a);u||(u=[],r.set(a,u)),u.push({name:s,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe($e(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(_e("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),pt(r.color?"mat-"+r.color:""),ee("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",ne],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:nV,decls:1,vars:0,template:function(i,r){i&1&&(Te(),le(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
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
`],encapsulation:2})}return t})();var aV=["*",[["mat-toolbar-row"]]],lV=["*","mat-toolbar-row"],cV=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),Df=(()=>{class t{_elementRef=d(F);_platform=d(me);_document=d(L);color;_toolbarRows;ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&Dn(o,cV,5),i&2){let s;Q(s=J())&&(r._toolbarRows=s)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(pt(r.color?"mat-"+r.color:""),ee("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:lV,decls:2,vars:0,template:function(i,r){i&1&&(Te(aV),le(0),le(1,1))},styles:[`.mat-toolbar {
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
`],encapsulation:2})}return t})();var dV=["*"],ux=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&ee("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:dV,decls:1,vars:0,template:function(i,r){i&1&&(Te(),le(0))},styles:[`.mat-internal-form-field {
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
`],encapsulation:2})}return t})();var uV=["input"],fV=["label"],hV=["*"],c_={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},mV=new g("mat-checkbox-default-options",{providedIn:"root",factory:()=>c_}),Tt=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(Tt||{}),d_=class{source;checked},Wl=(()=>{class t{_elementRef=d(F);_changeDetectorRef=d(Le);_ngZone=d(T);_animationsDisabled=Oe();_options=d(mV,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new d_;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new W;indeterminateChange=new W;value;disableRipple=!1;_inputElement;_labelElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=Tt.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){d(Qe).load(or);let e=d(new bi("tabindex"),{optional:!0});this._options=this._options||c_,this.color=this._options.color||c_.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=d(Ue).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(Tt.Indeterminate):this._transitionCheckState(this.checked?Tt.Checked:Tt.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=H(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?Tt.Checked:Tt.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case Tt.Init:if(i===Tt.Checked)return this._animationClasses.uncheckedToChecked;if(i==Tt.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case Tt.Unchecked:return i===Tt.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case Tt.Checked:return i===Tt.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case Tt.Indeterminate:return i===Tt.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_onTouchTargetClick(){this._handleInputClick(),this.disabled||this._inputElement.nativeElement.focus()}_preventBubblingFromLabel(e){e.target&&this._labelElement.nativeElement.contains(e.target)&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&Fe(uV,5)(fV,5),i&2){let o;Q(o=J())&&(r._inputElement=o.first),Q(o=J())&&(r._labelElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(qt("id",r.id),_e("tabindex",null)("aria-label",null)("aria-labelledby",null),pt(r.color?"mat-"+r.color:"mat-accent"),ee("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",ne],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",ne],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",ne],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:xt(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",ne],checked:[2,"checked","checked",ne],disabled:[2,"disabled","disabled",ne],indeterminate:[2,"indeterminate","indeterminate",ne]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Re([{provide:eo,useExisting:Bt(()=>t),multi:!0},{provide:to,useExisting:t,multi:!0}]),qe],ngContentSelectors:hV,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target",3,"click"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-label",3,"for"]],template:function(i,r){if(i&1&&(Te(),_(0,"div",3),z("click",function(s){return r._preventBubblingFromLabel(s)}),_(1,"div",4,0)(3,"div",5),z("click",function(){return r._onTouchTargetClick()}),b(),_(4,"input",6,1),z("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),b(),A(6,"div",7),_(7,"div",8),Ao(),_(8,"svg",9),A(9,"path",10),b(),td(),A(10,"div",11),b(),A(11,"div",12),b(),_(12,"label",13,2),le(14),b()()),i&2){let o=vi(2);k("labelPosition",r.labelPosition),y(4),ee("mdc-checkbox--selected",r.checked),k("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),_e("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),y(7),k("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),y(),k("for",r.inputId)}},dependencies:[mo,ux],styles:[`.mdc-checkbox {
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
`],encapsulation:2})}return t})(),fx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[Wl,Me]})}return t})();var sr=class t{http=d($a);utils=d(fn);state=H({default:[],custom:[]});constructor(){this.http.get("/data/presets.json").subscribe(n=>{let e=n,i=JSON.parse(localStorage.getItem("customPresets")||"[]");this.state.set({default:e,custom:i})})}checkIfExists(n){return!![...this.state().custom].find(e=>e.palette.name===n.name)}savePreset(n){this.state.set(P(p({},this.state()),{custom:[...this.state().custom,{type:"custom",codes:n.colors.flatMap(e=>this.colorToCode(e)),palette:n}]})),this._save()}updatePreset(n){this.state.set(P(p({},this.state()),{custom:[{type:"custom",codes:n.colors.flatMap(e=>this.colorToCode(e)),palette:n},...this.state().custom.filter(e=>e.palette.name!==n.name)]})),this._save()}deletePreset(n){this.state.set(P(p({},this.state()),{custom:[...this.state().custom.filter(e=>e!==n)]})),this._save()}colorToCode(n){let e=[];for(let i of n.shades){let r=[n.hue,n.saturation,i],o=this.utils.hslToRgb(r[0],r[1],r[2]);e.push(this.utils.rgbToHex(o[0],o[1],o[2]))}return e}_save(){localStorage.setItem("customPresets",JSON.stringify(this.state().custom))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})};function gV(t,n){}var ar=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext;bindings};var f_=(()=>{class t extends nr{_elementRef=d(F);_focusTrapFactory=d(Bv);_config;_interactivityChecker=d(Vv);_ngZone=d(T);_focusMonitor=d(oo);_renderer=d(we);_changeDetectorRef=d(Le);_injector=d(V);_platform=d(me);_document=d(L);_portalOutlet;_focusTrapped=new D;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(ar,{optional:!0})||new ar,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||at(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Dl(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Dl();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Dl()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&Fe(ir,7),i&2){let o;Q(o=J())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&_e("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[Ae],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&Ot(0,gV,0,0,"ng-template",0)},dependencies:[ir],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2,changeDetection:1})}return t})(),Gl=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new D;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!It(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},vV=new g("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(V);return()=>Ds(t)}}),_V=new g("DialogData"),bV=new g("DefaultDialogConfig");function yV(t){let n=H(t),e=new W;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var hx=(()=>{class t{_injector=d(V);_defaultOptions=d(bV,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(ff);_idGenerator=d(Ue);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new D;_afterOpenedAtThisLevel=new D;_ariaHiddenElements=new Map;_scrollStrategy=d(vV);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=pn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(kt(void 0)));open(e,i){let r=this._defaultOptions||new ar;i=p(p({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=xi(this._injector,o),a=new Gl(s,i),l=this._attachContainer(s,a,i);if(a.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe($e(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,a,l,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){u_(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){u_(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),u_(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new wi({positionStrategy:e.positionStrategy||rr().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:ar,useValue:r},{provide:Gl,useValue:i},{provide:Cs,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=f_;let l=new Mn(a,r.viewContainerRef,V.create({parent:o||this._injector,providers:s}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof jt){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=p(p({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Ei(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new Mn(e,o.viewContainerRef,s,null,o.bindings));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:_V,useValue:e.data},{provide:Gl,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(ut,null,{optional:!0}))&&a.push({provide:ut,useValue:yV(e.direction)}),V.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})();function u_(t,n){let e=t.length;for(;e--;)n(t[e])}function CV(t,n){}var wf=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration;bindings},h_="mdc-dialog--open",mx="mdc-dialog--opening",px="mdc-dialog--closing",SV=150,DV=75,EV=(()=>{class t extends f_{_animationStateChanged=new W;_animationsEnabled=!Oe();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?vx(this._config.enterAnimationDuration)??SV:0;_exitAnimationDuration=this._animationsEnabled?vx(this._config.exitAnimationDuration)??DV:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(gx,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(mx,h_)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(h_),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(h_),this._animationsEnabled?(this._hostElement.style.setProperty(gx,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(px)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(mx,px)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Dt(t)))(r||t)}})();static \u0275cmp=x({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(qt("id",r._config.id),_e("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),ee("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[Ae],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(_(0,"div",0)(1,"div",1),Ot(2,CV,0,0,"ng-template",2),b()())},dependencies:[ir],styles:[`.mat-mdc-dialog-container {
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
`],encapsulation:2,changeDetection:1})}return t})(),gx="--mat-dialog-transition-duration";function vx(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?Di(t.substring(0,t.length-2)):t.endsWith("s")?Di(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var Ef=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(Ef||{}),ql=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new Ai(1);_beforeClosed=new Ai(1);_result;_closeFallbackTimeout;_state=Ef.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(be(r=>r.state==="opened"),$e(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(be(r=>r.state==="closed"),$e(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),ii(this.backdropClick(),this.keydownEvents().pipe(be(r=>r.keyCode===27&&!this.disableClose&&!It(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),_x(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(be(i=>i.state==="closing"),$e(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=Ef.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=Ef.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function _x(t,n,e){return t._closeInteractionType=n,t.close(e)}var Yl=new g("MatMdcDialogData"),wV=new g("mat-mdc-dialog-default-options"),xV=new g("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(V);return()=>Ds(t)}}),As=(()=>{class t{_defaultOptions=d(wV,{optional:!0});_scrollStrategy=d(xV);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Ue);_injector=d(V);_dialog=d(hx);_animationsDisabled=Oe();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new D;_afterOpenedAtThisLevel=new D;dialogConfigClass=wf;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=pn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(kt(void 0)));constructor(){this._dialogRefConstructor=ql,this._dialogContainerType=EV,this._dialogDataToken=Yl}open(e,i){let r;i=p(p({},this._defaultOptions||new wf),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,P(p({},i),{positionStrategy:rr(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:ar,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,l)=>(r=new this._dialogRefConstructor(s,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})}return t})(),xf=(()=>{class t{dialogRef=d(ql,{optional:!0});_elementRef=d(F);_dialog=d(As);ariaLabel;type="button";dialogResult;_matDialogClose;ngOnInit(){this.dialogRef||(this.dialogRef=yx(this._elementRef,this._dialog.openDialogs))}ngOnChanges(e){let i=e._matDialogClose;i&&(this.dialogResult=i.currentValue)}_onButtonClick(e){this._elementRef.nativeElement.getAttribute("aria-disabled")!=="true"&&_x(this.dialogRef,e.screenX===0&&e.screenY===0?"keyboard":"mouse",this.dialogResult)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","mat-dialog-close",""],["","matDialogClose",""]],hostVars:2,hostBindings:function(i,r){i&1&&z("click",function(s){return r._onButtonClick(s)}),i&2&&_e("aria-label",r.ariaLabel||null)("type",r.type)},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],type:"type",dialogResult:[0,"mat-dialog-close","dialogResult"],_matDialogClose:[0,"matDialogClose","_matDialogClose"]},exportAs:["matDialogClose"],features:[qe]})}return t})(),bx=(()=>{class t{_dialogRef=d(ql,{optional:!0});_elementRef=d(F);_dialog=d(As);ngOnInit(){this._dialogRef||(this._dialogRef=yx(this._elementRef,this._dialog.openDialogs)),this._dialogRef&&Promise.resolve().then(()=>{this._onAdd()})}ngOnDestroy(){this._dialogRef?._containerInstance&&Promise.resolve().then(()=>{this._onRemove()})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t})}return t})(),If=(()=>{class t extends bx{id=d(Ue).getId("mat-mdc-dialog-title-");_onAdd(){this._dialogRef._containerInstance?._addAriaLabelledBy?.(this.id)}_onRemove(){this._dialogRef?._containerInstance?._removeAriaLabelledBy?.(this.id)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Dt(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["","mat-dialog-title",""],["","matDialogTitle",""]],hostAttrs:[1,"mat-mdc-dialog-title","mdc-dialog__title"],hostVars:1,hostBindings:function(i,r){i&2&&qt("id",r.id)},inputs:{id:"id"},exportAs:["matDialogTitle"],features:[Ae]})}return t})(),Tf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[Vp([Wv])]})}return t})(),Mf=(()=>{class t extends bx{align;_onAdd(){this._dialogRef._containerInstance?._updateActionSectionCount?.(1)}_onRemove(){this._dialogRef._containerInstance?._updateActionSectionCount?.(-1)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Dt(t)))(r||t)}})();static \u0275dir=N({type:t,selectors:[["","mat-dialog-actions",""],["mat-dialog-actions"],["","matDialogActions",""]],hostAttrs:[1,"mat-mdc-dialog-actions","mdc-dialog__actions"],hostVars:6,hostBindings:function(i,r){i&2&&ee("mat-mdc-dialog-actions-align-start",r.align==="start")("mat-mdc-dialog-actions-align-center",r.align==="center")("mat-mdc-dialog-actions-align-end",r.align==="end")},inputs:{align:"align"},features:[Ae]})}return t})();function yx(t,n){let e=t.nativeElement.parentElement;for(;e&&!e.classList.contains("mat-mdc-dialog-container");)e=e.parentElement;return e?n.find(i=>i.id===e.id):null}var IV=()=>({success:!1}),TV=()=>({success:!0}),Nf=class t{data=d(Yl);palette=Qd(this.data.palette);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-palette-save-dialog"]],inputs:{palette:[1,"palette"]},outputs:{palette:"paletteChange"},decls:10,vars:6,consts:[["mat-dialog-title",""],["matButton","",3,"mat-dialog-close"],["matButton","","cdkFocusInitial","",3,"mat-dialog-close"]],template:function(e,i){e&1&&(_(0,"h2",0),R(1),b(),_(2,"mat-dialog-content")(3,"p"),R(4),b()(),_(5,"mat-dialog-actions")(6,"button",1),R(7,"No"),b(),_(8,"button",2),R(9,"Yes"),b()()),e&2&&(y(),Yt("A preset for ",i.palette().name," already exists"),y(3),Yt("Do you want to overwrite ",i.palette().name,"?"),y(2),k("mat-dialog-close",qr(4,IV)),y(2),k("mat-dialog-close",qr(5,TV)))},dependencies:[Vl,hn,If,Tf,Mf,xf],encapsulation:2})};function MV(t,n){if(t&1){let e=Pt();_(0,"h3"),R(1),b(),_(2,"button",0),z("click",function(){Ct(e);let r=G();return St(r.edit())}),_(3,"mat-icon"),R(4,"edit"),b()()}if(t&2){let e=G();y(),wt(e.formField()().value())}}function NV(t,n){if(t&1){let e=Pt();_(0,"mat-form-field",1),A(1,"input",2),Ke(),b(),_(2,"button",3),z("click",function(){Ct(e);let r=G();return St(r.edit())}),_(3,"mat-icon"),R(4,"check"),b()()}if(t&2){let e=G();k("subscriptSizing","dynamic"),y(),k("formField",e.formField()),Xe()}}var kf=class t{formField=it.required();isEditing=H(!1);edit(){this.isEditing.set(!this.isEditing())}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-title-edit"]],inputs:{formField:[1,"formField"]},decls:2,vars:1,consts:[["matIconButton","","aria-label","Edit Field",3,"click"],[3,"subscriptSizing"],["matNativeControl","","input","text",3,"formField"],["matIconButton","","aria-label","Edit Field",1,"offset",3,"click"]],template:function(e,i){e&1&&Y(0,MV,5,1)(1,NV,5,2),e&2&&Z(i.isEditing()?1:0)},dependencies:[er,Xn,Ll,Sf,yf,bf],styles:["[_nghost-%COMP%]{display:flex;align-items:center;--%NS%mat-form-field-container-vertical-padding: 8px;--%NS%mat-form-field-container-height: 32px}h3[_ngcontent-%COMP%]{margin:0}"]})};function kV(t,n){if(t&1){let e=Pt();_(0,"button",17),z("click",function(){Ct(e);let r=G().$index,o=G();return St(o.removeColor(r))}),_(1,"mat-icon"),R(2,"remove"),b()()}}function AV(t,n){if(t&1&&(A(0,"mat-checkbox",1),Ke()),t&2){let e=G().$implicit;k("formField",e.lockHue),Xe()}}function RV(t,n){if(t&1&&(A(0,"mat-checkbox",1),Ke()),t&2){let e=G().$implicit;k("formField",e.lockSaturation),Xe()}}function OV(t,n){if(t&1){let e=Pt();_(0,"mat-form-field",15)(1,"button",18),z("click",function(){let r=Ct(e).$index,o=G().$index,s=G();return St(s.removeShade(o,r))}),_(2,"mat-icon"),R(3,"remove"),b()(),A(4,"input",19),Ke(),b()}if(t&2){let e=n.$implicit,i=G().$implicit;y(),k("disabled",i.lockShade().value()),y(3),k("formField",e),Xe()}}function PV(t,n){if(t&1&&(A(0,"mat-checkbox",1),Ke()),t&2){let e=G().$implicit;k("formField",e.lockShade),Xe()}}function FV(t,n){if(t&1){let e=Pt();_(0,"div",6)(1,"mat-toolbar"),A(2,"pbu-title-edit",1),Ke(),Y(3,kV,3,0,"button",9),b(),_(4,"div",10)(5,"mat-label"),R(6,"Hue"),b(),_(7,"mat-slider",11),A(8,"input",12),Ke(),b(),Y(9,AV,1,1,"mat-checkbox",1),b(),_(10,"div",10)(11,"mat-label"),R(12,"Saturation"),b(),_(13,"mat-slider",13),A(14,"input",12),Ke(),b(),Y(15,RV,1,1,"mat-checkbox",1),b(),_(16,"div",14),ct(17,OV,5,2,"mat-form-field",15,Sn),_(19,"button",16),z("click",function(){let r=Ct(e).$index,o=G();return St(o.addShade(r))}),R(20," Add shade "),_(21,"mat-icon",8),R(22,"add"),b()(),Y(23,PV,1,1,"mat-checkbox",1),b()()}if(t&2){let e=n.$implicit,i=n.$index;y(2),k("formField",e.name),Xe(),y(),Z(i>0?3:-1),y(4),k("disabled",e.lockHue().value()),y(),k("formField",e.hue),Xe(),y(),Z(i>0?9:-1),y(4),k("disabled",e.lockSaturation().value()),y(),k("formField",e.saturation),Xe(),y(),Z(i>0?15:-1),y(2),dt(e.shades),y(2),k("disabled",e.lockShade().value()),y(4),Z(i>0?23:-1)}}var Af=class t{colorState=d(Ut);presetState=d(sr);snackBar=d(Ts);dialog=d(As);stateForm=this.colorState.stateForm;addShade=this.colorState.addShade;addColor=this.colorState.addColor;removeShade=this.colorState.removeShade;removeColor=this.colorState.removeColor;savePreset(){let n=this.colorState.state();this.presetState.checkIfExists(n)?this.dialog.open(Nf,{data:{palette:n}}).afterClosed().subscribe(({success:r})=>{r&&(this.presetState.updatePreset(n),this.snackBar.open(`Preset "${n.name}" updated`,"Close",{duration:5e3}))}):(this.presetState.savePreset(n),this.snackBar.open(`Preset "${n.name}" saved`,"Close",{duration:5e3}))}resetForm(){this.colorState.reset()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-form"]],decls:14,vars:1,consts:[[1,"header"],[3,"formField"],[1,"spacer"],["matButton","",3,"click"],["matButton","filled",3,"click"],[1,"color-list"],[1,"color"],["matButton","","aria-label","Add color",3,"click"],["matSuffix",""],["matIconButton","","aria-label","Remove color"],[1,"control"],["min","0","max","360","step","1","discrete","",3,"disabled"],["matSliderThumb","",3,"formField"],["min","0","max","1","step","0.01","discrete","",3,"disabled"],[1,"shades"],["appearance","fill"],["matButton","","aria-label","Add shade",3,"click","disabled"],["matIconButton","","aria-label","Remove color",3,"click"],["matSuffix","","matIconButton","","aria-label","Remove shade",1,"top-right-icon",3,"click","disabled"],["matInput","","type","number","step","0.01",3,"formField"]],template:function(e,i){e&1&&(_(0,"div",0),A(1,"pbu-title-edit",1),Ke(),A(2,"div",2),_(3,"button",3),z("click",function(){return i.resetForm()}),R(4,"New"),b(),_(5,"button",4),z("click",function(){return i.savePreset()}),R(6,"Save Preset"),b()(),_(7,"div",5),ct(8,FV,24,10,"div",6,zo),b(),_(10,"button",7),z("click",function(){return i.addColor()}),R(11," Add color "),_(12,"mat-icon",8),R(13,"add"),b()()),e&2&&(y(),k("formField",i.stateForm.name),Xe(),y(7),dt(i.stateForm.colors))},dependencies:[Bw,Vw,a_,yf,bf,Xn,Ti,Ul,er,Sf,Ll,hn,Df,fx,Wl,kf],styles:[".header[_ngcontent-%COMP%]{display:flex;gap:1em;margin:1em 0 2em}.header[_ngcontent-%COMP%]   .spacer[_ngcontent-%COMP%]{flex:1 0 auto}.color-list[_ngcontent-%COMP%]{display:grid;gap:1em;grid-template-columns:1fr;grid-template-rows:min-content;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));margin-bottom:1em;--%NS%mat-toolbar-container-background-color: transparent;--%NS%mat-toolbar-standard-height: 34px}.color[_ngcontent-%COMP%]{background:#efefef;padding:1em 2em;border-radius:1em}.color[_ngcontent-%COMP%]   mat-toolbar[_ngcontent-%COMP%]{justify-content:space-between;padding:0;--%NS%mat-toolbar-title-text-size: 16px}.color[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{font-weight:700;font-size:16px}.color[_ngcontent-%COMP%]   mat-form-field[_ngcontent-%COMP%]{padding-right:1em;width:9em;--%NS%mat-form-field-container-text-size: 12px;--%NS%mat-icon-button-state-layer-size: 28px;--%NS%mat-form-field-container-vertical-padding: 3px;--%NS%mat-form-field-container-height: 28px;--%NS%mat-icon-button-touch-target-size: 28px}.color[_ngcontent-%COMP%]   mat-form-field[_ngcontent-%COMP%]   mat-icon[_ngcontent-%COMP%]{width:16px;height:16px;font-size:16px;margin-left:6px}"]})};var Cx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[Me]})}return t})();var Zl=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new D;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var Sx=(()=>{class t{_animationsDisabled=Oe();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&ee("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
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
`],encapsulation:2})}return t})();var LV=["text"],VV=[[["mat-icon"]],"*"],BV=["mat-icon","*"];function jV(t,n){if(t&1&&A(0,"mat-pseudo-checkbox",1),t&2){let e=G();k("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function UV(t,n){if(t&1&&A(0,"mat-pseudo-checkbox",3),t&2){let e=G();k("disabled",e.disabled)}}function HV(t,n){if(t&1&&(_(0,"span",4),R(1),b()),t&2){let e=G();y(),Yt("(",e.group.label,")")}}var p_=new g("MAT_OPTION_PARENT_COMPONENT"),g_=new g("MatOptgroup");var m_=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},lr=(()=>{class t{_element=d(F);_changeDetectorRef=d(Le);_parent=d(p_,{optional:!0});group=d(g_,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Ue).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=H(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new W;_text;_stateChanges=new D;constructor(){let e=d(Qe);e.load(or),e.load(vs),this._signalDisableRipple=!!this._parent&&Wt(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!It(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new m_(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Fe(LV,7),i&2){let o;Q(o=J())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&z("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(qt("id",r.id),_e("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),ee("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",ne]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:BV,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Te(VV),Y(0,jV,1,2,"mat-pseudo-checkbox",1),le(1),_(2,"span",2,0),le(4,1),b(),Y(5,UV,1,1,"mat-pseudo-checkbox",3),Y(6,HV,2,1,"span",4),A(7,"div",5)),i&2&&(Z(r.multiple?0:-1),y(5),Z(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),y(),Z(r.group&&r.group._inert?6:-1),y(),k("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[Sx,mo],styles:[`.mat-mdc-option {
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
`],encapsulation:2})}return t})();function Dx(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function Ex(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var wx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[Me]})}return t})();var v_=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[xs,wx,lr,Me]})}return t})();var zV=["trigger"],$V=["panel"],WV=[[["mat-select-trigger"]],"*"],GV=["mat-select-trigger","*"];function qV(t,n){if(t&1&&(_(0,"span",4),R(1),b()),t&2){let e=G();y(),wt(e.placeholder)}}function YV(t,n){t&1&&le(0)}function ZV(t,n){if(t&1&&(_(0,"span",11),R(1),b()),t&2){let e=G(2);y(),wt(e.triggerValue)}}function KV(t,n){if(t&1&&(_(0,"span",5),Y(1,YV,1,0)(2,ZV,2,1,"span",11),b()),t&2){let e=G();y(),Z(e.customTrigger?1:2)}}function XV(t,n){if(t&1){let e=Pt();_(0,"div",12,1),z("keydown",function(r){Ct(e);let o=G();return St(o._handleKeydown(r))}),le(2,1),b()}if(t&2){let e=G();pt(e.panelClass),ee("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),_e("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var QV=new g("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(V);return()=>ho(t)}}),JV=new g("MAT_SELECT_CONFIG"),eB=new g("MatSelectTrigger"),__=class{source;value;constructor(n,e){this.source=n,this.value=e}},Pf=(()=>{class t{_viewportRuler=d(tr);_changeDetectorRef=d(Le);_elementRef=d(F);_dir=d(ut,{optional:!0});_idGenerator=d(Ue);_renderer=d(we);_parentFormField=d(zl,{optional:!0});ngControl=d(Zi,{self:!0,optional:!0});_liveAnnouncer=d(xl);_defaultOptions=d(JV,{optional:!0});_animationsDisabled=Oe();_popoverLocation;_initialized=new D;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=Dx(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=Ex(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new __(this,e)}_scrollStrategyFactory=d(QV);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new D;_errorStateTracker;stateChanges=new D;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=H(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Kn.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=pn(()=>{let e=this.options;return e?e.changes.pipe(kt(e),ft(()=>ii(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(ft(()=>this.optionSelectionChanges))});openedChange=new W;_openedStream=this.openedChange.pipe(be(e=>e),re(()=>{}));_closedStream=this.openedChange.pipe(be(e=>!e),re(()=>{}));selectionChange=new W;valueChange=new W;constructor(){let e=d(_f),i=d(hl,{optional:!0}),r=d(ml,{optional:!0}),o=d(new bi("tabindex"),{optional:!0}),s=d(Rl,{optional:!0}),a=d(ps,{optional:!0,self:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new ks(e,a||this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Zl(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Ce(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Ce(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(kt(null),Ce(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe($e(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!It(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let l=this.selected;l&&a!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!It(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(a?l.select():l.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!It(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Ss?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Il(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=ii(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Ce(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),ii(...this.options.map(i=>i._stateChanges)).pipe(Ce(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=vt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Dn(o,eB,5)(o,lr,5)(o,g_,5),i&2){let s;Q(s=J())&&(r.customTrigger=s.first),Q(s=J())&&(r.options=s),Q(s=J())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&Fe(zV,5)($V,5)(hf,5),i&2){let o;Q(o=J())&&(r.trigger=o.first),Q(o=J())&&(r.panel=o.first),Q(o=J())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&z("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(_e("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),ee("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",ne],disableRipple:[2,"disableRipple","disableRipple",ne],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:xt(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",ne],placeholder:"placeholder",required:[2,"required","required",ne],multiple:[2,"multiple","multiple",ne],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",ne],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",xt],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",ne]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Re([{provide:Hl,useExisting:t},{provide:p_,useExisting:t}]),qe],ngContentSelectors:GV,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Te(WV),_(0,"div",2,0),z("click",function(){return r.open()}),_(3,"div",3),Y(4,qV,2,1,"span",4)(5,KV,3,1,"span",5),b(),_(6,"div",6)(7,"div",7),Ao(),_(8,"svg",8),A(9,"path",9),b()()()(),Ot(10,XV,3,16,"ng-template",10),z("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=vi(1);y(3),_e("id",r._valueId),y(),Z(r.empty?4:5),y(6),k("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[Ss,hf],styles:[`@keyframes _mat-select-enter {
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
`],encapsulation:2})}return t})();var xx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[Es,v_,Me,Tl,$l,v_]})}return t})();function nB(t,n){if(t&1&&A(0,"pbu-color-copy",1),t&2){let e=n.$implicit,i=G().$implicit;k("hue",i.hue)("saturation",i.saturation)("light",e)}}function iB(t,n){if(t&1&&(_(0,"div",0),ct(1,nB,1,3,"pbu-color-copy",1,Sn),b()),t&2){let e=n.$implicit;y(),dt(e.shades)}}var Ff=class t{colorState=d(Ut);colors=w(()=>this.colorState.state().colors);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-color-strips"]],decls:2,vars:0,consts:[[1,"shade-list"],[3,"hue","saturation","light"]],template:function(e,i){e&1&&ct(0,iB,3,0,"div",0,Sn),e&2&&dt(i.colors())},dependencies:[Ms],styles:["[_nghost-%COMP%]{display:flex;flex-flow:column nowrap;width:100%}.shade-list[_ngcontent-%COMP%]{display:flex;flex:1 0 auto}.shade-list[_ngcontent-%COMP%]   *[_ngcontent-%COMP%]{width:auto;height:auto;flex:1 0 auto}"]})};var Rs=class t{state=H({export:{type:"bitmap",format:"png",disposition:"sat-hua.lig",onlyDrawPalette:!0}});stateForm=io(this.state);static \u0275fac=function(e){return new(e||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})};var rB=()=>({success:!1}),oB=()=>({success:!0});function sB(t,n){if(t&1&&(_(0,"div",1)(1,"mat-form-field")(2,"mat-label"),R(3,"Image format:"),b(),_(4,"mat-select",2)(5,"mat-option",7),R(6,"PNG"),b()(),Ke(),b(),_(7,"mat-form-field")(8,"mat-label"),R(9,"Disposition:"),b(),_(10,"mat-select",2)(11,"mat-option",8),R(12,"Saturation > Hue (angle) x Light"),b(),_(13,"mat-option",9),R(14,"Saturation > Hue (temperature) x Light"),b()(),Ke(),b(),_(15,"mat-label"),R(16,"Only draw palette:"),b(),A(17,"mat-checkbox",2),Ke(),b()),t&2){let e=G();y(4),k("formField",e.exportForm.export.format),Xe(),y(6),k("formField",e.exportForm.export.disposition),Xe(),y(7),k("formField",e.exportForm.export.onlyDrawPalette),Xe()}}var Lf=class t{data=d(Yl);palette=Qd(this.data.palette);exportState=d(Rs);exportForm=this.exportState.stateForm;static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-palette-export-dialog"]],inputs:{palette:[1,"palette"]},outputs:{palette:"paletteChange"},decls:18,vars:7,consts:[["mat-dialog-title",""],[1,"fieldset"],[3,"formField"],["value","bitmap"],["value","json"],["matButton","",3,"mat-dialog-close"],["matButton","filled","cdkFocusInitial","",3,"mat-dialog-close"],["value","png"],["value","sat-hua.lig"],["value","sat-hut.lig"]],template:function(e,i){e&1&&(_(0,"h2",0),R(1),b(),_(2,"mat-dialog-content")(3,"div",1)(4,"mat-form-field")(5,"mat-label"),R(6,"Export type:"),b(),_(7,"mat-select",2)(8,"mat-option",3),R(9,"Bitmap"),b(),_(10,"mat-option",4),R(11,"JSON"),b()(),Ke(),b()(),Y(12,sB,18,3,"div",1),b(),_(13,"mat-dialog-actions")(14,"button",5),R(15,"Cancel"),b(),_(16,"button",6),R(17,"Export"),b()()),e&2&&(y(),Yt("Export palette ",i.palette().name),y(6),k("formField",i.exportForm.export.type),Xe(),y(5),Z(i.exportForm.export.type().value()==="bitmap"?12:-1),y(2),k("mat-dialog-close",qr(5,rB)),y(2),k("mat-dialog-close",qr(6,oB)))},dependencies:[Vl,hn,If,Tf,Mf,xf,Pf,lr,Xn,er,Ti,Wl],styles:["[_nghost-%COMP%]{min-width:400px;display:block}.fieldset[_ngcontent-%COMP%]{padding:2em 0 1em}.fieldset[_ngcontent-%COMP%] + .fieldset[_ngcontent-%COMP%]{border-top:1px solid #ededed}.fieldset[_ngcontent-%COMP%] > mat-form-field[_ngcontent-%COMP%]{display:block}"]})};var Vf=class t{utils=d(fn);export(n,e){switch(e.type){case"bitmap":return this._exportBitmap(n,e);case"json":return this._exportJSON(n)}}_exportBitmap(n,e){switch(e.disposition){case"sat-hua.lig":case"sat-hut.lig":return this._exportXYBitmap(n,e)}}_exportXYBitmap(n,e){let i=new Array,r=new Set,o=n.colors;o.forEach(O=>{O.shades.forEach(pe=>{let ye=[O.hue,O.saturation,pe];i.push({h:ye[0],s:ye[1]}),r.add(ye[2])})});let s=Array.from(new Set(i.map(O=>JSON.stringify(O)))).map(O=>JSON.parse(O)).toSorted((O,pe)=>e.disposition==="sat-hua.lig"?O.h%360-pe.h%360:Math.abs(O.h%360-180)-Math.abs(pe.h%360-180)).toSorted((O,pe)=>O.s-pe.s),a=Array.from(r).toSorted((O,pe)=>pe-O),l=document.createElement("canvas");l.width=1024,l.height=1024;let c=l.getContext("2d");if(!c)return;let u=l.width,f=l.height,h=s.length,m=r.size,v=Math.round(u/h),S=Math.round(f/m);a.forEach((O,pe)=>{let ye=0,Nn="";s.forEach(({h:Ni,s:kn},cr)=>{(!e.onlyDrawPalette||o.find(An=>An.hue===Ni&&An.shades.includes(O)&&An.saturation===kn))&&(Nn=`hsl(${Ni}, ${kn*100}%, ${O*100}%)`,c.fillStyle=Nn,c.fillRect(ye,pe*S,cr*v-ye+v,S),ye=(cr+1)*v)}),c.fillStyle=Nn,c.fillRect(ye,pe*S,u-ye,S)});let M=l.toDataURL("image/png");window.open(M),URL.revokeObjectURL(M)}_exportJSON(n){let e={name:n.name,colors:n.colors.flatMap(i=>i.shades.map(r=>{let o=[i.hue,i.saturation,r],s=this.utils.hslToRgb(o[0],o[1],o[2]),a=this.utils.rgbToHex(s[0],s[1],s[2]);return{hue:o[0],saturation:o[1],light:o[2],red:s[0],green:s[1],blue:s[2],code:a}}))};this.utils.exportJsonAsText(e,"colors.json")}static \u0275fac=function(e){return new(e||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})};var Bf=class t{colorState=d(Ut);exportState=d(Rs);dialog=d(As);exportService=d(Vf);openExport(){this.dialog.open(Lf,{data:{palette:this.colorState.state()}}).afterClosed().subscribe(({success:e})=>{e&&this.exportService.export(this.colorState.state(),this.exportState.state().export)})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-header"]],decls:6,vars:0,consts:[[1,"spacer"],["matButton","filled",3,"click"]],template:function(e,i){e&1&&(_(0,"mat-toolbar")(1,"h1"),R(2,"Palette Builder"),b(),A(3,"span",0),_(4,"button",1),z("click",function(){return i.openExport()}),R(5,"Export"),b()())},dependencies:[Df,hn],styles:[".spacer[_ngcontent-%COMP%]{flex:1 0 auto}"]})};var jf=class t{state=H({display:{color:"wheel",form:"edit"}});stateForm=io(this.state);static \u0275fac=function(e){return new(e||t)};static \u0275prov=E({token:t,factory:t.\u0275fac})};var aB=["*"];var lB=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"],cB=["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],dB=new g("MAT_CARD_CONFIG"),Ix=(()=>{class t{appearance;constructor(){let e=d(dB,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&ee("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:aB,decls:1,vars:0,template:function(i,r){i&1&&(Te(),le(0))},styles:[`.mat-mdc-card {
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
`],encapsulation:2})}return t})(),Tx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var Mx=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=N({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&ee("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})(),Nx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=x({type:t,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:cB,decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(i,r){i&1&&(Te(lB),le(0),mt(1,"div",0),le(2,1),Et(),le(3,2))},encapsulation:2})}return t})();var kx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=te({type:t});static \u0275inj=X({imports:[Me]})}return t})();function fB(t,n){if(t&1&&A(0,"div",5),t&2){let e=n.$implicit;an("background-color",e)}}function hB(t,n){if(t&1){let e=Pt();_(0,"button",4),z("click",function(){Ct(e);let r=G();return St(r.deletePreset(r.preset()))}),R(1,"Delete"),b()}}var Uf=class t{colorState=d(Ut);presetState=d(sr);snackBar=d(Ts);preset=it.required();loadPreset(n){let{name:e,colors:i}=n.palette;this.colorState.setColor(e,i),this.snackBar.open(`Preset "${e}" loaded`,"Close",{duration:5e3})}deletePreset(n){this.presetState.deletePreset(n),this.snackBar.open(`Preset "${n.palette.name}" deleted`,"Close",{duration:5e3})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-preset-card"]],inputs:{preset:[1,"preset"]},decls:11,vars:2,consts:[["appearance","outlined",1,"preset-card"],[1,"colors"],[1,"color",3,"backgroundColor"],["matButton",""],["matButton","",3,"click"],[1,"color"]],template:function(e,i){e&1&&(_(0,"mat-card",0)(1,"mat-card-header")(2,"mat-card-title"),R(3),b()(),_(4,"div",1),ct(5,fB,1,2,"div",2,zo),b(),_(7,"mat-card-actions"),Y(8,hB,2,0,"button",3),_(9,"button",4),z("click",function(){return i.loadPreset(i.preset())}),R(10,"Load Preset"),b()()()),e&2&&(y(3),wt(i.preset().palette.name),y(2),dt(i.preset().codes),y(3),Z(i.preset().type==="custom"?8:-1))},dependencies:[kx,Ix,Mx,Nx,Tx,hn],styles:[".colors[_ngcontent-%COMP%]{background:pink;aspect-ratio:16/9;display:flex;align-items:stretch}.colors[_ngcontent-%COMP%]   *[_ngcontent-%COMP%]{flex:1}"]})};var Ax=(t,n)=>n.palette.name;function mB(t,n){t&1&&(_(0,"h2"),R(1,"Custom Presets"),b())}function pB(t,n){if(t&1&&A(0,"pbu-preset-card",1),t&2){let e=n.$implicit;k("preset",e)}}function gB(t,n){if(t&1&&A(0,"pbu-preset-card",1),t&2){let e=n.$implicit;k("preset",e)}}var Hf=class t{colorState=d(Ut);presetState=d(sr);state=this.presetState.state;customPresets=w(()=>this.state().custom);defaultPresets=w(()=>this.state().default);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["pbu-preset-list"]],decls:9,vars:1,consts:[[1,"list"],[3,"preset"]],template:function(e,i){e&1&&(Y(0,mB,2,0,"h2"),_(1,"div",0),ct(2,pB,1,1,"pbu-preset-card",1,Ax),b(),_(4,"h2"),R(5,"Default Presets"),b(),_(6,"div",0),ct(7,gB,1,1,"pbu-preset-card",1,Ax),b()),e&2&&(Z(i.customPresets().length?0:-1),y(2),dt(i.customPresets()),y(5),dt(i.defaultPresets()))},dependencies:[Uf],styles:[".list[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,1fr);gap:1em;margin-bottom:2em}"]})};function vB(t,n){t&1&&A(0,"pbu-color-wheel")}function _B(t,n){t&1&&A(0,"pbu-color-strips")}function bB(t,n){t&1&&A(0,"pbu-color-form")}function yB(t,n){t&1&&A(0,"pbu-preset-list")}var zf=class t{globalState=d(jf);display=this.globalState.stateForm.display;static \u0275fac=function(e){return new(e||t)};static \u0275cmp=x({type:t,selectors:[["app-root"]],decls:17,vars:4,consts:[[1,"col-palette"],[3,"formField"],["value","wheel"],["value","strips"],[1,"col-colors"],["value","edit"],["value","presets"]],template:function(e,i){if(e&1&&(A(0,"pbu-header"),_(1,"div",0)(2,"mat-select",1)(3,"mat-option",2),R(4,"Color Wheel"),b(),_(5,"mat-option",3),R(6,"Strips"),b()(),Ke(),Y(7,vB,1,0,"pbu-color-wheel")(8,_B,1,0,"pbu-color-strips"),b(),_(9,"div",4)(10,"mat-select",1)(11,"mat-option",5),R(12,"Build Palette"),b(),_(13,"mat-option",6),R(14,"Preset List"),b()(),Ke(),Y(15,bB,1,0,"pbu-color-form")(16,yB,1,0,"pbu-preset-list"),b()),e&2){let r,o;y(2),k("formField",i.display.color),Xe(),y(5),Z((r=i.display.color().value())==="wheel"?7:r==="strips"?8:-1),y(3),k("formField",i.display.form),Xe(),y(5),Z((o=i.display.form().value())==="edit"?15:o==="presets"?16:-1)}},dependencies:[gf,Af,Ff,Cx,xx,Pf,lr,er,Bf,Hf],styles:['pbu-header[_ngcontent-%COMP%]{grid-area:title}[_nghost-%COMP%]{display:grid;grid-template-rows:min-content 1fr;grid-template-columns:1fr 2fr;grid-template-areas:"title title" "left right";gap:2em;padding:1em;height:100%}.col-palette[_ngcontent-%COMP%]{border:1px solid #dfdfdf;border-radius:10px;display:grid;grid-template-rows:min-content 1fr;gap:1em;padding:1em 2em;overflow-x:hidden;overflow-y:auto}.col-colors[_ngcontent-%COMP%]{border:1px solid #dfdfdf;border-radius:10px;padding:1em 2em;display:grid;grid-template-rows:min-content 1fr;gap:1em;overflow-x:hidden;overflow-y:auto}pbu-color-wheel[_ngcontent-%COMP%]{margin:2em 0}']})};rg(zf,x0).catch(t=>console.error(t));
