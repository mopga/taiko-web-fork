(function(global){
        "use strict";

        function createDeferred(){
                var deferred = {};
                deferred.promise = new Promise(function(resolve, reject){
                        deferred.resolve = resolve;
                        deferred.reject = reject;
                });
                return deferred;
        }

        function createRequestPool(limit){
                var maxConcurrency = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 1;
                var queue = [];
                var active = 0;

                function schedule(){
                        if(active >= maxConcurrency){
                                return;
                        }
                        var task = queue.shift();
                        if(!task){
                                return;
                        }
                        active++;
                        var result;
                        try{
                                result = task.fn();
                        }catch(error){
                                active--;
                                task.reject(error);
                                schedule();
                                return;
                        }
                        Promise.resolve(result).then(function(value){
                                active--;
                                task.resolve(value);
                                schedule();
                        }).catch(function(error){
                                active--;
                                task.reject(error);
                                schedule();
                        });
                }

                return {
                        enqueue: function(fn){
                                return new Promise(function(resolve, reject){
                                        queue.push({fn: fn, resolve: resolve, reject: reject});
                                        schedule();
                                });
                        },
                };
        }

        function normaliseId(id){
                if(typeof id === "string" && id){
                        return id;
                }
                if(typeof id === "number" && !Number.isNaN(id)){
                        return String(id);
                }
                if(id && typeof id === "object" && typeof id.id === "string"){
                        return id.id;
                }
                return null;
        }

        function toDetailKey(detail, fallback){
                if(detail && typeof detail === "object"){
                        var stable = normaliseId(detail.stableId);
                        if(stable){
                                return stable;
                        }
                        var id = normaliseId(detail.id);
                        if(id){
                                return id;
                        }
                }
                return normaliseId(fallback);
        }

        function parseJsonSafe(text){
                if(typeof text !== "string" || !text){
                        return null;
                }
                try{
                        return JSON.parse(text);
                }catch(error){
                        return null;
                }
        }

        function mapDetailArray(array){
                var map = {};
                if(!Array.isArray(array)){
                        return map;
                }
                array.forEach(function(entry){
                        var key = toDetailKey(entry);
                        if(key){
                                map[key] = entry;
                        }
                });
                return map;
        }

        function SongCatalogDataSource(options){
                options = options || {};
                this.pageSize = Number.isFinite(options.pageSize) && options.pageSize > 0 ? Math.min(200, Math.floor(options.pageSize)) : 100;
                this.maxConcurrency = Number.isFinite(options.maxConcurrency) && options.maxConcurrency > 0 ? Math.floor(options.maxConcurrency) : 6;
                this.detailBatchSize = Number.isFinite(options.detailBatchSize) && options.detailBatchSize > 0 ? Math.min(50, Math.floor(options.detailBatchSize)) : 50;
                this.hardPageCapDefault = Number.isFinite(options.hardPageCap) && options.hardPageCap > 0 ? Math.floor(options.hardPageCap) : 5;
                this.minPageCap = Number.isFinite(options.minPageCap) && options.minPageCap > 0 ? Math.floor(options.minPageCap) : 5;

                this.requestPool = createRequestPool(this.maxConcurrency);

                this.pageCache = new Map();
                this.detailCache = new Map();
                this.detailCacheByLegacy = new Map();
                this.detailWaiters = new Map();
                this.pendingDetailQueue = [];
                this.pendingDetailSet = new Set();
                this.detailFlushScheduled = false;
                this.detailFlushInProgress = false;
                this.batchEndpointAvailable = true;

                this.items = [];
                this.totalCount = null;
                this.hardPageCap = this.hardPageCapDefault;
                this.nextPage = 1;
                this.reachedEnd = false;
                this.loadingMore = false;
        }

        SongCatalogDataSource.prototype.updateTotalCountFromHeaders = function(headers){
                if(!headers || typeof headers.get !== "function"){
                        return;
                }
                var value = headers.get("X-Total-Count");
                if(!value){
                        return;
                }
                var parsed = parseInt(value, 10);
                if(Number.isFinite(parsed) && parsed >= 0){
                        this.totalCount = parsed;
                        var computed = Math.ceil(parsed / this.pageSize);
                        if(Number.isFinite(computed) && computed > 0){
                                var minCap = this.minPageCap;
                                if(!Number.isFinite(minCap) || minCap <= 0){
                                        minCap = 1;
                                }
                                this.hardPageCap = Math.min(this.hardPageCapDefault, Math.max(minCap, computed));
                        }else{
                                this.hardPageCap = this.hardPageCapDefault;
                        }
                }
        };

        SongCatalogDataSource.prototype.hasMore = function(){
                if(this.reachedEnd){
                        return false;
                }
                return this.nextPage <= this.hardPageCap;
        };

        SongCatalogDataSource.prototype.getItems = function(){
                return this.items.slice();
        };

        SongCatalogDataSource.prototype.storeDetail = function(detail, requestedId){
                if(!detail || typeof detail !== "object"){
                        return;
                }
                var key = toDetailKey(detail, requestedId);
                if(!key){
                        return;
                }
                this.detailCache.set(key, detail);
                if(typeof detail.legacy_id === "number"){
                        this.detailCacheByLegacy.set(String(detail.legacy_id), detail);
                }
                if(requestedId && requestedId !== key){
                        this.detailCache.set(requestedId, detail);
                }
        };

        SongCatalogDataSource.prototype.getDetailFromCache = function(id){
                var key = normaliseId(id);
                if(!key){
                        return null;
                }
                if(this.detailCache.has(key)){
                        return this.detailCache.get(key);
                }
                if(this.detailCacheByLegacy.has(key)){
                        return this.detailCacheByLegacy.get(key);
                }
                return null;
        };

        SongCatalogDataSource.prototype.loadNextPage = function(){
                var _this = this;
                if(this.loadingMore){
                        return this.loadingMore;
                }
                if(!this.hasMore()){
                        return Promise.resolve([]);
                }
                var pageNumber = this.nextPage;
                this.loadingMore = this.requestPool.enqueue(function(){
                        var cachedPage = _this.pageCache.get(pageNumber);
                        var usedCached = false;
                        var url = "api/songs?page=" + pageNumber + "&limit=" + _this.pageSize;
                        var retryAttempted = false;

                        function appendBypassParameter(baseUrl){
                                return baseUrl + (baseUrl.indexOf("?") === -1 ? "?" : "&") + "_bypass=" + Date.now();
                        }

                        function fetchPage(currentUrl, bypass){
                                var options = {
                                        method: "GET",
                                        credentials: "same-origin",
                                };
                                if(bypass){
                                        options.cache = "no-store";
                                }
                                return fetch(currentUrl, options).then(function(response){
                                        _this.updateTotalCountFromHeaders(response.headers);
                                        if(response.status === 304){
                                                if(!_this.pageCache.has(pageNumber)){
                                                        if(!retryAttempted){
                                                                retryAttempted = true;
                                                                return fetchPage(appendBypassParameter(url), true);
                                                        }
                                                        return [];
                                                }
                                                usedCached = true;
                                                return cachedPage ? cachedPage.slice() : [];
                                        }
                                        if(!response.ok){
                                                var error = new Error(currentUrl + " (" + response.status + ")");
                                                error.status = response.status;
                                                throw error;
                                        }
                                        return response.json().catch(function(){
                                                return [];
                                        });
                                });
                        }

                        return fetchPage(url, false).then(function(data){
                                if(!Array.isArray(data)){
                                        data = [];
                                }
                                _this.pageCache.set(pageNumber, data.slice());
                                if(!usedCached || !Array.isArray(cachedPage)){
                                        Array.prototype.push.apply(_this.items, data);
                                }
                                _this.nextPage += 1;
                                if(data.length < _this.pageSize || _this.nextPage > _this.hardPageCap){
                                        _this.reachedEnd = true;
                                }
                                return data.slice();
                        }).catch(function(error){
                                throw error;
                        }).finally(function(){
                                _this.loadingMore = false;
                        });
                });
                return this.loadingMore;
        };

        SongCatalogDataSource.prototype.resolveDetailWaiters = function(id, value, reject){
                var key = normaliseId(id);
                if(!key){
                        return;
                }
                var waiters = this.detailWaiters.get(key);
                if(!waiters){
                        return;
                }
                this.detailWaiters.delete(key);
                waiters.forEach(function(deferred){
                        try{
                                if(reject){
                                        deferred.reject(value);
                                }else{
                                        deferred.resolve(value);
                                }
                        }catch(e){}
                });
        };

        SongCatalogDataSource.prototype.queueDetail = function(id){
                var key = normaliseId(id);
                if(!key){
                        return Promise.resolve(null);
                }
                var cached = this.getDetailFromCache(key);
                if(cached){
                        return Promise.resolve(cached);
                }
                var deferred = createDeferred();
                if(this.detailWaiters.has(key)){
                        this.detailWaiters.get(key).push(deferred);
                }else{
                        this.detailWaiters.set(key, [deferred]);
                        if(!this.pendingDetailSet.has(key)){
                                this.pendingDetailSet.add(key);
                                this.pendingDetailQueue.push(key);
                        }
                }
                this.scheduleDetailFlush();
                return deferred.promise;
        };

        SongCatalogDataSource.prototype.scheduleDetailFlush = function(){
                if(this.detailFlushInProgress || this.detailFlushScheduled){
                        return;
                }
                var _this = this;
                this.detailFlushScheduled = true;
                var scheduleFn = function(){
                        _this.detailFlushScheduled = false;
                        _this.flushDetailQueue();
                };
                if(typeof requestIdleCallback === "function"){
                        requestIdleCallback(scheduleFn, {timeout: 120});
                }else{
                        setTimeout(scheduleFn, 50);
                }
        };

        SongCatalogDataSource.prototype.flushDetailQueue = function(){
                var _this = this;
                if(this.detailFlushInProgress){
                        return;
                }
                if(!this.pendingDetailQueue.length){
                        return;
                }
                this.detailFlushInProgress = true;
                (function process(){
                        if(!_this.pendingDetailQueue.length){
                                _this.detailFlushInProgress = false;
                                return;
                        }
                        var batch = [];
                        while(batch.length < _this.detailBatchSize && _this.pendingDetailQueue.length){
                                var id = _this.pendingDetailQueue.shift();
                                if(!_this.pendingDetailSet.has(id)){
                                        continue;
                                }
                                _this.pendingDetailSet.delete(id);
                                if(_this.detailCache.has(id)){
                                        var cached = _this.detailCache.get(id);
                                        _this.resolveDetailWaiters(id, cached, false);
                                        continue;
                                }
                                if(_this.detailCacheByLegacy.has(id)){
                                        var legacyCached = _this.detailCacheByLegacy.get(id);
                                        _this.resolveDetailWaiters(id, legacyCached, false);
                                        continue;
                                }
                                batch.push(id);
                        }
                        if(!batch.length){
                                if(_this.pendingDetailQueue.length){
                                        process();
                                }else{
                                        _this.detailFlushInProgress = false;
                                }
                                return;
                        }
                        _this.performDetailBatch(batch).then(function(map){
                                return _this.handleDetailBatchResult(batch, map);
                        }).catch(function(error){
                                batch.forEach(function(id){
                                        _this.resolveDetailWaiters(id, error, true);
                                });
                        }).finally(function(){
                                if(_this.pendingDetailQueue.length){
                                        process();
                                }else{
                                        _this.detailFlushInProgress = false;
                                }
                        });
                })();
        };

        SongCatalogDataSource.prototype.performDetailBatch = function(ids){
                var _this = this;
                if(!Array.isArray(ids) || !ids.length){
                        return Promise.resolve({});
                }
                if(!this.batchEndpointAvailable){
                        return Promise.resolve({});
                }
                var query = ids.map(function(id){
                        return encodeURIComponent(id);
                }).join(",");
                var url = "api/songs/details?ids=" + query;
                return this.requestPool.enqueue(function(){
                        return fetch(url, {
                                method: "GET",
                                credentials: "same-origin",
                        }).then(function(response){
                                if(response.status === 404 || response.status === 405){
                                        _this.batchEndpointAvailable = false;
                                        return {};
                                }
                                if(!response.ok){
                                        var error = new Error(url + " (" + response.status + ")");
                                        error.status = response.status;
                                        throw error;
                                }
                                return response.text().then(function(text){
                                        var parsed = parseJsonSafe(text);
                                        return mapDetailArray(parsed);
                                });
                        });
                });
        };

        SongCatalogDataSource.prototype.fetchSingleDetail = function(id){
                var _this = this;
                var key = normaliseId(id);
                if(!key){
                        return Promise.resolve(null);
                }
                return this.requestPool.enqueue(function(){
                        var url = "api/song/" + encodeURIComponent(key);
                        return fetch(url, {
                                method: "GET",
                                credentials: "same-origin",
                        }).then(function(response){
                                if(response.status === 404){
                                        return null;
                                }
                                if(!response.ok){
                                        var error = new Error(url + " (" + response.status + ")");
                                        error.status = response.status;
                                        throw error;
                                }
                                return response.text().then(function(text){
                                        var parsed = parseJsonSafe(text);
                                        if(parsed && typeof parsed === "object"){
                                                _this.storeDetail(parsed, key);
                                                return parsed;
                                        }
                                        return null;
                                });
                        });
                });
        };

        SongCatalogDataSource.prototype.handleDetailBatchResult = function(ids, map){
                var _this = this;
                if(typeof map !== "object" || map === null){
                        map = {};
                }
                var keys = Object.keys(map);
                if(keys.length){
                        keys.forEach(function(key){
                                var detail = map[key];
                                if(detail && typeof detail === "object"){
                                        _this.storeDetail(detail, key);
                                }
                        });
                }
                var promises = [];
                ids.forEach(function(id){
                        var cached = _this.getDetailFromCache(id);
                        if(cached){
                                _this.resolveDetailWaiters(id, cached, false);
                                return;
                        }
                        promises.push(
                                _this.fetchSingleDetail(id).then(function(detail){
                                        if(detail){
                                                _this.storeDetail(detail, id);
                                                _this.resolveDetailWaiters(id, detail, false);
                                        }else{
                                                _this.resolveDetailWaiters(id, null, false);
                                        }
                                }).catch(function(error){
                                        _this.resolveDetailWaiters(id, error, true);
                                })
                        );
                });
                if(!promises.length){
                        return Promise.resolve();
                }
                return Promise.all(promises).then(function(){ return undefined; });
        };

        global.SongCatalogDataSource = SongCatalogDataSource;
})(this);
