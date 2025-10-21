(function(global){
        "use strict";

        var availabilityCache = new Map();
        var RANGE_HEADERS = { Range: "bytes=0-0" };
        var RANGE_STATUS_RETRY = { 403: true, 405: true, 501: true };
        var RANGE_EXTENSION_REGEX = /\.(?:ogg|mp3)(?=$|[?#])/i;
        var resolveAudio = typeof global.resolveAudioSrc === "function" ? global.resolveAudioSrc : null;
        var toRemote = global.songAudio && typeof global.songAudio.toRemoteFile === "function" ? global.songAudio.toRemoteFile : null;

        function toUrl(source){
                if(!source){
                        return null;
                }
                if(resolveAudio){
                        try{
                                var resolved = resolveAudio(source);
                                if(resolved && resolved !== "muted"){
                                        return resolved;
                                }
                        }catch(e){}
                }
                if(typeof source === "string"){
                        return source !== "muted" ? source : null;
                }
                if(source === "muted"){
                        return null;
                }
                if(source && typeof source.url === "string"){
                        return source.url;
                }
                return null;
        }

        function toRemoteFile(source){
                if(!source){
                        return null;
                }
                if(source === "muted"){
                        return null;
                }
                if(typeof source === "string"){
                        if(source === "muted"){
                                return null;
                        }
                        if(toRemote){
                                var remote = toRemote(source);
                                if(remote && remote !== "muted"){
                                        return remote;
                                }
                        }
                        return new RemoteFile(source);
                }
                if(source instanceof RemoteFile){
                        return source;
                }
                if(toRemote){
                        var converted = toRemote(source);
                        if(converted && converted !== "muted"){
                                return converted;
                        }
                }
                if(source && typeof source.url === "string"){
                        return new RemoteFile(source.url);
                }
                return null;
        }

        function replaceFilename(url, filename){
                if(!url){
                        return null;
                }
                var queryIndex = url.indexOf("?");
                var suffix = "";
                if(queryIndex !== -1){
                        suffix = url.slice(queryIndex);
                        url = url.slice(0, queryIndex);
                }
                var slashIndex = url.lastIndexOf("/");
                if(slashIndex === -1){
                        return filename + suffix;
                }
                return url.slice(0, slashIndex + 1) + filename + suffix;
        }

        function isAbortError(error){
                return error && (error.name === "AbortError" || error.code === 20);
        }

        function hasAudioExtension(url){
                if(!url){
                        return false;
                }
                var cleanUrl = url.split(/[?#]/)[0];
                return RANGE_EXTENSION_REGEX.test(cleanUrl);
        }

        function shouldTryRange(url, response){
                if(!hasAudioExtension(url)){
                        return false;
                }
                if(!response){
                        return true;
                }
                if(response.status === 0){
                        return true;
                }
                return !!RANGE_STATUS_RETRY[response.status];
        }

        function tryRangeRequest(url, signal){
                if(!hasAudioExtension(url)){
                        return Promise.resolve(false);
                }
                return fetch(url, {
                        method: "GET",
                        headers: RANGE_HEADERS,
                        signal: signal
                }).then(response => {
                        return response && response.ok;
                }).catch(error => {
                        if(isAbortError(error)){
                                throw error;
                        }
                        return false;
                });
        }

        function performAvailabilityCheck(url, options){
                var signal = options && options.signal;
                return fetch(url, {
                        method: "HEAD",
                        signal: signal
                }).then(response => {
                        if(response && response.ok){
                                return true;
                        }
                        if(shouldTryRange(url, response)){
                                return tryRangeRequest(url, signal);
                        }
                        return false;
                }).catch(error => {
                        if(isAbortError(error)){
                                throw error;
                        }
                        if(shouldTryRange(url)){
                                return tryRangeRequest(url, signal);
                        }
                        return false;
                });
        }

        function checkAvailability(url, options){
                if(!url){
                        return Promise.resolve(false);
                }
                if(url.startsWith("data:") || url.startsWith("blob:")){
                        return Promise.resolve(true);
                }
                var cached = availabilityCache.get(url);
                if(typeof cached === "boolean"){
                        return Promise.resolve(cached);
                }
                var optionsObj = options || {};
                return performAvailabilityCheck(url, optionsObj).then(result => {
                        availabilityCache.set(url, result);
                        return result;
                }).catch(error => {
                        if(isAbortError(error)){
                                availabilityCache.delete(url);
                                throw error;
                        }
                        availabilityCache.set(url, false);
                        return false;
                });
        }

        function createCandidate(source){
                var file = toRemoteFile(source);
                var url = toUrl(file);
                if(!file || !url){
                        return null;
                }
                return { file: file, url: url };
        }

        function tryCandidates(candidates, index, options){
                if(index >= candidates.length){
                        return Promise.resolve(null);
                }
                var candidate = candidates[index];
                if(!candidate){
                        return tryCandidates(candidates, index + 1, options);
                }
                return checkAvailability(candidate.url, options).then(available => {
                        if(available){
                                return candidate.file;
                        }
                        return tryCandidates(candidates, index + 1, options);
                });
        }

        function resolvePreviewSource(song, options){
                if(!song || !song.previewMusic){
                        return Promise.resolve(null);
                }
                var baseUrl = toUrl(song.previewMusic);
                if(!baseUrl){
                        return Promise.resolve(null);
                }
                var candidates = [];
                var seen = new Set();
                ["preview.ogg", "preview.mp3"].forEach(filename => {
                        var url = replaceFilename(baseUrl, filename);
                        if(!url || seen.has(url)){
                                return;
                        }
                        seen.add(url);
                        var source = baseUrl === url ? song.previewMusic : url;
                        candidates.push(createCandidate(source));
                });
                return tryCandidates(candidates, 0, options);
        }

        function clearCache(){
                availabilityCache.clear();
        }

        global.previewUtils = global.previewUtils || {};
        global.previewUtils.resolveSongPreview = function(song, options){
                return resolvePreviewSource(song, options);
        };
        global.previewUtils.clearPreviewCache = clearCache;
})(this);
