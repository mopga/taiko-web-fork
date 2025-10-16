(function(global){
        "use strict";

        var availabilityCache = new Map();
        var HEAD_OPTIONS = { method: "HEAD" };
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

        function checkAvailability(url){
                if(!url){
                        return Promise.resolve(false);
                }
                if(url.startsWith("data:") || url.startsWith("blob:")){
                        return Promise.resolve(true);
                }
                var cached = availabilityCache.get(url);
                if(cached){
                        return cached;
                }
                var promise = fetch(url, HEAD_OPTIONS).then(response => {
                        return response && response.ok;
                }).catch(() => false);
                availabilityCache.set(url, promise);
                return promise;
        }

        function createCandidate(source){
                var file = toRemoteFile(source);
                var url = toUrl(file);
                if(!file || !url){
                        return null;
                }
                return { file: file, url: url };
        }

        function tryCandidates(candidates, index){
                if(index >= candidates.length){
                        return Promise.resolve(null);
                }
                var candidate = candidates[index];
                if(!candidate){
                        return tryCandidates(candidates, index + 1);
                }
                return checkAvailability(candidate.url).then(available => {
                        if(available){
                                return candidate.file;
                        }
                        return tryCandidates(candidates, index + 1);
                });
        }

        function resolvePreviewSource(song){
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
                return tryCandidates(candidates, 0);
        }

        function clearCache(){
                availabilityCache.clear();
        }

        global.previewUtils = global.previewUtils || {};
        global.previewUtils.resolveSongPreview = function(song){
                return resolvePreviewSource(song);
        };
        global.previewUtils.clearPreviewCache = clearCache;
})(this);
