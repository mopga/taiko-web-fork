(function(global){
        "use strict";

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

        function buildPreviewCandidates(song){
                var candidates = [];
                var seen = new Set();

                function pushCandidate(source){
                        var file = toRemoteFile(source);
                        var url = toUrl(file);
                        if(!file || !url || seen.has(url)){
                                return;
                        }
                        seen.add(url);
                        candidates.push(file);
                }

                if(song && song.paths && typeof song.paths.preview_url === "string" && song.paths.preview_url){
                        pushCandidate(song.paths.preview_url);
                }
                if(song && song.previewMusic){
                        pushCandidate(song.previewMusic);
                }
                var dirUrl = song && song.paths && song.paths.dir_url;
                if(typeof dirUrl === "string" && dirUrl){
                        if(dirUrl.slice(-1) !== "/"){
                                dirUrl += "/";
                        }
                        ["preview.ogg", "preview.mp3"].forEach(function(filename){
                                pushCandidate(dirUrl + filename);
                        });
                }
                return candidates;
        }

        function resolveSongPreview(song){
                if(!song){
                        return Promise.resolve(null);
                }
                if(song.preview_available === false){
                        song.__previewFallbacks = [];
                        return Promise.resolve(null);
                }
                var candidates = buildPreviewCandidates(song);
                song.__previewFallbacks = candidates.slice(1);
                if(!candidates.length){
                        return Promise.resolve(null);
                }
                return Promise.resolve(candidates[0]);
        }

        global.previewUtils = global.previewUtils || {};
        global.previewUtils.resolveSongPreview = function(song){
                        return resolveSongPreview(song);
        };
        global.previewUtils.clearPreviewCache = function(){ };
})(this);
