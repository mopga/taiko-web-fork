(function(global){
        "use strict";

        function toRemoteFile(source){
                if(!source){
                        return null;
                }
                if(source instanceof RemoteFile){
                        return source;
                }
                if(typeof source === "string"){
                        if(source === "muted"){
                                return source;
                        }
                        return new RemoteFile(source);
                }
                if(source && source.url && typeof source.arrayBuffer !== "function"){
                        return new RemoteFile(source.url);
                }
                return source;
        }

        function normalizeSongAudio(song){
                if(!song){
                        return null;
                }
                if(song.music || song.audio){
                        var audioSource = song.music || song.audio;
                        song.music = toRemoteFile(audioSource);
                }
                if(song.previewMusic){
                        song.previewMusic = toRemoteFile(song.previewMusic);
                }
                return song.music || null;
        }

        global.songAudio = global.songAudio || {};
        global.songAudio.toRemoteFile = toRemoteFile;
        global.songAudio.normalizeSongAudio = normalizeSongAudio;
})(this);
