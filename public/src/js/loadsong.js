function isDesktopEnvironment(){
        return typeof window !== "undefined" && window.desktop && typeof window.desktop === "object";
}

function coerceNumber(value, fallback){
        var number = Number(value);
        if(number === number && isFinite(number)){
                return number;
        }
        return fallback;
}

function normaliseRelativePath(value){
        return String(value || "").replace(/\\/g, "/");
}

function ensureSongsBase(){
        var base = gameConfig && typeof gameConfig.songs_baseurl === "string" ? gameConfig.songs_baseurl : "/songs/";
        if(!base){
                base = "/songs/";
        }
        if(base.slice(-1) !== "/"){
                base += "/";
        }
        return base;
}

function resolvePlaylistAudioUrl(baseUrl, playlistPath, segment){
        if(!segment || typeof segment !== "object"){
                return null;
        }
        var audioName = segment.audio;
        if(typeof audioName !== "string" || !audioName.trim()){
                return null;
        }
        var normalisedAudio = audioName.trim();
        var normalisedTja = normaliseRelativePath(segment.tja_path);
        var directory = "";
        if(normalisedTja){
                var idx = normalisedTja.lastIndexOf("/");
                if(idx !== -1){
                        directory = normalisedTja.slice(0, idx + 1);
                }
        }
        if(!directory && playlistPath){
                var normalisedPlaylist = normaliseRelativePath(playlistPath);
                var playlistIdx = normalisedPlaylist.lastIndexOf("/");
                if(playlistIdx !== -1){
                        directory = normalisedPlaylist.slice(0, playlistIdx + 1);
                }
        }
        var relative = directory + normalisedAudio;
        if(relative.startsWith("http://") || relative.startsWith("https://") || relative.startsWith("file://")){
                return relative;
        }
        if(relative.slice(0, 1) === "/"){
                relative = relative.slice(1);
        }
        return baseUrl + relative;
}

class PlaylistCourseSound{
        constructor(gain, segments, totalDuration){
                this.gain = gain;
                this.soundBuffer = gain.soundBuffer;
                this.segments = Array.isArray(segments) ? segments : [];
                this.duration = totalDuration || 0;
                this.cfg = null;
        }
        getTime(){
                return this.soundBuffer.getTime();
        }
        convertTime(time, absolute){
                return this.soundBuffer.convertTime(time || 0, absolute);
        }
        scheduleSegments(baseTime, seek, until){
                var endLimit = seek + until;
                for(var i = 0; i < this.segments.length; i++){
                        var segment = this.segments[i];
                        if(!segment || !segment.sound){
                                continue;
                        }
                        var sound = segment.sound;
                        var start = segment.start || 0;
                        var segmentEnd = start + sound.duration;
                        if(segmentEnd <= seek){
                                continue;
                        }
                        if(start >= endLimit){
                                break;
                        }
                        var playWindowStart = Math.max(start, seek);
                        var playWindowEnd = Math.min(segmentEnd, endLimit);
                        var playDuration = playWindowEnd - playWindowStart;
                        if(!(playDuration > 0)){
                                continue;
                        }
                        var segmentSeek = playWindowStart - start;
                        var relativeStart = playWindowStart - seek;
                        var playStartTime = baseTime + relativeStart;
                        var untilValue = segmentSeek + playDuration;
                        sound.play(playStartTime, true, segmentSeek, untilValue);
                }
        }
        play(time, absolute, seek, until){
                var seekValue = typeof seek === "number" && seek === seek ? seek : 0;
                var untilValue = typeof until === "number" && until === until ? until : this.duration;
                if(untilValue <= 0){
                        return;
                }
                this.stop(time, absolute);
                var baseTime = this.convertTime(time || 0, absolute);
                this.cfg = {
                        started: baseTime,
                        seek: seekValue,
                        until: untilValue
                };
                this.scheduleSegments(baseTime, seekValue, untilValue);
        }
        stop(time, absolute){
                        var stopAt = this.convertTime(time || 0, absolute);
                        for(var i = 0; i < this.segments.length; i++){
                                var segment = this.segments[i];
                                if(segment && segment.sound){
                                        segment.sound.stop(stopAt, true);
                                }
                        }
                        this.cfg = null;
        }
        pause(time, absolute){
                if(!this.cfg){
                        return;
                }
                var stopAt = this.convertTime(time || 0, absolute);
                var elapsed = stopAt - this.cfg.started;
                this.cfg.pauseSeek = this.cfg.seek + Math.max(0, elapsed);
                for(var i = 0; i < this.segments.length; i++){
                        var segment = this.segments[i];
                        if(segment && segment.sound){
                                segment.sound.pause(stopAt, true);
                        }
                }
        }
        resume(time, absolute){
                if(!this.cfg || typeof this.cfg.pauseSeek !== "number"){
                        return;
                }
                var resumeSeek = this.cfg.pauseSeek;
                var untilValue = this.cfg.until;
                this.play(time, absolute, resumeSeek, untilValue);
        }
        playLoop(time, absolute, seek1, seek2, until){
                var seekValue = typeof seek1 === "number" && seek1 === seek1 ? seek1 : 0;
                var untilValue = typeof until === "number" && until === until ? until : this.duration;
                this.play(time, absolute, seekValue, untilValue);
        }
        clean(){
                for(var i = 0; i < this.segments.length; i++){
                        var segment = this.segments[i];
                        if(segment && segment.sound){
                                segment.sound.clean();
                        }
                }
                this.cfg = null;
        }
}

class LoadSong{
	constructor(...args){
		this.init(...args)
	}
	init(selectedSong, autoPlayEnabled, multiplayer, touchEnabled){
		this.selectedSong = selectedSong
		this.autoPlayEnabled = autoPlayEnabled
		this.multiplayer = multiplayer
		this.touchEnabled = touchEnabled
		var resolution = settings.getItem("resolution")
		this.imgScale = 1
		if(resolution === "medium"){
			this.imgScale = 0.75
		}else if(resolution === "low"){
			this.imgScale = 0.5
		}else if(resolution === "lowest"){
			this.imgScale = 0.25
		}
		
		loader.changePage("loadsong", true)
		var loadingText = document.getElementById("loading-text")
		loadingText.appendChild(document.createTextNode(strings.loading))
		loadingText.setAttribute("alt", strings.loading)
		if(multiplayer){
			var cancel = document.getElementById("p2-cancel-button")
			cancel.appendChild(document.createTextNode(strings.cancel))
			cancel.setAttribute("alt", strings.cancel)
		}
		this.run()
		pageEvents.send("load-song", {
			selectedSong: selectedSong,
			autoPlayEnabled: autoPlayEnabled,
			multiplayer: multiplayer,
			touchEnabled: touchEnabled
		})
	}
	run(){
		var song = this.selectedSong
		var id = song.folder
		var songObj
                this.promises = []
                this._legacyNotesQueued = false
                this._legacyNotesPromise = null
		if(id !== "calibration"){
			assets.sounds["v_start"].play()
			assets.songs.forEach(song => {
				if(song.id === id){
					songObj = song
				}else{
					if(song.sound){
						song.sound.clean()
						delete song.sound
					}
					delete song.lyricsData
				}
			})
		}else{
			songObj = {
				music: "muted",
				custom: true
			}
		}
                songAudio.normalizeSongAudio(songObj)
                songAudio.normalizeSongAudio(song)
                this.songObj = songObj
                if(this.songObj && typeof this.songObj.duration_ms === "number"){
                        this.selectedSong.duration_ms = this.songObj.duration_ms
                }
                song.songBg = this.randInt(1, 5)
                song.songStage = this.randInt(1, 3)
                song.donBg = this.randInt(1, 6)
		if(this.songObj && this.songObj.category_id === 9){
			 LoadSong.insertBackgroundVideo(this.songObj.id)
				}
		if(song.songSkin && song.songSkin.name){
			var imgLoad = []
			for(var type in song.songSkin){
				var value = song.songSkin[type]
				if(["song", "stage", "don"].indexOf(type) !== -1 && value && value !== "none"){
					var filename = "bg_" + type + "_" + song.songSkin.name
					if(value === "static"){
						imgLoad.push({
							filename: filename,
							type: type
						})
					}else{
						imgLoad.push({
							filename: filename + "_a",
							type: type
						})
						imgLoad.push({
							filename: filename + "_b",
							type: type
						})
					}
					if(type === "don"){
						song.donBg = null
					}else if(type === "song"){
						song.songBg = null
					}else if(type === "stage"){
						song.songStage = null
					}
				}
			}
			var skinBase = gameConfig.assets_baseurl + "song_skins/"
			for(var i = 0; i < imgLoad.length; i++){
				let filename = imgLoad[i].filename
				let prefix = song.songSkin.prefix || ""
				if((prefix + filename) in assets.image){
					continue
				}
				let img = document.createElement("img")
				let force = imgLoad[i].type === "song" && this.touchEnabled
				if(!songObj.custom){
					img.crossOrigin = "anonymous"
				}
				let promise = pageEvents.load(img)
				this.addPromise(promise.then(() => {
					return this.scaleImg(img, filename, prefix, force)
				}), songObj.custom ? filename + ".png" : skinBase + filename + ".png")
				if(songObj.custom){
					this.addPromise(song.songSkin[filename + ".png"].blob().then(blob => {
						img.src = URL.createObjectURL(blob)
					}), song.songSkin[filename + ".png"].url)
				}else{
					img.src = skinBase + filename + ".png"
				}
			}
		}
                this.loadSongBg(id)

                var selection = {
                        title: this.selectedSong.title,
                        difficulty: this.selectedSong.difficulty,
                        course: this.selectedSong.difficulty,
                        rank: this.selectedSong.rank || this.selectedSong.difficulty,
                        category: this.selectedSong.category,
                        mode: this.selectedSong.mode || songObj.mode || songObj.default_mode,
                        stars: this.selectedSong.stars
                }

                this._desktopPlaylistState = {enabled: false, candidate: null, preparing: false, prepared: false}
                if(isDesktopEnvironment()){
                        var desktopCandidate = this.detectDesktopPlaylistCourse(songObj, selection)
                        if(desktopCandidate){
                                this._desktopPlaylistState.enabled = true
                                this._desktopPlaylistState.candidate = desktopCandidate
                                songObj.music = "muted"
                                if(songObj.sound){
                                        delete songObj.sound
                                }
                        }
                }
                var notesPromise
                if(typeof loadNotesForSong === "function"){
                        try{
                                var maybePromise = loadNotesForSong(songObj, selection)
                                if(maybePromise && typeof maybePromise.then === "function"){
                                        notesPromise = maybePromise
                                }else{
                                        notesPromise = Promise.resolve(maybePromise || null)
                                }
                        }catch(e){
                                console.warn("notes-loader: call failed", e)
                                notesPromise = Promise.resolve(null)
                        }
                }else{
                        notesPromise = Promise.resolve(null)
                }

                const notesHandling = Promise.resolve(notesPromise).then(result => {
                        if(result && result.rest === true){
                                if(Array.isArray(result.notes) && result.notes.length){
                                        if(result.modeKey){
                                                this.selectedSong.mode = result.modeKey
                                                songObj.mode = result.modeKey
                                        }
                                        if(result.meta){
                                                this.selectedSong.notesMeta = result.meta
                                        }else{
                                                this.selectedSong.notesMeta = {
                                                        mode: result.modeKey || selection.mode || songObj.mode || "standard",
                                                        totalNotes: result.notes.length
                                                }
                                        }
                                        if(result.durationMs && !songObj.duration_ms){
                                                songObj.duration_ms = result.durationMs
                                        }
                                        if(songObj.duration_ms){
                                                this.selectedSong.duration_ms = songObj.duration_ms
                                        }else if(result.durationMs){
                                                this.selectedSong.duration_ms = result.durationMs
                                        }
                                        if(this.selectedSong.notesMeta && result.durationMs && this.selectedSong.notesMeta.durationMs == null){
                                                this.selectedSong.notesMeta.durationMs = result.durationMs
                                        }
                                        if(result.parsedChart){
                                                this.songData = {format: "parsed-chart", data: result.parsedChart}
                                        }else{
                                                this.songData = {format: "note-events", data: result}
                                        }
                                        this.selectedSong.songData = this.songData
                                        this.updateSongDataReference()
                                        if(this._desktopPlaylistState && this._desktopPlaylistState.enabled){
                                                this.prepareDesktopPlaylistCourse(song, songObj, selection, result)
                                        }
                                }
                                return true
                        }
                        if(result && Array.isArray(result.notes) && result.notes.length){
                                if(result.modeKey){
                                        this.selectedSong.mode = result.modeKey
                                        songObj.mode = result.modeKey
                                }
                                if(result.meta){
                                        this.selectedSong.notesMeta = result.meta
                                }else{
                                        this.selectedSong.notesMeta = {
                                                mode: result.modeKey || selection.mode || songObj.mode || "standard",
                                                totalNotes: result.notes.length
                                        }
                                }
                                if(result.durationMs && !songObj.duration_ms){
                                        songObj.duration_ms = result.durationMs
                                }
                                if(songObj.duration_ms){
                                        this.selectedSong.duration_ms = songObj.duration_ms
                                }else if(result.durationMs){
                                        this.selectedSong.duration_ms = result.durationMs
                                }
                                if(this.selectedSong.notesMeta && result.durationMs && this.selectedSong.notesMeta.durationMs == null){
                                        this.selectedSong.notesMeta.durationMs = result.durationMs
                                }
                                if(result.parsedChart){
                                        this.songData = {format: "parsed-chart", data: result.parsedChart}
                                }else{
                                        this.songData = {format: "note-events", data: result}
                                }
                                this.selectedSong.songData = this.songData
                                this.updateSongDataReference()
                                if(this._desktopPlaylistState && this._desktopPlaylistState.enabled){
                                        this.prepareDesktopPlaylistCourse(song, songObj, selection, result)
                                }
                                return true
                        }
                        return this.queueLegacyNotesLoad(song, songObj).then(() => false)
                }).catch(error => {
                        console.warn("notes-loader: promise rejected", error)
                        return this.queueLegacyNotesLoad(song, songObj).then(() => false)
                })
                this.addPromise(notesHandling, "notes-loader")

                if(songObj.sound && songObj.sound.buffer){
                        songObj.sound.gain = snd.musicGain
                }else if(songObj.music !== "muted"){
			this.addPromise(snd.musicGain.load(songObj.music).then(sound => {
				songObj.sound = sound
			}), songObj.music.url)
		}
		if(songObj.lyricsFile && !songObj.lyricsData && !this.multiplayer && (!this.touchEnabled || this.autoPlayEnabled) && settings.getItem("showLyrics")){
			this.addPromise(songObj.lyricsFile.read().then(data => {
				songObj.lyricsData = data
			}, () => {}), songObj.lyricsFile.url)
		}
		if(this.touchEnabled && !assets.image["touch_drum"]){
			let img = document.createElement("img")
			img.crossOrigin = "anonymous"
			var url = gameConfig.assets_baseurl + "img/touch_drum.png"
			this.addPromise(pageEvents.load(img).then(() => {
				return this.scaleImg(img, "touch_drum", "")
			}), url)
			img.src = url
		}
		var resultsImg = [
			"results_flowers",
			"results_mikoshi",
			"results_tetsuohana",
			"results_tetsuohana2"
		]
		resultsImg.forEach(id => {
			if(!assets.image[id]){
				var img = document.createElement("img")
				img.crossOrigin = "anonymous"
				var url = gameConfig.assets_baseurl + "img/" + id + ".png"
				this.addPromise(pageEvents.load(img).then(() => {
					return this.scaleImg(img, id, "")
				}), url)
				img.src = url
			}
		})
		if(songObj.volume && songObj.volume !== 1){
			this.promises.push(new Promise(resolve => setTimeout(resolve, 500)))
		}
                Promise.all(this.promises).then(() => {
                        if(!this.error){
                                return this.setupMultiplayer()
                        }
                }).catch(error => {
                        this.handleGameStartError(error)
                })
        }
        detectDesktopPlaylistCourse(songObj, selection){
                if(!songObj){
                        return null
                }
                var charts = Array.isArray(songObj.charts) ? songObj.charts : []
                var targetMode = selection && selection.mode ? String(selection.mode).toLowerCase() : null
                for(var i = 0; i < charts.length; i++){
                        var chart = charts[i]
                        if(!chart || typeof chart !== "object"){
                                continue
                        }
                        var chartData = chart.chart_data
                        if(!chartData || typeof chartData !== "object"){
                                continue
                        }
                        var meta = chartData.meta
                        if(!meta || typeof meta !== "object" || !meta.is_playlist_course){
                                continue
                        }
                        var segments = Array.isArray(meta.segments) ? meta.segments : []
                        if(!segments.length){
                                continue
                        }
                        var chartMode = chart.mode || chart.display_course || chart.canonical_course || chart.course || null
                        return {
                                chart: chart,
                                chartData: chartData,
                                meta: meta,
                                mode: chartMode ? String(chartMode).toLowerCase() : targetMode
                        }
                }
                return null
        }
        prepareDesktopPlaylistCourse(song, songObj, selection, restResult){
                var state = this._desktopPlaylistState
                if(!state || !state.enabled || state.prepared || state.preparing){
                        return
                }
                var candidate = state.candidate || {}
                var chartData = restResult && restResult.chartData ? restResult.chartData : candidate.chartData
                if(!chartData || typeof chartData !== "object"){
                        return
                }
                var meta = chartData.meta || candidate.meta || {}
                if(!meta || typeof meta !== "object"){
                        return
                }
                var segmentsMeta = Array.isArray(meta.segments) ? meta.segments : []
                if(!segmentsMeta.length){
                        return
                }
                var baseUrl = ensureSongsBase()
                var playlistPath = meta.playlist_path || (songObj.paths && songObj.paths.playlist_path) || (candidate.meta && candidate.meta.playlist_path) || null
                var prepared = []
                var fallbackOffset = 0
                for(var i = 0; i < segmentsMeta.length; i++){
                        var seg = segmentsMeta[i]
                        if(!seg || typeof seg !== "object"){
                                continue
                        }
                        var audioUrl = resolvePlaylistAudioUrl(baseUrl, playlistPath, seg)
                        if(!audioUrl){
                                continue
                        }
                        var offsetMs = coerceNumber(seg.offset_ms, null)
                        var durationMs = coerceNumber(seg.duration_ms, null)
                        var startSeconds = offsetMs !== null ? Math.max(0, offsetMs / 1000) : fallbackOffset
                        var estimatedDuration = durationMs !== null ? Math.max(0, durationMs / 1000) : null
                        if(estimatedDuration !== null){
                                fallbackOffset = startSeconds + estimatedDuration
                        }else{
                                fallbackOffset = startSeconds
                        }
                        prepared.push({
                                url: audioUrl,
                                start: startSeconds,
                                meta: seg,
                                estimatedDuration: estimatedDuration
                        })
                }
                if(!prepared.length){
                        return
                }
                state.preparing = true
                var audioPromise = Promise.all(prepared.map(seg => {
                        var remote = new RemoteFile(seg.url)
                        return snd.musicGain.load(remote).then(sound => ({
                                sound: sound,
                                start: seg.start,
                                meta: seg.meta,
                                estimatedDuration: seg.estimatedDuration
                        }))
                })).then(loaded => {
                        var segments = loaded.map(entry => {
                                return {
                                        sound: entry.sound,
                                        start: entry.start || 0,
                                        meta: entry.meta,
                                        estimatedDuration: entry.estimatedDuration
                                }
                        })
                        segments.sort((a, b) => a.start - b.start)
                        var totalDuration = 0
                        for(var i = 0; i < segments.length; i++){
                                var segment = segments[i]
                                var durationSeconds = segment.sound && segment.sound.duration ? segment.sound.duration : 0
                                if(!(durationSeconds > 0) && segment.estimatedDuration){
                                        durationSeconds = segment.estimatedDuration
                                }
                                segment.duration = durationSeconds
                                var segmentEnd = segment.start + (durationSeconds || 0)
                                if(segmentEnd > totalDuration){
                                        totalDuration = segmentEnd
                                }
                        }
                        var chartDuration = coerceNumber(chartData.duration_ms, null)
                        if(chartDuration !== null){
                                var chartSeconds = Math.max(0, chartDuration / 1000)
                                if(chartSeconds > totalDuration){
                                        totalDuration = chartSeconds
                                }
                        }
                        var playlistSound = new PlaylistCourseSound(snd.musicGain, segments, totalDuration)
                        songObj.sound = playlistSound
                        songObj.music = "muted"
                        if(totalDuration > 0){
                                var durationMs = Math.round(totalDuration * 1000)
                                songObj.duration_ms = durationMs
                                this.selectedSong.duration_ms = durationMs
                        }
                        var playlistMeta = {
                                segments: segments.map(segment => ({
                                        start: segment.start,
                                        duration: segment.duration,
                                        meta: segment.meta
                                })),
                                chartData: chartData,
                                playlistUrl: meta.playlist_url || null
                        }
                        songObj.desktopPlaylist = playlistMeta
                        this.selectedSong.desktopPlaylist = playlistMeta
                        if(chartData.total_notes != null && (!this.selectedSong.notesMeta || this.selectedSong.notesMeta.totalNotes == null)){
                                var totalNotes = coerceNumber(chartData.total_notes, null)
                                if(totalNotes !== null){
                                        this.selectedSong.notesMeta = this.selectedSong.notesMeta || {}
                                        this.selectedSong.notesMeta.totalNotes = totalNotes
                                }
                        }
                        state.prepared = true
                }).finally(() => {
                        state.preparing = false
                })
                this.addPromise(audioPromise, "desktop-playlist-audio")
        }
        queueLegacyNotesLoad(song, songObj){
                if(this._legacyNotesQueued){
                        return this._legacyNotesPromise || Promise.resolve()
                }
                this._legacyNotesQueued = true
                var chart = songObj.chart
                if(chart && chart.separateDiff){
                        var chartDiff = this.selectedSong.difficulty
                        chart = chart[chartDiff]
                }
                let legacyPromise
                if(chart){
                        const readPromise = chart.read(song.type === "tja" ? "utf-8" : "").then(data => {
                                this.songData = data.replace(/\0/g, "").split("\n")
                                this.selectedSong.songData = this.songData
                                this.updateSongDataReference()
                        })
                        legacyPromise = this.addPromise(readPromise, chart.url)
                }else{
                        this.songData = ""
                        this.selectedSong.songData = this.songData
                        this.updateSongDataReference()
                        legacyPromise = Promise.resolve()
                }
                this._legacyNotesPromise = Promise.resolve(legacyPromise)
                return this._legacyNotesPromise
        }
        addPromise(promise, url){
                const wrapped = Promise.resolve(promise).catch(response => {
                        this.errorMsg(response, url)
                })
                this.promises.push(wrapped)
                return wrapped
        }
        updateSongDataReference(){
                if(typeof window === "undefined"){
                        return
                }
                const root = window._taiko = window._taiko || {}
                const songScope = root.song = root.song || {}
                songScope.songData = this.songData
                songScope.selectedSong = this.selectedSong
                if(this.songObj){
                        songScope.songObj = this.songObj
                }
                if(this.selectedSong && this.selectedSong.notesMeta){
                        songScope.meta = this.selectedSong.notesMeta
                }else if(songScope.meta){
                        delete songScope.meta
                }
                if(this.selectedSong && typeof this.selectedSong.duration_ms === "number"){
                        songScope.durationMs = this.selectedSong.duration_ms
                }else if(this.selectedSong && this.selectedSong.notesMeta && this.selectedSong.notesMeta.durationMs != null){
                        songScope.durationMs = this.selectedSong.notesMeta.durationMs
                }else if(songScope.meta && songScope.meta.durationMs != null){
                        songScope.durationMs = songScope.meta.durationMs
                }else{
                        delete songScope.durationMs
                }
        }
	errorMsg(error, url){
		if(!this.error){
			if(url){
				error = (Array.isArray(error) ? error[0] + ": " : (error ? error + ": " : "")) + url
			}
			pageEvents.send("load-song-error", error)
			errorMessage(new Error(error).stack)
			var title = this.selectedSong.title
			if(title !== this.selectedSong.originalTitle){
				title += " (" + this.selectedSong.originalTitle + ")"
			}
			assets.sounds["v_start"].stop()
			setTimeout(() => {
				this.clean()
				new SongSelect(false, false, this.touchEnabled, null, {
					name: "loadSongError",
					title: title,
					id: this.selectedSong.folder,
					error: error
				})
			}, 500)
		}
		this.error = true
	}
	loadSongBg(){
		var filenames = []
		if(this.selectedSong.songBg !== null){
			filenames.push("bg_song_" + this.selectedSong.songBg)
		}
		if(this.selectedSong.donBg !== null){
			filenames.push("bg_don_" + this.selectedSong.donBg)
			if(this.multiplayer){
				filenames.push("bg_don2_" + this.selectedSong.donBg)
			}
		}
		if(this.selectedSong.songStage !== null){
			filenames.push("bg_stage_" + this.selectedSong.songStage)
		}
		for(var i = 0; i < filenames.length; i++){
			var filename = filenames[i]
			var stage = filename.startsWith("bg_stage_")
			for(var letter = 0; letter < (stage ? 1 : 2); letter++){
				let filenameAb = filenames[i] + (stage ? "" : (letter === 0 ? "a" : "b"))
				if(!(filenameAb in assets.image)){
					let img = document.createElement("img")
					let force = filenameAb.startsWith("bg_song_") && this.touchEnabled
					img.crossOrigin = "anonymous"
					var url = gameConfig.assets_baseurl + "img/" + filenameAb + ".png"
					this.addPromise(pageEvents.load(img).then(() => {
						return this.scaleImg(img, filenameAb, "", force)
					}), url)
					img.src = url
				}
			}
		}
	}
	scaleImg(img, filename, prefix, force){
		return new Promise((resolve, reject) => {
			var scale = this.imgScale
			if(force && scale > 0.5){
				scale = 0.5
			}
			var canvas = document.createElement("canvas")
			var w = Math.floor(img.width * scale)
			var h = Math.floor(img.height * scale)
			canvas.width = Math.max(1, w)
			canvas.height = Math.max(1, h)
			var ctx = canvas.getContext("2d")
			ctx.drawImage(img, 0, 0, w, h)
			var saveScaled = url => {
				let img2 = document.createElement("img")
				pageEvents.load(img2).then(() => {
					assets.image[prefix + filename] = img2
					loader.assetsDiv.appendChild(img2)
					resolve()
				}, reject)
				img2.id = prefix + filename
				img2.src = url
			}
			if("toBlob" in canvas){
				canvas.toBlob(blob => {
					saveScaled(URL.createObjectURL(blob))
				})
			}else{
				saveScaled(canvas.toDataURL())
			}
		})
	}
	randInt(min, max){
		return Math.floor(Math.random() * (max - min + 1)) + min
	}
        async setupMultiplayer(){
                var song = this.selectedSong

                try{
                        await this.ensureSongPlayable(song)
                }catch(error){
                        this.handleGameStartError(error)
                        return
                }

                if(this.multiplayer){
                        var loadingText = document.getElementsByClassName("loading-text")[0]
                        loadingText.firstChild.data = strings.waitingForP2
                        loadingText.setAttribute("alt", strings.waitingForP2)
			
			this.cancelButton = document.getElementById("p2-cancel-button")
			this.cancelButton.style.display = "inline-block"
			pageEvents.add(this.cancelButton, ["mousedown", "touchstart"], this.cancelLoad.bind(this))
			
			this.song2Data = this.songData
			this.selectedSong2 = song
			pageEvents.add(p2, "message", event => {
				if(event.type === "gameload"){
					this.cancelButton.style.display = ""
					
					if(event.value.diff === song.difficulty){
						this.startMultiplayer()
					}else{
						this.selectedSong2 = {}
						for(var i in this.selectedSong){
							this.selectedSong2[i] = this.selectedSong[i]
						}
						this.selectedSong2.difficulty = event.value.diff
						var chart = this.songObj.chart
						var chartDiff = this.selectedSong2.difficulty
						if(song.type === "tja" || !chart || !chart.separateDiff || !chart[chartDiff]){
							this.startMultiplayer()
						}else{
							chart[chartDiff].read(song.type === "tja" ? "utf-8" : "").then(data => {
								this.song2Data = data.replace(/\0/g, "").split("\n")
							}, () => {}).then(() => {
								this.startMultiplayer()
							})
						}
					}
                                }else if(event.type === "gamestart"){
                                        this.clean()
                                        p2.clearMessage("songsel")
                                        try{
                                                var taikoGame1 = new Controller(song, this.songData, false, 1, this.touchEnabled)
                                                var taikoGame2 = new Controller(this.selectedSong2, this.song2Data, true, 2, this.touchEnabled)
                                                taikoGame1.run(taikoGame2)
                                                pageEvents.send("load-song-player2", this.selectedSong2)
                                        }catch(error){
                                                this.handleGameStartError(error)
                                        }
                                }else if(event.type === "left" || event.type === "gameend"){
                                        this.clean()
                                        new SongSelect(false, false, this.touchEnabled)
                                }
                        })
			p2.send("join", {
				id: song.folder,
				diff: song.difficulty,
				name: account.loggedIn ? account.displayName : null,
				don: account.loggedIn ? account.don : null
			})
                }else{
                        this.clean()
                        try{
                                var taikoGame = new Controller(song, this.songData, this.autoPlayEnabled, false, this.touchEnabled)
                                taikoGame.run()
                        }catch(error){
                                this.handleGameStartError(error)
                        }
                }
        }
        async ensureSongPlayable(song){
                const catalogFlag = typeof window !== "undefined" && window.CATALOG_ASSUME_VALID === 1
                if(!catalogFlag){
                        return
                }
                if(!song || !song.catalogAssumeValid){
                        return
                }
                const songId = song.folder || song.id
                if(!songId){
                        return
                }
                const encodedId = encodeURIComponent(songId)
                const url = "api/song/" + encodedId + "?notes=none"
                let response
                try{
                        response = await fetch(url, {method: "GET", credentials: "same-origin"})
                }catch(fetchError){
                        const error = new Error("network_error")
                        error.code = "network_error"
                        error.cause = fetchError
                        throw error
                }
                if(response.status === 404){
                        let payload = null
                        try{
                                payload = await response.json()
                        }catch(e){}
                        const errorCode = payload && payload.error ? payload.error : "chart_not_found"
                        const error = new Error(errorCode)
                        error.code = errorCode
                        error.status = response.status
                        throw error
                }
                if(!response.ok){
                        const error = new Error("song_request_failed")
                        error.code = "song_request_failed"
                        error.status = response.status
                        throw error
                }
                try{
                        await response.json()
                }catch(e){}
        }
        handleGameStartError(error){
                if(this.error){
                        return
                }
                this.error = true
                const code = error && error.code ? String(error.code) : (error && error.message ? String(error.message) : "unknown_error")
                const status = error && error.status ? error.status : null
                const detail = status ? code + " (" + status + ")" : code
                try{
                        assets.sounds["v_start"].stop()
                }catch(e){}
                try{
                        pageEvents.send("load-song-error", detail)
                }catch(e){}
                try{
                        errorMessage(new Error(detail).stack)
                }catch(e){}
                this.clean()
                var title = this.selectedSong && this.selectedSong.title ? this.selectedSong.title : ""
                if(this.selectedSong && this.selectedSong.originalTitle && this.selectedSong.originalTitle !== title){
                        title += " (" + this.selectedSong.originalTitle + ")"
                }
                const warning = {
                        name: "catalogStartError",
                        title: title,
                        id: this.selectedSong && this.selectedSong.folder ? this.selectedSong.folder : "",
                        reason: detail
                }
                setTimeout(() => {
                        new SongSelect(false, false, this.touchEnabled, null, warning)
                }, 500)
        }
        startMultiplayer(repeat){
                if(document.hasFocus()){
                        p2.send("gamestart")
                }else{
                        if(!repeat){
				assets.sounds["v_sanka"].play()
				pageEvents.send("load-song-unfocused")
			}
			setTimeout(() => {
				this.startMultiplayer(true)
			}, 100)
		}
	}
	cancelLoad(event){
		if(event.type === "mousedown"){
			if(event.which !== 1){
				return
			}
		}else{
			event.preventDefault()
		}
		p2.send("leave")
		assets.sounds["se_don"].play()
		this.cancelButton.style.pointerEvents = "none"
		pageEvents.send("load-song-cancel")
	}
	clean(){
		delete this.promises
		delete this.songObj
        delete this.videoElement
		pageEvents.remove(p2, "message")
		if(this.cancelButton){
			pageEvents.remove(this.cancelButton, ["mousedown", "touchstart"])
			delete this.cancelButton
		}
	}
	
	static insertBackgroundVideo(songId) {
        const video = document.createElement("video");
        video.src = `songs/${songId}/main.mp4`; 
        video.autoplay = true;
        video.muted = true;  // 可选：静音
        video.style.objectFit = 'cover';
        video.style.position = 'fixed';
        video.style.top = "0";
        video.style.left = "0";
        video.style.zIndex = "0";  // 背景视频
        video.style.width = "100vw";
        video.style.height = "100vh";
        document.body.appendChild(video);
        window.videoElement = video;
    }

	
}
