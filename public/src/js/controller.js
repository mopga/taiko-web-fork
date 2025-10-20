class Controller{
	constructor(...args){
		this.init(...args)
	}
	init(selectedSong, songData, autoPlayEnabled, multiplayer, touchEnabled){
		this.selectedSong = selectedSong
		this.songData = songData
                this.autoPlayEnabled = autoPlayEnabled
                this.saveScore = !autoPlayEnabled
                this.multiplayer = multiplayer
                this.touchEnabled = touchEnabled
                this.songEndGuard = null
                const loaderApi = typeof notesLoader !== "undefined" ? notesLoader : (typeof window !== "undefined" ? window.notesLoader : null)
                this.songDataStruct = typeof getSongDataStruct === "function" ? getSongDataStruct(songData) : null
                if(multiplayer === 2){
                        this.snd = p2.player === 2 ? "_p1" : "_p2"
                        this.don = p2.don || defaultDon
                }else{
                        this.snd = multiplayer ? "_p" + p2.player : ""
			this.don = account.loggedIn ? account.don : defaultDon
		}
		if(this.snd === "_p2" && this.objEqual(defaultDon, this.don)){
			this.don = {
				body_fill: defaultDon.face_fill,
				face_fill: defaultDon.body_fill
			}
		}
		
		this.calibrationMode = selectedSong.folder === "calibration"
		this.audioLatency = 0
		this.videoLatency = 0
		if(!this.calibrationMode){
			var latency = settings.getItem("latency")
			if(!autoPlayEnabled || this.multiplayer){
				this.audioLatency = Math.round(latency.audio) || 0
			}
			this.videoLatency = Math.round(latency.video) || 0 + this.audioLatency
		}
		if(this.multiplayer !== 2){
			loader.changePage("game", false)
		}
		
                if(songData && typeof songData === "object" && songData.format === "parsed-chart" && songData.data){
                        this.parsedSongData = songData.data
                }else if(this.songDataStruct && this.songDataStruct.format === "note-events" && songData && typeof songData === "object"){ 
                        let parsedChart = songData.data && songData.data.parsedChart
                        if(!parsedChart && loaderApi && typeof loaderApi.buildFallbackParsedChart === "function"){
                                parsedChart = loaderApi.buildFallbackParsedChart(this.songDataStruct.notes)
                        }
                        if(parsedChart){
                                this.parsedSongData = parsedChart
                        }
                }
                if(!this.parsedSongData){
                        if(selectedSong.type === "tja"){
                                this.parsedSongData = new ParseTja(songData, selectedSong.difficulty, selectedSong.stars, selectedSong.offset)
                        }else{
                                this.parsedSongData = new ParseOsu(songData, selectedSong.difficulty, selectedSong.stars, selectedSong.offset)
                        }
                }
                if(!this.parsedSongData){
                        this.parsedSongData = {circles: [], measures: [], events: [], branches: null, beatInfo: {beatInterval: 600}, soundOffset: 0}
                }
                this.offset = this.parsedSongData.soundOffset
		
		var maxCombo = this.parsedSongData.circles.filter(circle => ["don", "ka", "daiDon", "daiKa"].indexOf(circle.type) > -1 && (!circle.branch || circle.branch.name == "master")).length
		if (maxCombo >= 50) {
			var comboVoices = ["v_combo_50"].concat(Array.from(Array(Math.min(50, Math.floor(maxCombo / 100))), (d, i) => "v_combo_" + ((i + 1) * 100)))
			var promises = []
			
			comboVoices.forEach(name => {
				if (!assets.sounds[name + "_p1"]) {
					promises.push(loader.loadSound(name + ".ogg", snd.sfxGain).then(sound => {
						assets.sounds[name + "_p1"] = assets.sounds[name].copy(snd.sfxGainL)
						assets.sounds[name + "_p2"] = assets.sounds[name].copy(snd.sfxGainR)
					}))
				}
			})
			
			Promise.all(promises)
		}
		
		if(this.calibrationMode){
			this.volume = 1
		}else{
			assets.songs.forEach(song => {
				if(song.id == this.selectedSong.folder){
					this.mainAsset = song.sound
					this.volume = song.volume || 1
					if(!multiplayer && (!this.touchEnabled || this.autoPlayEnabled) && settings.getItem("showLyrics")){
						if(song.lyricsData){
							var lyricsDiv = document.getElementById("song-lyrics")
							this.lyrics = new Lyrics(song.lyricsData, selectedSong.offset, lyricsDiv)
						}else if(this.parsedSongData.lyrics){
							var lyricsDiv = document.getElementById("song-lyrics")
							this.lyrics = new Lyrics(this.parsedSongData.lyrics, selectedSong.offset, lyricsDiv, true)
						}
					}
				}
			})
		}
		
		this.game = new Game(this, this.selectedSong, this.parsedSongData)
		this.view = new View(this)
		if (parseFloat(localStorage.getItem("baisoku") ?? "1", 10) !== 1) {
			this.saveScore = false;
		}
		this.mekadon = new Mekadon(this, this.game)
		this.keyboard = new GameInput(this)
		if(!autoPlayEnabled && this.multiplayer !== 2){
			this.easierBigNotes = settings.getItem("easierBigNotes") || this.keyboard.keyboard.TaikoForceLv5
		}else{
			this.easierBigNotes = false
		}
		
		this.drumSounds = settings.getItem("latency").drumSounds
		this.playedSounds = {}
	}
	run(syncWith){
		if(syncWith){
			this.syncWith = syncWith
		}
		if(this.multiplayer !== 2){
			snd.musicGain.setVolumeMul(this.volume)
		}
		this.game.run()
		this.view.run()
		if(this.multiplayer === 1){
			syncWith.run(this)
			syncWith.game.elapsedTime = this.game.elapsedTime
			syncWith.game.startDate = this.game.startDate
		}
		requestAnimationFrame(() => {
			this.startMainLoop()
			if(!this.multiplayer){
				debugObj.controller = this
				if(debugObj.debug){
					debugObj.debug.updateStatus()
				}
			}
		})
	}
        startMainLoop(){
                this.mainLoopRunning = true
                window.gamestatus = 'start'
                this.gameLoop()
                this.viewLoop()
                this.installSongEndGuard()
                if(this.multiplayer !== 2){
                        this.gameInterval = setInterval(this.gameLoop.bind(this), 1000 / 60)
                        pageEvents.send("game-start", {
                                selectedSong: this.selectedSong,
                                autoPlayEnabled: this.autoPlayEnabled,
				multiplayer: this.multiplayer,
				touchEnabled: this.touchEnabled
			})
		}
	}
        stopMainLoop(){
                this.mainLoopRunning = false
                window.gamestatus = 'stop'

                if (window.videoElement) {
        // 停止视频播放
        window.videoElement.pause();
        
        // 移除视频元素
        document.body.removeChild(window.videoElement);
        
        // 解除引用，允许垃圾回收
        window.videoElement = null;
    }
		

                if(this.game.mainAsset){
                        this.game.mainAsset.stop()
                }
                if(this.multiplayer !== 2){
                        clearInterval(this.gameInterval)
                }
                if(this.songEndGuard){
                        clearTimeout(this.songEndGuard)
                        this.songEndGuard = null
                }
        }
	gameLoop(){
		if(this.mainLoopRunning){
			if(this.multiplayer === 1){
				this.syncWith.game.elapsedTime = this.game.elapsedTime
				this.syncWith.game.startDate = this.game.startDate
			}
			var ms = this.game.elapsedTime
			
			if(this.game.musicFadeOut < 3){
				this.keyboard.checkMenuKeys()
			}
			if(this.calibrationMode){
				this.game.calibration()
			}
			if(!this.game.isPaused()){
				this.keyboard.checkGameKeys()
				
				if(ms < 0){
					this.game.updateTime()
				}else{
					if(!this.calibrationMode){
						this.game.update()
					}
					if(!this.mainLoopRunning){
						return
					}
					this.game.playMainMusic()
				}
			}
			if(this.multiplayer === 1){
				this.syncWith.gameLoop()
			}
		}
	}
	viewLoop(){
		if(this.mainLoopRunning){
			if(this.multiplayer !== 2){
				requestAnimationFrame(() => {
					var player = this.multiplayer ? p2.player : 1
					if(player === 1){
						this.viewLoop()
					}
					if(this.multiplayer === 1){
						this.syncWith.viewLoop()
					}
					if(player === 2){
						this.viewLoop()
					}
					if(this.scoresheet){
						if(this.view.ctx){
							this.view.ctx.save()
							this.view.ctx.setTransform(1, 0, 0, 1, 0, 0)
						}
						this.scoresheet.redraw()
						if(this.view.ctx){
							this.view.ctx.restore()
						}
					}
				})
			}
			this.view.refresh()
		}
	}
	gameEnded(){
		var score = this.getGlobalScore()
		var vp
		if(this.game.rules.clearReached(score.gauge)){
			if(score.bad === 0){
				vp = "fullcombo"
				this.playSound("v_fullcombo", 1.350)
			}else{
				vp = "clear"
			}
		}else{
			vp = "fail"
		}
		this.playSound("se_game" + vp)
	}
	displayResults(){
		if(this.multiplayer !== 2){
			if(this.view.cursorHidden){
				this.view.canvas.style.cursor = ""
			}
			this.scoresheet = new Scoresheet(this, this.getGlobalScore(), this.multiplayer, this.touchEnabled)
		}
	}
	displayScore(score, notPlayed, bigNote){
		this.view.displayScore(score, notPlayed, bigNote)
	}
	songSelection(fadeIn, showWarning){
		if(!fadeIn){
			if(this.cleaned){
				return
			}
			this.clean()
		}
		if(this.calibrationMode){
			new SettingsView(this.touchEnabled, false, null, "latency")
		}else{
			new SongSelect(false, fadeIn, this.touchEnabled, null, showWarning)
		}
	}
	restartSong(){
		if(this.cleaned){
			return
		}
		this.clean()
		if(this.multiplayer){
			new LoadSong(this.selectedSong, false, true, this.touchEnabled)
		}else{
			new Promise(resolve => {
				if(this.calibrationMode){
					resolve()
				}else{
					var songObj = assets.songs.find(song => song.id === this.selectedSong.folder)
					var promises = []
					if(songObj.chart && songObj.chart !== "blank"){
						var chart = songObj.chart
						if(chart.separateDiff){
							var chartDiff = this.selectedSong.difficulty
							chart = chart[chartDiff]
						}
						this.addPromise(promises, chart.read(this.selectedSong.type === "tja" ? "utf-8" : undefined).then(data => {
							this.songData = data.replace(/\0/g, "").split("\n")
							return Promise.resolve()
						}), chart.url)
					}
					if(songObj.lyricsFile){
						this.addPromise(promises, songObj.lyricsFile.read().then(result => {
							songObj.lyricsData = result
						}, () => Promise.resolve()), songObj.lyricsFile.url)
					}
					if(songObj && songObj.category_id === 9){
					    LoadSong.insertBackgroundVideo(songObj.id)
					}
					Promise.all(promises).then(resolve)
				}
			}).then(() => {
				var taikoGame = new Controller(this.selectedSong, this.songData, this.autoPlayEnabled, false, this.touchEnabled)
				taikoGame.run()
			})
		}
	}
        addPromise(promises, promise, url){
                promises.push(promise.catch(error => {
                        if(this.restartSongError){
                                return
                        }
			this.restartSongError = true
			if(url){
				error = (Array.isArray(error) ? error[0] + ": " : (error ? error + ": " : "")) + url
			}
			pageEvents.send("load-song-error", error)
			errorMessage(new Error(error).stack)
			var title = this.selectedSong.title
			if(title !== this.selectedSong.originalTitle){
				title += " (" + this.selectedSong.originalTitle + ")"
			}
			setTimeout(() => {
				new SongSelect(false, false, this.touchEnabled, null, {
					name: "loadSongError",
					title: title,
					id: this.selectedSong.folder,
					error: error
				})
			}, 500)
			return Promise.reject(error)
                }))
        }
        resolveExpectedSongDuration(){
                let duration = 0
                if(this.selectedSong && typeof this.selectedSong.duration_ms === "number" && this.selectedSong.duration_ms > duration){
                        duration = this.selectedSong.duration_ms
                }
                if(this.selectedSong && this.selectedSong.notesMeta && typeof this.selectedSong.notesMeta.durationMs === "number" && this.selectedSong.notesMeta.durationMs > duration){
                        duration = this.selectedSong.notesMeta.durationMs
                }
                if(this.parsedSongData && Array.isArray(this.parsedSongData.circles)){
                        for(let i = 0; i < this.parsedSongData.circles.length; i++){
                                const circle = this.parsedSongData.circles[i]
                                if(!circle || typeof circle !== "object"){
                                        continue
                                }
                                const end = typeof circle.endTime === "number" ? circle.endTime : null
                                const start = typeof circle.ms === "number" ? circle.ms : null
                                if(end !== null && end > duration){
                                        duration = end
                                }
                                if(start !== null && start > duration){
                                        duration = start
                                }
                        }
                }
                if(this.parsedSongData && Array.isArray(this.parsedSongData.measures) && this.parsedSongData.measures.length){
                        const lastMeasure = this.parsedSongData.measures[this.parsedSongData.measures.length - 1]
                        if(lastMeasure){
                                const measureTime = typeof lastMeasure.ms === "number" ? lastMeasure.ms : (typeof lastMeasure.originalMS === "number" ? lastMeasure.originalMS : null)
                                if(measureTime !== null && measureTime > duration){
                                        duration = measureTime
                                }
                        }
                }
                if(this.parsedSongData && typeof this.parsedSongData.soundOffset === "number" && this.parsedSongData.soundOffset < 0){
                        duration += Math.abs(this.parsedSongData.soundOffset)
                }
                if(!duration && this.songDataStruct && Array.isArray(this.songDataStruct.notes) && this.songDataStruct.notes.length){
                        let eventsMax = 0
                        for(let i = 0; i < this.songDataStruct.notes.length; i++){
                                const entry = this.songDataStruct.notes[i]
                                if(!entry || typeof entry !== "object"){
                                        continue
                                }
                                const start = typeof entry.time === "number" ? entry.time : (typeof entry.ms === "number" ? entry.ms : null)
                                const end = typeof entry.endTime === "number" ? entry.endTime : (typeof entry.end_ms === "number" ? entry.end_ms : null)
                                if(end !== null && end > eventsMax){
                                        eventsMax = end
                                }
                                if(start !== null && start > eventsMax){
                                        eventsMax = start
                                }
                        }
                        if(eventsMax > duration){
                                duration = eventsMax
                        }
                }
                if(this.songDataStruct && typeof this.songDataStruct.durationMs === "number" && this.songDataStruct.durationMs > duration){
                        duration = this.songDataStruct.durationMs
                }
                return duration
        }
        installSongEndGuard(){
                if(this.songEndGuard){
                        clearTimeout(this.songEndGuard)
                        this.songEndGuard = null
                }
                const duration = this.resolveExpectedSongDuration()
                if(!Number.isFinite(duration) || duration <= 0){
                        return
                }
                const wait = Math.max(0, Math.round(duration + 2000))
                this.songEndGuard = setTimeout(() => {
                        if(!this.mainLoopRunning || this.game.musicFadeOut >= 2){
                                return
                        }
                        if(!this.game.fadeOutStarted){
                                const fallbackStart = typeof this.game.getAccurateTime === "function" ? this.game.getAccurateTime() : this.game.elapsedTime
                                const reference = Number.isFinite(fallbackStart) ? fallbackStart : duration
                                this.game.fadeOutStarted = reference
                                if(this.multiplayer === 1 && this.syncWith && this.syncWith.game && !this.syncWith.game.fadeOutStarted){
                                        this.syncWith.game.fadeOutStarted = reference
                                }
                        }
                        this.game.whenFadeoutMusic()
                }, wait)
        }
	playSound(id, time, noSnd){
		if(!this.drumSounds && (id === "neiro_1_don" || id === "neiro_1_ka" || id === "se_don" || id === "se_ka")){
			return
		}
		var ms = Date.now() + (time || 0) * 1000
		if(!(id in this.playedSounds) || ms > this.playedSounds[id] + 30){
			assets.sounds[id + (noSnd ? "" : this.snd)].play(time)
			this.playedSounds[id] = ms
		}
	}
	togglePause(forcePause, pauseMove, noSound){
		if(this.multiplayer === 1){
			this.syncWith.game.togglePause(forcePause, pauseMove, noSound)
		}
		this.game.togglePause(forcePause, pauseMove, noSound)
		
		
	}
	getKeys(){
		return this.keyboard.getKeys()
	}
	setKey(pressed, name, ms){
		return this.keyboard.setKey(pressed, name, ms)
	}
	getElapsedTime(){
		return this.game.elapsedTime
	}
	getCircles(){
		return this.game.getCircles()
	}
	getCurrentCircle(){
		return this.game.getCurrentCircle()
	}
	isWaiting(key, type){
		return this.keyboard.isWaiting(key, type)
	}
	waitForKeyup(key, type){
		this.keyboard.waitForKeyup(key, type)
	}
	getKeyTime(){
		return this.keyboard.getKeyTime()
	}
	getCombo(){
		return this.game.getCombo()
	}
	getGlobalScore(){
		return this.game.getGlobalScore()
	}
	autoPlay(circle){
		if(this.multiplayer){
			p2.play(circle, this.mekadon)
		}else{
			return this.mekadon.play(circle)
		}
	}
	objEqual(a, b){
		for(var i in a){
			if(a[i] !== b[i]){
				return false
			}
		}
		return true
	}
	clean(){
		this.cleaned = true
		if(this.multiplayer === 1){
			this.syncWith.clean()
		}
		this.stopMainLoop()
		this.keyboard.clean()
		this.view.clean()
		snd.buffer.loadSettings()
		
		if(!this.multiplayer){
			debugObj.controller = null
			if(debugObj.debug){
				debugObj.debug.updateStatus()
			}
		}
		if(this.lyrics){
			this.lyrics.clean()
		}
	}
}
