;(function(){
        const cfg = (window.gameConfig || window.GAME_CONFIG || {});
        const v = (cfg.catalogAssumeValid ?? cfg.catalog_assume_valid ?? false);
        if(typeof window.catalogAssumeValid === "undefined"){
                window.catalogAssumeValid = Boolean(v);
        }
})();
const DEFAULT_MODES_MANIFEST_CACHE_TTL_MS = 12000;
const USE_DETAILS_IN_CATALOG = 0;
const songsCatalogCache = {
        lastResult: [],
        etag: null,
};


function resolveManifestStatus(manifest){
        if(manifest && typeof manifest === "object" && typeof manifest.status === "string" && manifest.status.trim()){
                return manifest.status.trim();
        }
        return "ok";
}

function commitModesStore(store){
        if(!store || typeof store !== "object"){
                return;
        }
        if(typeof store.cacheTtlMs !== "number" || store.cacheTtlMs <= 0){
                store.cacheTtlMs = DEFAULT_MODES_MANIFEST_CACHE_TTL_MS;
        }
        if(store.manifest && (!store.categoryIndex || typeof store.categoryIndex !== "object") && resolveManifestStatus(store.manifest) === "ok"){
                store.categoryIndex = buildModesCategoryIndex(store.manifest);
        }
        if(typeof window !== "undefined"){
                window.__modes__ = store;
        }
        const status = store.status || resolveManifestStatus(store.manifest);
        if(store.manifest){
                assets.modesManifest = store.manifest;
        }else if(status){
                assets.modesManifest = {status: status};
        }
        if(typeof modesHelper === "object" && modesHelper && typeof modesHelper.updateManifest === "function"){
                const payload = store.manifest || (status ? {status: status} : null);
                if(payload){
                        modesHelper.updateManifest(payload);
                }
        }
}

function buildModesCategoryIndex(manifest){
        const index = {};
        if(!manifest || typeof manifest !== "object"){
                return index;
        }
        const modes = Array.isArray(manifest.modes) ? manifest.modes : [];
        modes.forEach(entry => {
                if(!entry || typeof entry !== "object"){
                        return;
                }
                const rawKey = typeof entry.key === "string" ? entry.key : (typeof entry.mode === "string" ? entry.mode : "");
                const key = rawKey.trim();
                if(!key){
                        return;
                }
                const lower = key.toLowerCase();
                const canonical = lower === "dan" || lower === "dojo" ? "dandojo" : lower;
                if(!canonical){
                        return;
                }
                const categories = Array.isArray(entry.categories) ? entry.categories : [];
                categories.forEach(category => {
                        if(typeof category === "string" && category.trim()){
                                index[category.trim().toLowerCase()] = canonical;
                        }
                });
        });
        return index;
}

class Loader{
	constructor(...args){
		this.init(...args)
	}
	init(callback){
		this.callback = callback
		this.loadedAssets = 0
		this.assetsDiv = document.getElementById("assets")
		this.screen = document.getElementById("screen")
		this.startTime = Date.now()
		this.errorMessages = []
		this.songSearchGradient = "linear-gradient(to top, rgba(245, 246, 252, 0.08), #ff5963), "
		
		var promises = []
		
		promises.push(this.ajax("src/views/loader.html").then(page => {
			this.screen.innerHTML = page
		}))
		
		promises.push(this.ajax("api/config").then(conf => {
			gameConfig = JSON.parse(conf)
		}))
		
		Promise.all(promises).then(this.run.bind(this))
	}
        run(){
                this.promises = []
                this.loaderDiv = document.querySelector("#loader")
                this.loaderPercentage = document.querySelector("#loader .percentage")
                this.loaderProgress = document.querySelector("#loader .progress")

                const configCatalogFlag = gameConfig.catalogAssumeValid ?? gameConfig.catalog_assume_valid
                if(typeof window !== "undefined"){
                        if(typeof configCatalogFlag !== "undefined"){
                                window.catalogAssumeValid = Boolean(configCatalogFlag)
                        }
                        const resolvedCatalogFlag = window.catalogAssumeValid ? 1 : 0
                        window.CATALOG_ASSUME_VALID = resolvedCatalogFlag
                        window.catalogAssumeValid = resolvedCatalogFlag === 1
                }
		
		this.queryString = gameConfig._version.commit_short ? "?" + gameConfig._version.commit_short : ""
		
		if(gameConfig.custom_js){
			this.addPromise(this.loadScript(gameConfig.custom_js), gameConfig.custom_js)
		}
		var oggSupport = new Audio().canPlayType("audio/ogg;codecs=vorbis")
		if(!oggSupport){
			assets.js.push("lib/oggmented-wasm.js")
		}
		assets.js.forEach(name => {
			this.addPromise(this.loadScript("src/js/" + name), "src/js/" + name)
		})
		
		var pageVersion = versionLink.href
		var index = pageVersion.lastIndexOf("/")
		if(index !== -1){
			pageVersion = pageVersion.slice(index + 1)
		}
		this.addPromise(new Promise((resolve, reject) => {
			if(
				versionLink.href !== gameConfig._version.url &&
				gameConfig._version.commit &&
				versionLink.href.indexOf(gameConfig._version.commit) === -1
			){
				reject("Version on the page and config does not match\n(page:  " + pageVersion + ",\nconfig: "+ gameConfig._version.commit + ")")
			}
			var cssCount = document.styleSheets.length + assets.css.length
			assets.css.forEach(name => {
				var stylesheet = document.createElement("link")
				stylesheet.rel = "stylesheet"
				stylesheet.href = "src/css/" + name + this.queryString
				document.head.appendChild(stylesheet)
			})
			var checkStyles = () => {
				if(document.styleSheets.length >= cssCount){
					resolve()
					clearInterval(interval)
				}
			}
			var interval = setInterval(checkStyles, 100)
			checkStyles()
		}))
		
		for(var name in assets.fonts){
			var url = gameConfig.assets_baseurl + "fonts/" + assets.fonts[name]
			this.addPromise(new FontFace(name, "url('" + url + "')").load().then(font => {
				document.fonts.add(font)
			}), url)
		}
		
		assets.img.forEach(name => {
			var id = this.getFilename(name)
			var image = document.createElement("img")
			image.crossOrigin = "anonymous"
			var url = gameConfig.assets_baseurl + "img/" + name
			this.addPromise(pageEvents.load(image), url)
			image.id = name
			image.src = url
			this.assetsDiv.appendChild(image)
			assets.image[id] = image
		})
		
		var css = []
		for(let selector in assets.cssBackground){
			let name = assets.cssBackground[selector]
			var url = gameConfig.assets_baseurl + "img/" + name
			this.addPromise(loader.ajax(url, request => {
				request.responseType = "blob"
			}).then(blob => {
				var id = this.getFilename(name)
				var image = document.createElement("img")
				let blobUrl = URL.createObjectURL(blob)
				var promise = pageEvents.load(image).then(() => {
					var gradient = ""
					if(selector === ".pattern-bg"){
						loader.screen.style.backgroundImage = "url(\"" + blobUrl + "\")"
					}else if(selector === "#song-search"){
						gradient = this.songSearchGradient
					}
					css.push(this.cssRuleset({
						[selector]: {
							"background-image": gradient + "url(\"" + blobUrl + "\")"
						}
					}))
				})
				image.id = name
				image.src = blobUrl
				this.assetsDiv.appendChild(image)
				assets.image[id] = image
				return promise
			}), url)
		}
		
		assets.views.forEach(name => {
			var id = this.getFilename(name)
			var url = "src/views/" + name + this.queryString
			this.addPromise(this.ajax(url).then(page => {
				assets.pages[id] = page
			}), url)
		})
		
                this.addPromise(this.loadModesManifest(), "api/modes")

                this.addPromise(this.ajax("api/categories").then(cats => {
                        assets.categories = JSON.parse(cats)
                        assets.categories.forEach(cat => {
                                if(cat.song_skin){
					cat.songSkin = cat.song_skin //rename the song_skin property and add category title to categories array
					delete cat.song_skin
					cat.songSkin.infoFill = cat.songSkin.info_fill
					delete cat.songSkin.info_fill
				}
			})
			
                        assets.categories.push({
                                title: "default",
                                songSkin: {
                                        background: "#ececec",
                                        border: ["#fbfbfb", "#8b8b8b"],
                                        outline: "#656565",
                                        infoFill: "#656565"
                                }
                        })
                        if(typeof modesHelper === "object" && modesHelper && typeof modesHelper.registerCategories === "function"){
                                modesHelper.registerCategories(assets.categories)
                        }
                }), "api/categories")
		
		var url = gameConfig.assets_baseurl + "img/vectors.json" + this.queryString
		this.addPromise(this.ajax(url).then(response => {
			vectors = JSON.parse(response)
		}), url)
		
                this.afterJSCount =
                        [
                                "api/songs",
                                "api/modes",
                                "blurPerformance",
                                "categories"
                        ].length +
			assets.audioSfx.length +
			assets.audioMusic.length +
			assets.audioSfxLR.length +
			assets.audioSfxLoud.length +
			(gameConfig.accounts ? 1 : 0)
		
		Promise.all(this.promises).then(() => {
			if(this.error){
				return
			}
			
			var style = document.createElement("style")
			style.appendChild(document.createTextNode(css.join("\n")))
			document.head.appendChild(style)
			
                        this.addPromise(this.loadSongsCatalog().then(songs => {
                                songs = songs.filter(song => song && typeof song === "object");
                                songs.forEach(song => {
                                        const stableId = typeof song.id === "string" ? song.id : (typeof song.stableId === "string" ? song.stableId : "");
                                        if(typeof song.legacy_id === "number"){
                                                song.numericId = song.legacy_id;
                                                song.id = song.legacy_id;
                                        }else if(typeof song.id !== "string" && typeof song.id !== "number"){
                                                song.id = stableId;
                                        }
                                        song.stableId = stableId || (typeof song.id === "string" ? song.id : "");
                                        song.enabled = song.enabled !== false;
                                        if(!Array.isArray(song.import_issues)){
                                                song.import_issues = [];
                                        }
                                        if(!song.paths || typeof song.paths !== "object"){
                                                song.paths = {};
                                        }
                                        if(!song.type){
                                                song.type = "tja";
                                        }
                                        if(typeof song.preview_available === "boolean"){
                                                song.previewAvailable = song.preview_available;
                                        }else if(typeof song.previewAvailable === "boolean"){
                                                song.previewAvailable = !!song.previewAvailable;
                                        }else if(Object.prototype.hasOwnProperty.call(song, "previewAvailable")){
                                                song.previewAvailable = !!song.previewAvailable;
                                        }else{
                                                song.previewAvailable = undefined;
                                        }
                                });
                                songs = songs.filter(song => song.enabled !== false);
                                songs.forEach(song => {
                                        if(typeof modesHelper === "object" && modesHelper && typeof modesHelper.enrichSongMetadata === "function"){
                                                modesHelper.enrichSongMetadata(song)
                                        }
                                        var paths = song.paths || {}
                                        if(!Array.isArray(song.import_issues)){
                                                song.import_issues = []
                                        }
                                        var stableId = song.stableId || song.id || ""
                                        var dirUrl = paths.dir_url || (gameConfig.songs_baseurl + stableId + "/")
                                        if(dirUrl.slice(-1) !== "/"){
                                                dirUrl += "/"
                                        }
                                        paths.dir_url = dirUrl
                                        if(!paths.tja_url){
                                                paths.tja_url = dirUrl + "main.tja"
                                        }
                                        if(paths.audio_url){
                                                song.music = new RemoteFile(paths.audio_url)
                                        }else if(song.music_type){
                                                paths.audio_url = dirUrl + "main." + song.music_type
                                                song.music = new RemoteFile(paths.audio_url)
                                        }

                                        var difficultyOrder = ["easy", "normal", "hard", "oni", "ura"]
                                        var difficultyDetails = {}
                                        var courseInfo = {}
                                        difficultyOrder.forEach(diff => {
                                                courseInfo[diff] = null
                                                difficultyDetails[diff] = null
                                        })
                                        var diffs = song.difficulties && typeof song.difficulties === "object" ? song.difficulties : {}
                                        const isValid = d => d && typeof d === "object" ? d.valid !== false : d === true
                                        let playable = Object.entries(diffs).filter(([_, d]) => isValid(d))
                                        if(playable.length === 0 && window.catalogAssumeValid){
                                                song.difficulties = { oni: { valid: true } }
                                                diffs = song.difficulties
                                                playable = [["oni", { valid: true }]]
                                        }
                                        var validCourses = 0
                                        difficultyOrder.forEach(diff => {
                                                var entry = diffs[diff]
                                                if(entry && typeof entry === "object"){
                                                        var issues = Array.isArray(entry.issues) ? entry.issues.filter(issue => typeof issue === "string") : []
                                                        var stars = 0
                                                        if(typeof entry.stars === "number" && isFinite(entry.stars)){
                                                                stars = entry.stars
                                                        }else if(typeof entry.level === "number" && isFinite(entry.level)){
                                                                stars = entry.level
                                                        }else if(typeof entry.stars === "string" || typeof entry.level === "string"){
                                                                var parsedStars = parseInt(entry.stars || entry.level, 10)
                                                                if(!isNaN(parsedStars)){
                                                                        stars = parsedStars
                                                                }
                                                        }
                                                        var branch = entry.branch === true
                                                        var valid = entry.valid !== false
                                                        var info = {
                                                                stars: stars,
                                                                branch: branch,
                                                                valid: valid,
                                                                issues: issues
                                                        }
                                                        difficultyDetails[diff] = info
                                                        if(valid){
                                                                courseInfo[diff] = info
                                                                validCourses++
                                                        }
                                                }else if(entry === true || typeof entry === "number"){
                                                        var starsValue = typeof entry === "number" && isFinite(entry) ? entry : 0
                                                        var info = {
                                                                stars: starsValue,
                                                                branch: false,
                                                                valid: true,
                                                                issues: []
                                                        }
                                                        difficultyDetails[diff] = info
                                                        courseInfo[diff] = info
                                                        validCourses++
                                                }
                                        })
                                        song.chartDetails = difficultyDetails
                                        const rawValidChartsLegacy = typeof song.valid_charts === "number" && isFinite(song.valid_charts) ? song.valid_charts : null
                                        const rawValidChartCount = typeof song.valid_chart_count === "number" && isFinite(song.valid_chart_count) ? song.valid_chart_count : null
                                        var declaredValid = rawValidChartsLegacy !== null ? rawValidChartsLegacy : (rawValidChartCount !== null ? rawValidChartCount : 0)
                                        if(!declaredValid){
                                                declaredValid = validCourses
                                        }
                                        song.valid_charts = declaredValid
                                        song.valid_chart_count = declaredValid
                                        song.hasValidCharts = declaredValid > 0
                                        const assumedViaCatalog = window.catalogAssumeValid && song.is_playable && validCourses > 0 && (!rawValidChartsLegacy || rawValidChartsLegacy <= 0) && (!rawValidChartCount || rawValidChartCount <= 0)
                                        song.catalogAssumeValid = assumedViaCatalog
                                        song.courses = song.hasValidCharts ? courseInfo : null

                                        if(song.type === "tja"){
                                                var fallbackUrl = paths.tja_url || (dirUrl + "main.tja")
                                                song.chart = fallbackUrl ? new RemoteFile(fallbackUrl) : null
                                        }else if(song.courses){
                                                song.chart = {separateDiff: true}
                                                for(var diff in song.courses){
                                                        if(song.courses[diff]){
                                                                song.chart[diff] = new RemoteFile(dirUrl + diff + ".osu")
                                                        }
                                                }
                                        }else{
                                                song.chart = null
                                        }

                                        if(song.lyrics){
                                                song.lyricsFile = new RemoteFile(dirUrl + "main.vtt")
                                        }
                                        if(song.previewAvailable === true){
                                                song.previewMusic = new RemoteFile(dirUrl + "preview." + gameConfig.preview_type)
                                        }else if(song.previewAvailable === false){
                                                song.previewMusic = null
                                        }
                                })
                                assets.songsDefault = songs
                                assets.songs = assets.songsDefault
                        }), "api/songs")
			
			var categoryPromises = []
			assets.categories //load category backgrounds to DOM
				.filter(cat => cat.songSkin && cat.songSkin.bg_img)
				.forEach(cat => {
					let name = cat.songSkin.bg_img
					var url = gameConfig.assets_baseurl + "img/" + name
					categoryPromises.push(loader.ajax(url, request => {
						request.responseType = "blob"
					}).then(blob => {
						var id = this.getFilename(name)
						var image = document.createElement("img")
						let blobUrl = URL.createObjectURL(blob)
						var promise = pageEvents.load(image)
						image.id = name
						image.src = blobUrl
						this.assetsDiv.appendChild(image)
						assets.image[id] = image
						return promise
					}).catch(response => {
						return this.errorMsg(response, url)
					}))
				})
			this.addPromise(Promise.all(categoryPromises))
			
			snd.buffer = new SoundBuffer()
			if(!oggSupport){
				snd.buffer.oggDecoder = snd.buffer.fallbackDecoder
			}
			snd.musicGain = snd.buffer.createGain()
			snd.sfxGain = snd.buffer.createGain()
			snd.previewGain = snd.buffer.createGain()
			snd.sfxGainL = snd.buffer.createGain("left")
			snd.sfxGainR = snd.buffer.createGain("right")
			snd.sfxLoudGain = snd.buffer.createGain()
			snd.buffer.setCrossfade(
				[snd.musicGain, snd.previewGain],
				[snd.sfxGain, snd.sfxGainL, snd.sfxGainR],
				0.5
			)
			snd.sfxLoudGain.setVolume(1.2)
			snd.buffer.saveSettings()
			
			this.afterJSCount = 0
			
			assets.audioSfx.forEach(name => {
				this.addPromise(this.loadSound(name, snd.sfxGain), this.soundUrl(name))
			})
			assets.audioMusic.forEach(name => {
				this.addPromise(this.loadSound(name, snd.musicGain), this.soundUrl(name))
			})
			assets.audioSfxLR.forEach(name => {
				this.addPromise(this.loadSound(name, snd.sfxGain).then(sound => {
					var id = this.getFilename(name)
					assets.sounds[id + "_p1"] = assets.sounds[id].copy(snd.sfxGainL)
					assets.sounds[id + "_p2"] = assets.sounds[id].copy(snd.sfxGainR)
				}), this.soundUrl(name))
			})
			assets.audioSfxLoud.forEach(name => {
				this.addPromise(this.loadSound(name, snd.sfxLoudGain), this.soundUrl(name))
			})
			
			this.canvasTest = new CanvasTest()
			this.addPromise(this.canvasTest.blurPerformance().then(result => {
				perf.blur = result
				if(result > 1000 / 50){
					// Less than 50 fps with blur enabled
					disableBlur = true
				}
			}), "blurPerformance")
			
			if(gameConfig.accounts){
				this.addPromise(this.ajax("api/scores/get").then(response => {
					response = JSON.parse(response)
					if(response.status === "ok"){
						account.loggedIn = true
						account.username = response.username
						account.displayName = response.display_name
						account.don = response.don
						scoreStorage.load(response.scores)
						pageEvents.send("login", account.username)
					}
				}), "api/scores/get")
			}
			
			settings = new Settings()
			pageEvents.setKbd()
			scoreStorage = new ScoreStorage()
			db = new IDB("taiko", "store")
			plugins = new Plugins()
			
			if(localStorage.getItem("lastSearchQuery")){
				localStorage.removeItem("lastSearchQuery")
			}
			
			Promise.all(this.promises).then(() => {
				if(this.error){
					return
				}
				if(!account.loggedIn){
					scoreStorage.load()
				}
				for(var i in assets.songsDefault){
					var song = assets.songsDefault[i]
					if(!song.hash){
						song.hash = song.title
					}
					scoreStorage.songTitles[song.title] = song.hash
					var score = scoreStorage.get(song.hash, false, true)
					if(score){
						score.title = song.title
					}
				}
				var promises = []
				
				var readyEvent = "normal"
				var songId
				var hashLower = location.hash.toLowerCase()
				p2 = new P2Connection()
				if(hashLower.startsWith("#song=")){
					var number = parseInt(location.hash.slice(6))
					if(number > 0){
						songId = number
						readyEvent = "song-id"
					}
				}else if(location.hash.length === 6){
					p2.hashLock = true
					promises.push(new Promise(resolve => {
						p2.open()
						pageEvents.add(p2, "message", response => {
							if(response.type === "session"){
								pageEvents.send("session-start", "invited")
								readyEvent = "session-start"
								resolve()
							}else if(response.type === "gameend"){
								p2.hash("")
								p2.hashLock = false
								readyEvent = "session-expired"
								resolve()
							}
						})
						p2.send("invite", {
							id: location.hash.slice(1).toLowerCase(),
							name: account.loggedIn ? account.displayName : null,
							don: account.loggedIn ? account.don : null
						})
						setTimeout(() => {
							if(p2.socket.readyState !== 1){
								p2.hash("")
								p2.hashLock = false
								resolve()
							}
						}, 10000)
					}).then(() => {
						pageEvents.remove(p2, "message")
					}))
				}else{
					p2.hash("")
				}
				
				promises.push(this.canvasTest.drawAllImages().then(result => {
					perf.allImg = result
				}))
				
				if(gameConfig.plugins){
					gameConfig.plugins.forEach(obj => {
						if(obj.url){
							var plugin = plugins.add(obj.url, {
								hide: obj.hide
							})
							if(plugin){
								plugin.loadErrors = true
								promises.push(plugin.load(true).then(() => {
									if(obj.start){
										return plugin.start(false, true)
									}
								}).catch(response => {
									return this.errorMsg(response, obj.url)
								}))
							}
						}
					})
				}
				
				Promise.all(promises).then(() => {
					perf.load = Date.now() - this.startTime
					this.canvasTest.clean()
					this.clean()
					this.callback(songId)
					this.ready = true
					pageEvents.send("ready", readyEvent)
				}, e => this.errorMsg(e))
			}, e => this.errorMsg(e))
		})
	}
	addPromise(promise, url){
		this.promises.push(promise)
		promise.then(this.assetLoaded.bind(this), response => {
			return this.errorMsg(response, url)
		})
	}
	soundUrl(name){
		return gameConfig.assets_baseurl + "audio/" + name
	}
	loadSound(name, gain){
		var id = this.getFilename(name)
		return gain.load(new RemoteFile(this.soundUrl(name))).then(sound => {
			assets.sounds[id] = sound
		})
	}
	getFilename(name){
		return name.slice(0, name.lastIndexOf("."))
	}
	errorMsg(error, url){
		var rethrow
		if(url || error){
			if(typeof error === "object" && error.constructor === Error){
				rethrow = error
				error = error.stack || ""
				var index = error.indexOf("\n    ")
				if(index !== -1){
					error = error.slice(0, index)
				}
			}else if(Array.isArray(error)){
				error = error[0]
			}
			if(url){
				error = (error ? error + ": " : "") + url
			}
			this.errorMessages.push(error)
			pageEvents.send("loader-error", url || error)
		}
		if(!this.error){
			this.error = true
			cancelTouch = false
			this.loaderDiv.classList.add("loaderError")
			if(typeof allStrings === "object"){
				var lang = localStorage.lang
				if(!lang){
					var userLang = navigator.languages.slice()
					userLang.unshift(navigator.language)
					for(var i in userLang){
						for(var j in allStrings){
							if(allStrings[j].regex.test(userLang[i])){
								lang = j
							}
						}
					}
				}
				if(!lang){
					lang = "en"
				}
				loader.screen.getElementsByClassName("view-content")[0].innerText = allStrings[lang] && allStrings[lang].errorOccured || allStrings.en.errorOccured
			}
			var loaderError = loader.screen.getElementsByClassName("loader-error-div")[0]
			loaderError.style.display = "flex"
			var diagTxt = loader.screen.getElementsByClassName("diag-txt")[0]
			var debugLink = loader.screen.getElementsByClassName("debug-link")[0]
			if(navigator.userAgent.indexOf("Android") >= 0){
				var iframe = document.createElement("iframe")
				diagTxt.appendChild(iframe)
				var body = iframe.contentWindow.document.body
				body.setAttribute("style", `
					font-family: monospace;
					margin: 2px 0 0 2px;
					white-space: pre-wrap;
					word-break: break-all;
					cursor: text;
				`)
				body.setAttribute("onblur", `
					getSelection().removeAllRanges()
				`)
				this.errorTxt = {
					element: body,
					method: "innerText"
				}
			}else{
				var textarea = document.createElement("textarea")
				textarea.readOnly = true
				diagTxt.appendChild(textarea)
				if(!this.touchEnabled){
					textarea.addEventListener("focus", () => {
						textarea.select()
					})
					textarea.addEventListener("blur", () => {
						getSelection().removeAllRanges()
					})
				}
				this.errorTxt = {
					element: textarea,
					method: "value"
				}
			}
			var show = () => {
				diagTxt.style.display = "block"
				debugLink.style.display = "none"
			}
			debugLink.addEventListener("click", show)
			debugLink.addEventListener("touchstart", show)
			this.clean(true)
		}
		var percentage = Math.floor(this.loadedAssets * 100 / (this.promises.length + this.afterJSCount))
		this.errorTxt.element[this.errorTxt.method] = "```\n" + this.errorMessages.join("\n") + "\nPercentage: " + percentage + "%\n```"
		if(rethrow || error){
			console.error(rethrow || error)
		}
		return Promise.reject()
	}
	assetLoaded(){
		if(!this.error){
			this.loadedAssets++
			var percentage = Math.floor(this.loadedAssets * 100 / (this.promises.length + this.afterJSCount))
			this.loaderProgress.style.width = percentage + "%"
			this.loaderPercentage.firstChild.data = percentage + "%"
		}
	}
	changePage(name, patternBg){
		this.screen.innerHTML = assets.pages[name]
		this.screen.classList[patternBg ? "add" : "remove"]("pattern-bg")
	}
	cssRuleset(rulesets){
		var css = []
		for(var selector in rulesets){
			var declarationsObj = rulesets[selector]
			var declarations = []
			for(var property in declarationsObj){
				var value = declarationsObj[property]
				declarations.push("\t" + property + ": " + value + ";")
			}
			css.push(selector + "{\n" + declarations.join("\n") + "\n}")
		}
		return css.join("\n")
	}
        ajax(url, customRequest, customResponse){
                var request = new XMLHttpRequest()
                request.open("GET", url)
                var promise = pageEvents.load(request)
                if(!customResponse){
                        promise = promise.then(() => {
                                if(request.status === 304){
                                        return {__notModified: true}
                                }
                                if(request.status === 200){
                                        return request.response
                                }
                                return Promise.reject(`${url} (${request.status})`)
                        })
                }
                if(customRequest){
                        customRequest(request)
                }
                request.send()
                return promise
        }

        loadSongsCatalog(){
                if(!USE_DETAILS_IN_CATALOG){
                        console.warn("details-batcher disabled for catalog")
                }

                const loaderInstance = this
                const supportsFetch = typeof fetch === "function"
                const catalogUrl = "api/songs"
                const cachedList = Array.isArray(songsCatalogCache.lastResult) ? songsCatalogCache.lastResult.slice() : null

                function normaliseEntries(payload){
                        if(Array.isArray(payload)){
                                return payload
                        }
                        if(payload && typeof payload === "object" && Array.isArray(payload.entries)){
                                return payload.entries
                        }
                        if(typeof payload === "string" && payload){
                                try{
                                        const parsed = JSON.parse(payload)
                                        return Array.isArray(parsed) ? parsed : []
                                }catch(e){
                                        return []
                                }
                        }
                        return []
                }

                function rememberEtag(value){
                        if(typeof value === "string" && value){
                                songsCatalogCache.etag = value
                        }
                }

                async function performRequest(url, bypassCache){
                        if(supportsFetch){
                                const headers = {}
                                if(!bypassCache && typeof songsCatalogCache.etag === "string" && songsCatalogCache.etag){
                                        headers["If-None-Match"] = songsCatalogCache.etag
                                }
                                if(bypassCache){
                                        headers["Cache-Control"] = "no-cache"
                                        headers["Pragma"] = "no-cache"
                                }
                                const init = {
                                        method: "GET",
                                        credentials: "same-origin",
                                        headers,
                                }
                                if(bypassCache){
                                        init.cache = "no-store"
                                }
                                const response = await fetch(url, init)
                                const responseEtag = response.headers ? response.headers.get("ETag") : null
                                rememberEtag(responseEtag)
                                if(response.status === 304){
                                        return {notModified: true}
                                }
                                if(response.status !== 200){
                                        const error = new Error(`${url} (${response.status})`)
                                        error.status = response.status
                                        throw error
                                }
                                return {body: await response.json()}
                        }

                        let responseEtag = null
                        const result = await loaderInstance
                                .ajax(url, request => {
                                        if(!bypassCache && typeof songsCatalogCache.etag === "string" && songsCatalogCache.etag){
                                                try{
                                                        request.setRequestHeader("If-None-Match", songsCatalogCache.etag)
                                                }catch(e){}
                                        }
                                        if(bypassCache){
                                                try{
                                                        request.setRequestHeader("Cache-Control", "no-cache")
                                                }catch(e){}
                                                try{
                                                        request.setRequestHeader("Pragma", "no-cache")
                                                }catch(e){}
                                        }
                                        request.addEventListener("load", () => {
                                                try{
                                                        responseEtag = request.getResponseHeader("ETag")
                                                        rememberEtag(responseEtag)
                                                }catch(e){}
                                        })
                                })
                                .catch(error => {
                                        if(error && typeof error === "object" && error.__notModified){
                                                return {__notModified: true}
                                        }
                                        throw error
                                })
                        if(result && typeof result === "object" && result.__notModified){
                                return {notModified: true}
                        }
                        rememberEtag(responseEtag)
                        return {body: result}
                }

                async function fetchEntries(){
                        let response
                        try{
                                response = await performRequest(catalogUrl, false)
                        }catch(error){
                                if(Array.isArray(cachedList)){
                                        return {entries: cachedList, fromCache: true}
                                }
                                throw error
                        }

                        if(response.notModified){
                                if(Array.isArray(cachedList)){
                                        return {entries: cachedList, fromCache: true}
                                }
                                const bypassUrl = `${catalogUrl}${catalogUrl.indexOf("?") === -1 ? "?" : "&"}_=${Date.now()}`
                                const retry = await performRequest(bypassUrl, true)
                                if(retry.notModified){
                                        return {entries: []}
                                }
                                return {entries: normaliseEntries(retry.body), fromCache: false}
                        }

                        return {entries: normaliseEntries(response.body), fromCache: false}
                }

                return fetchEntries()
                        .then(result => {
                                const entries = Array.isArray(result.entries) ? result.entries : []
                                if(result.fromCache){
                                        return entries.slice()
                                }
                                const normalized = entries.map(entry => {
                                        if(entry && typeof entry === "object"){
                                                return Object.assign({}, entry)
                                        }
                                        return entry
                                })
                                songsCatalogCache.lastResult = normalized
                                return normalized.slice()
                        })
                        .catch(() => {
                                if(Array.isArray(cachedList)){
                                        return cachedList.slice()
                                }
                                return []
                        })
        }
        loadModesManifest(){
                const existingStore = typeof window !== "undefined" ? window.__modes__ : null
                const now = Date.now()
                if(
                        existingStore &&
                        typeof existingStore.cacheTtlMs === "number" &&
                        existingStore.cacheTtlMs > 0 &&
                        now - (existingStore.fetchedAt || 0) < existingStore.cacheTtlMs
                ){
                        commitModesStore(existingStore)
                        return Promise.resolve()
                }
                return this.ajax("api/modes").then(response => {
                        let manifest
                        try{
                                manifest = JSON.parse(response)
                        }catch(e){
                                const errorStore = {
                                        manifest: null,
                                        status: "error",
                                        fetchedAt: Date.now(),
                                        cacheTtlMs: DEFAULT_MODES_MANIFEST_CACHE_TTL_MS,
                                        categoryIndex: {},
                                }
                                commitModesStore(errorStore)
                                return
                        }
                        if(!manifest || typeof manifest !== "object"){
                                const invalidStore = {
                                        manifest: null,
                                        status: "error",
                                        fetchedAt: Date.now(),
                                        cacheTtlMs: DEFAULT_MODES_MANIFEST_CACHE_TTL_MS,
                                        categoryIndex: {},
                                }
                                commitModesStore(invalidStore)
                                return
                        }
                        const status = resolveManifestStatus(manifest)
                        const ttlSeconds = Number(manifest.cache_ttl)
                        const cacheTtlMs = Number.isFinite(ttlSeconds) && ttlSeconds > 0 ? ttlSeconds * 1000 : DEFAULT_MODES_MANIFEST_CACHE_TTL_MS
                        const isOk = status === "ok"
                        const store = {
                                manifest: isOk ? manifest : null,
                                status,
                                fetchedAt: Date.now(),
                                cacheTtlMs,
                                categoryIndex: isOk ? buildModesCategoryIndex(manifest) : {},
                        }
                        commitModesStore(store)
                }).catch(() => {})
        }
	loadScript(url){
		var script = document.createElement("script")
		var url = url + this.queryString
		var promise = pageEvents.load(script)
		script.src = url
		document.head.appendChild(script)
		return promise
	}
	getCsrfToken(){
		return this.ajax("api/csrftoken").then(response => {
			var json = JSON.parse(response)
			if(json.status === "ok"){
				return Promise.resolve(json.token)
			}else{
				return Promise.reject()
			}
		})
	}
	clean(error){
		delete this.loaderDiv
		delete this.loaderPercentage
		delete this.loaderProgress
		if(!error){
			delete this.promises
			delete this.errorText
		}
		pageEvents.remove(root, "touchstart")
	}
}

;(function setupRestNotesLoaderRegistration(){
	const globalObject = typeof window !== "undefined" ? window : (typeof globalThis !== "undefined" ? globalThis : this)
	if(!globalObject){
		return
	}
	const registerWithLoader = () => {
		if(typeof globalObject.registerRestNotesLoader === "function"){
			try{
				globalObject.registerRestNotesLoader(Loader)
			}catch(error){
				console.warn("[notes-loader] register failed", error)
			}
		}
	}
	if(typeof globalObject.registerRestNotesLoader === "function"){
		registerWithLoader()
	}else{
		const queue = globalObject.__restNotesLoaderRegistrations__
		if(Array.isArray(queue)){
			queue.push(registerWithLoader)
		}else{
			globalObject.__restNotesLoaderRegistrations__ = [registerWithLoader]
		}
	}
})()
