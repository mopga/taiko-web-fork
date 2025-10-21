const DEFAULT_MODES_MANIFEST_CACHE_TTL_MS = 12000;
const songsCatalogCache = {
        pages: Object.create(null),
        lastResult: [],
        details: Object.create(null),
};

function createRequestPool(limit){
        const maxConcurrency = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 1;
        const queue = [];
        let active = 0;

        function schedule(){
                if(active >= maxConcurrency){
                        return;
                }
                const task = queue.shift();
                if(!task){
                        return;
                }
                active++;
                let result;
                try{
                        result = task.fn();
                }catch(err){
                        active--;
                        task.reject(err);
                        schedule();
                        return;
                }
                Promise.resolve(result).then(value => {
                        active--;
                        task.resolve(value);
                        schedule();
                }).catch(error => {
                        active--;
                        task.reject(error);
                        schedule();
                });
        }

        return {
                enqueue(fn){
                        return new Promise((resolve, reject) => {
                                queue.push({fn, resolve, reject});
                                schedule();
                        });
                },
        };
}

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

                                        if(song.type === "tja"){
                                                var charts = Array.isArray(song.charts) ? song.charts : []
                                                var canonicalMap = {
                                                        "Easy": "easy",
                                                        "Normal": "normal",
                                                        "Hard": "hard",
                                                        "Oni": "oni",
                                                        "UraOni": "ura"
                                                }
                                                var courseOrder = ["easy", "normal", "hard", "oni", "ura"]
                                                var courseInfo = {}
                                                var chartDetails = {}
                                                courseOrder.forEach(diff => courseInfo[diff] = null)
                                                var coursesByDiff = {}
                                                charts.forEach(chart => {
                                                        var canonical = chart.canonical_course
                                                        var courseKey = null
                                                        if(canonical && canonicalMap[canonical]){
                                                                courseKey = canonicalMap[canonical]
                                                        }else if(typeof chart.course === "string"){
                                                                var lowered = chart.course.toLowerCase()
                                                                var matched = null
                                                                for(var name in canonicalMap){
                                                                        if(canonicalMap[name] === lowered){
                                                                                matched = canonicalMap[name]
                                                                                break
                                                                        }
                                                                }
                                                                courseKey = matched || lowered
                                                        }
                                                        if(!courseKey || courseOrder.indexOf(courseKey) === -1){
                                                                return
                                                        }
                                                        if(!coursesByDiff[courseKey] || (chart.valid && !coursesByDiff[courseKey].valid)){
                                                                coursesByDiff[courseKey] = chart
                                                        }
                                                })
                                                song.courses = courseInfo
                                                Object.keys(coursesByDiff).forEach(diff => {
                                                        var chart = coursesByDiff[diff]
                                                        var sourceUrl = chart.tja_url || null
                                                        var info = {
                                                                stars: chart.level || 0,
                                                                branch: !!chart.branch,
                                                                valid: !!chart.valid,
                                                                issues: chart.issues || [],
                                                                tja_url: sourceUrl
                                                        }
                                                        chartDetails[diff] = info
                                                        song.courses[diff] = info.valid ? info : null
                                                })
                                                song.chartDetails = chartDetails
                                                var validCount = song.valid_chart_count
                                                if(typeof validCount !== "number"){
                                                        validCount = Object.values(song.courses).filter(info => info && info.valid).length
                                                }
                                                song.hasValidCharts = validCount > 0
                                                if(!song.hasValidCharts){
                                                        song.courses = null
                                                }

                                                var uniqueUrls = new Set()
                                                Object.values(song.chartDetails).forEach(info => {
                                                        if(info && info.tja_url){
                                                                uniqueUrls.add(info.tja_url)
                                                        }
                                                })

                                                if(uniqueUrls.size === 0){
                                                        var fallbackUrl = paths.tja_url || (dirUrl + "main.tja")
                                                        song.chart = fallbackUrl ? new RemoteFile(fallbackUrl) : null
                                                }else if(uniqueUrls.size === 1){
                                                        var singleUrl = uniqueUrls.values().next().value
                                                        song.chart = new RemoteFile(singleUrl)
                                                }else{
                                                        song.chart = {separateDiff: true}
                                                        Object.entries(song.chartDetails).forEach(([diff, info]) => {
                                                                if(info && info.tja_url){
                                                                        song.chart[diff] = new RemoteFile(info.tja_url)
                                                                }
                                                        })
                                                }
                                                Object.values(song.chartDetails).forEach(info => {
                                                        if(info){
                                                                delete info.tja_url
                                                        }
                                                })
                                        }else{
                                                song.chart = {separateDiff: true}
                                                for(var diff in song.courses){
                                                        if(song.courses[diff]){
                                                                song.chart[diff] = new RemoteFile(dirUrl + diff + ".osu")
                                                        }
                                                }
                                        }

                                        if(song.lyrics){
                                                song.lyricsFile = new RemoteFile(dirUrl + "main.vtt")
                                        }
                                        const previewUrl = typeof paths.preview_url === "string" && paths.preview_url ? paths.preview_url : null
                                        if(typeof song.preview_available !== "boolean"){
                                                song.preview_available = !!(previewUrl || song.preview > 0)
                                        }
                                        if(previewUrl){
                                                song.previewMusic = new RemoteFile(previewUrl)
                                        }else if(song.preview_available === false){
                                                song.previewMusic = null
                                        }else if(song.preview > 0){
                                                const previewPath = dirUrl + "preview." + gameConfig.preview_type
                                                paths.preview_url = previewPath
                                                song.previewMusic = new RemoteFile(previewPath)
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
                if(typeof customResponse === "function"){
                        promise = promise.then(() => customResponse(request))
                }else{
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
                const MAX_CONCURRENCY = 6
                const DETAIL_BATCH_SIZE = 50
                const HARD_PAGE_CAP_DEFAULT = 200
                const MIN_PAGE_CAP = 5
                const LIMIT = 200
                const collected = []
                const loaderInstance = this
                const detailCache = songsCatalogCache.details || (songsCatalogCache.details = Object.create(null))
                let pageCache = songsCatalogCache.pages
                const requestPool = createRequestPool(MAX_CONCURRENCY)
                const supportsFetch = typeof fetch === "function"
                let batchEndpointAvailable = typeof songsCatalogCache.batchEndpointAvailable === "boolean" ? songsCatalogCache.batchEndpointAvailable : true
                let totalCount = typeof songsCatalogCache.totalCount === "number" ? songsCatalogCache.totalCount : null
                let hardPageCap = HARD_PAGE_CAP_DEFAULT

                function normalisePageCache(existing){
                        if(existing instanceof Map){
                                return existing
                        }
                        const map = new Map()
                        if(existing && typeof existing === "object"){
                                Object.keys(existing).forEach(key => {
                                        const numericKey = parseInt(key, 10)
                                        if(Number.isFinite(numericKey) && Array.isArray(existing[key])){
                                                map.set(numericKey, existing[key].slice())
                                        }
                                })
                        }
                        return map
                }

                function recomputePageCap(){
                        if(Number.isFinite(totalCount) && totalCount > 0){
                                const computed = Math.ceil(totalCount / LIMIT)
                                if(Number.isFinite(computed) && computed > 0){
                                        hardPageCap = Math.min(HARD_PAGE_CAP_DEFAULT, Math.max(MIN_PAGE_CAP, computed))
                                        return
                                }
                        }
                        hardPageCap = HARD_PAGE_CAP_DEFAULT
                }

                pageCache = normalisePageCache(pageCache)
                songsCatalogCache.pages = pageCache
                recomputePageCap()

                function cachePage(pageNumber, items){
                        if(!(pageCache instanceof Map)){
                                return
                        }
                        pageCache.set(pageNumber, Array.isArray(items) ? items.slice() : [])
                }

                function getCachedPage(pageNumber){
                        if(!(pageCache instanceof Map)){
                                return null
                        }
                        if(!pageCache.has(pageNumber)){
                                return null
                        }
                        const cached = pageCache.get(pageNumber)
                        return Array.isArray(cached) ? cached.slice() : null
                }

                function pruneCacheAfter(pageNumber){
                        if(!(pageCache instanceof Map)){
                                return
                        }
                        const keysToDelete = []
                        pageCache.forEach((_, key) => {
                                if(key > pageNumber){
                                        keysToDelete.push(key)
                                }
                        })
                        keysToDelete.forEach(key => pageCache.delete(key))
                }

                function appendItems(items){
                        if(!Array.isArray(items)){
                                return
                        }
                        items.forEach(detail => {
                                if(detail && typeof detail === "object"){
                                        collected.push(detail)
                                }
                        })
                }

                function makeHeadersShim(request){
                        return {
                                get(name){
                                        try{
                                                return request.getResponseHeader(name)
                                        }catch(e){
                                                return null
                                        }
                                },
                        }
                }

                function updateTotalCountFromHeaders(headers){
                        if(!headers || typeof headers.get !== "function"){
                                return
                        }
                        const headerValue = headers.get("X-Total-Count")
                        if(!headerValue){
                                return
                        }
                        const parsed = parseInt(headerValue, 10)
                        if(Number.isFinite(parsed) && parsed >= 0){
                                totalCount = parsed
                                songsCatalogCache.totalCount = parsed
                                recomputePageCap()
                        }
                }

                function performRequest(url, bypassCache){
                        if(supportsFetch){
                                return requestPool.enqueue(() => {
                                        const init = {
                                                method: "GET",
                                                credentials: "same-origin",
                                        }
                                        if(bypassCache){
                                                init.cache = "no-store"
                                                init.headers = {
                                                        "Cache-Control": "no-cache",
                                                        "Pragma": "no-cache",
                                                }
                                        }
                                        return fetch(url, init).then(response => {
                                                const headers = response.headers
                                                if(response.status === 304){
                                                        return {notModified: true, headers}
                                                }
                                                if(response.ok){
                                                        return response.text().then(body => ({body, headers}))
                                                }
                                                const error = new Error(`${url} (${response.status})`)
                                                error.status = response.status
                                                throw error
                                        })
                                })
                        }
                        return requestPool.enqueue(() => loaderInstance.ajax(
                                url,
                                request => {
                                        if(bypassCache){
                                                try{
                                                        request.setRequestHeader("Cache-Control", "no-cache")
                                                }catch(e){}
                                                try{
                                                        request.setRequestHeader("Pragma", "no-cache")
                                                }catch(e){}
                                        }
                                },
                                request => {
                                        const headers = makeHeadersShim(request)
                                        if(request.status === 304){
                                                return {notModified: true, headers}
                                        }
                                        if(request.status === 200){
                                                return {body: request.response, headers}
                                        }
                                        const error = new Error(`${url} (${request.status})`)
                                        error.status = request.status
                                        throw error
                                }
                        ))
                }

                function parseEntries(payload){
                        if(Array.isArray(payload)){
                                return payload
                        }
                        if(typeof payload === "string" && payload){
                                try{
                                        return JSON.parse(payload)
                                }catch(e){
                                        return []
                                }
                        }
                        return []
                }

                async function requestDetailBatch(ids){
                        if(!Array.isArray(ids) || ids.length === 0){
                                return {}
                        }
                        const query = ids.map(id => encodeURIComponent(id)).join(",")
                        const url = `api/songs/details?ids=${query}`
                        const response = await performRequest(url, false)
                        if(response && response.headers){
                                updateTotalCountFromHeaders(response.headers)
                        }
                        if(response && response.notModified){
                                return {}
                        }
                        const body = response ? response.body : null
                        let parsed = body
                        if(typeof parsed === "string" && parsed){
                                try{
                                        parsed = JSON.parse(parsed)
                                }catch(e){
                                        parsed = []
                                }
                        }
                        const map = {}
                        if(Array.isArray(parsed)){
                                parsed.forEach(item => {
                                        if(item && typeof item === "object"){
                                                const stableId = typeof item.id === "string" ? item.id : null
                                                if(stableId){
                                                        map[stableId] = item
                                                }
                                        }
                                })
                        }
                        return map
                }

                async function requestSingleDetail(id){
                        const url = `api/song/${encodeURIComponent(id)}`
                        const response = await performRequest(url, false)
                        if(response && response.notModified){
                                return detailCache[id] || null
                        }
                        const body = response ? response.body : null
                        if(body === "" || body === null || typeof body === "undefined"){
                                return detailCache[id] || null
                        }
                        let parsed = body
                        if(typeof parsed === "string"){
                                try{
                                        parsed = JSON.parse(parsed)
                                }catch(e){
                                        parsed = null
                                }
                        }
                        if(parsed && typeof parsed === "object"){
                                detailCache[id] = parsed
                                return parsed
                        }
                        return detailCache[id] || null
                }

                async function loadDetails(entries){
                        if(!Array.isArray(entries) || entries.length === 0){
                                return []
                        }
                        const seen = new Set()
                        const order = []
                        entries.forEach(entry => {
                                if(!entry || typeof entry !== "object"){
                                        return
                                }
                                const detailId = typeof entry.id === "string" ? entry.id : ""
                                if(!detailId){
                                        return
                                }
                                order.push(detailId)
                                if(!seen.has(detailId)){
                                        seen.add(detailId)
                                }
                        })
                        const uniqueIds = Array.from(seen)
                        const missingIds = uniqueIds.filter(id => !detailCache[id])

                        async function fetchBatch(ids){
                                if(!ids.length){
                                        return
                                }
                                if(batchEndpointAvailable){
                                        try{
                                                const map = await requestDetailBatch(ids)
                                                const unresolved = []
                                                ids.forEach(id => {
                                                        const detail = map[id]
                                                        if(detail && typeof detail === "object"){
                                                                detailCache[id] = detail
                                                        }else if(!detailCache[id]){
                                                                unresolved.push(id)
                                                        }
                                                })
                                                if(unresolved.length){
                                                        await Promise.all(unresolved.map(requestSingleDetail))
                                                }
                                                return
                                        }catch(error){
                                                if(error && (error.status === 404 || error.status === 405)){
                                                        batchEndpointAvailable = false
                                                        songsCatalogCache.batchEndpointAvailable = false
                                                }else{
                                                        throw error
                                                }
                                        }
                                }
                                await Promise.all(ids.map(requestSingleDetail))
                        }

                        for(let index = 0; index < missingIds.length; index += DETAIL_BATCH_SIZE){
                                const slice = missingIds.slice(index, index + DETAIL_BATCH_SIZE)
                                await fetchBatch(slice)
                        }

                        const pageDetails = order.map(id => detailCache[id] || null).filter(detail => detail && typeof detail === "object")
                        return pageDetails
                }

                async function fetchPage(pageNumber){
                        const baseUrl = `api/songs?page=${pageNumber}&limit=${LIMIT}`
                        const hasCache = pageCache instanceof Map && pageCache.has(pageNumber)
                        let response
                        try{
                                response = await performRequest(baseUrl, false)
                        }catch(error){
                                throw error
                        }
                        if(response && response.headers){
                                updateTotalCountFromHeaders(response.headers)
                        }
                        if(response && response.notModified){
                                if(hasCache){
                                        return {__notModified: true}
                                }
                                const bypassUrl = `${baseUrl}${baseUrl.indexOf("?") === -1 ? "?" : "&"}_bypass=${Date.now()}`
                                const retry = await performRequest(bypassUrl, true)
                                if(retry && retry.headers){
                                        updateTotalCountFromHeaders(retry.headers)
                                }
                                if(retry && retry.notModified){
                                        return {body: []}
                                }
                                return retry
                        }
                        return response
                }

                async function processPage(pageNumber){
                        let response
                        try{
                                response = await fetchPage(pageNumber)
                        }catch(err){
                                const cached = getCachedPage(pageNumber)
                                if(cached && cached.length){
                                        appendItems(cached)
                                        if(cached.length < LIMIT){
                                                pruneCacheAfter(pageNumber)
                                                return false
                                        }
                                        return true
                                }
                                return false
                        }

                        if(response && response.__notModified){
                                const cached = getCachedPage(pageNumber)
                                if(Array.isArray(cached) && cached.length){
                                        appendItems(cached)
                                        if(cached.length < LIMIT){
                                                pruneCacheAfter(pageNumber)
                                                return false
                                        }
                                        return true
                                }
                                pruneCacheAfter(pageNumber)
                                return false
                        }

                        const rawEntries = response ? response.body : response
                        const entries = parseEntries(rawEntries)

                        if(!Array.isArray(entries) || entries.length === 0){
                                if(pageCache instanceof Map){
                                        pageCache.delete(pageNumber)
                                }
                                pruneCacheAfter(pageNumber)
                                return false
                        }

                        const pageDetails = await loadDetails(entries)
                        if(pageDetails.length === 0){
                                if(pageCache instanceof Map){
                                        pageCache.delete(pageNumber)
                                }
                                pruneCacheAfter(pageNumber)
                                return false
                        }

                        cachePage(pageNumber, pageDetails)
                        appendItems(pageDetails)
                        if(entries.length < LIMIT || pageDetails.length < LIMIT){
                                pruneCacheAfter(pageNumber)
                                return false
                        }
                        return true
                }

                async function loadAllPages(){
                        let page = 1
                        while(page <= hardPageCap){
                                const shouldContinue = await processPage(page)
                                if(!shouldContinue){
                                        break
                                }
                                page += 1
                        }
                }

                return loadAllPages().then(() => {
                        songsCatalogCache.batchEndpointAvailable = batchEndpointAvailable
                        if(Number.isFinite(totalCount)){
                                songsCatalogCache.totalCount = totalCount
                        }
                        if(collected.length === 0){
                                if(Array.isArray(songsCatalogCache.lastResult) && songsCatalogCache.lastResult.length){
                                        return songsCatalogCache.lastResult.slice()
                                }
                                return []
                        }
                        songsCatalogCache.lastResult = collected.slice()
                        return collected
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
