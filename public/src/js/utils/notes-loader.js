;(function(global){
        const REST_CACHE_TTL = 12000;
        const restCache = new Map();

        function normaliseToken(value){
                if(typeof value !== "string"){
                        return "";
                }
                return value.trim().toLowerCase();
        }

        function coerceNumber(value, fallback){
                if(value === null || value === undefined){
                        return fallback;
                }
                const num = Number(value);
                if(Number.isFinite(num)){
                        return num;
                }
                return fallback;
        }

        function buildRestUrl(source, songMeta, selection, modeKey){
                const endpoint = typeof source.endpoint === "string" ? source.endpoint : "";
                const params = Array.isArray(source.params) ? source.params : [];
                const query = [];
                params.forEach(param => {
                        let value = "";
                        switch(param){
                                case "title":
                                        value = selection.title || songMeta.title || songMeta.originalTitle || songMeta.id || "";
                                        break;
                                case "course":
                                case "difficulty":
                                        value = selection.course || selection.difficulty || selection.rank || "";
                                        break;
                                case "rank":
                                        value = selection.rank || selection.difficulty || selection.course || "";
                                        break;
                                case "mode":
                                        value = modeKey;
                                        break;
                                default:
                                        if(selection[param]){
                                                value = selection[param];
                                        }else if(songMeta[param]){
                                                value = songMeta[param];
                                        }
                        }
                        if(value !== undefined && value !== null && String(value).length){
                                query.push(encodeURIComponent(param) + "=" + encodeURIComponent(String(value)));
                        }
                });
                return {
                        url: endpoint + (query.length ? "?" + query.join("&") : ""),
                };
        }

        function fetchWithCache(url, ttl){
                const now = Date.now();
                const existing = restCache.get(url);
                if(existing && existing.expires > now){
                        return existing.promise;
                }
                const fetchPromise = fetch(url, {credentials: "same-origin"}).then(response => {
                        if(!response.ok){
                                throw new Error(url + " (" + response.status + ")");
                        }
                        return response.json();
                }).then(json => {
                        if(!json || typeof json !== "object" || json.status !== "ok"){
                                const message = json && json.message ? json.message : "invalid_response";
                                throw new Error(message);
                        }
                        return json;
                });
                restCache.set(url, {promise: fetchPromise, expires: now + ttl});
                fetchPromise.catch(() => {
                        restCache.delete(url);
                });
                return fetchPromise;
        }

        function noteTypeFromEntry(entry){
                const rawType = normaliseToken(entry && entry.type ? String(entry.type) : "");
                const kind = entry && entry.kind;
                const sizeToken = normaliseToken(entry && entry.size);
                const isBig = entry && entry.big === true || rawType.startsWith("dai") || sizeToken === "big" || rawType.includes("big");
                let base = rawType;
                if(rawType.startsWith("dai")){
                        base = rawType.slice(3);
                }
                if(kind === 2 || base === "ka"){
                        return isBig ? "daiKa" : "ka";
                }
                if(kind === 1 || base === "don"){
                        return isBig ? "daiDon" : "don";
                }
                return null;
        }

        function longTypeFromEntry(entry){
                const kind = normaliseToken(entry && entry.kind ? String(entry.kind) : "");
                const sizeToken = normaliseToken(entry && entry.size);
                const isBig = entry && entry.big === true || sizeToken === "big";
                if(kind === "drumroll"){
                        return isBig ? "daiDrumroll" : "drumroll";
                }
                if(kind === "balloon"){
                        return "balloon";
                }
                return null;
        }

        function buildCircleConfig(config){
                return {
                        id: config.id,
                        start: config.ms,
                        type: config.type,
                        txt: (global.strings && global.strings.note && global.strings.note[config.type]) || config.type,
                        speed: config.speed,
                        gogoTime: false,
                        endTime: config.endTime,
                        requiredHits: config.requiredHits,
                        beatMS: config.beatMS,
                        section: config.section,
                        branch: null,
                };
        }

        function convertMeasuresToParsedChart(payload, context){
                const chartData = payload && payload.chart_data ? payload.chart_data : {};
                const measures = Array.isArray(chartData.measures) ? chartData.measures : [];
                const circles = [];
                const events = [];
                const parsedMeasures = [];
                let circleId = 0;
                let lastBeatMS = null;
                let earliestMs = null;
                let firstBeatMS = null;

                measures.forEach((measure, index) => {
                        const bpm = coerceNumber(measure && measure.bpm, 120) || 120;
                        const scroll = coerceNumber(measure && measure.scroll, 1) || 1;
                        const startMs = coerceNumber(measure && measure.start_ms, 0) || 0;
                        const durationMs = coerceNumber(measure && measure.duration_ms, Math.round(240000 / bpm));
                        const beatMS = bpm > 0 ? 60000 / bpm : 600;
                        if(firstBeatMS === null){
                                firstBeatMS = beatMS;
                        }
                        const speed = bpm * scroll / 60;
                        parsedMeasures.push({
                                ms: startMs,
                                originalMS: startMs,
                                speed: speed,
                                visible: true,
                                branch: null,
                                branchFirst: index === 0,
                        });

                        if(lastBeatMS === null || Math.abs(lastBeatMS - beatMS) > 0.01){
                                events.push({
                                        ms: startMs,
                                        beatMS: beatMS,
                                        gogoTime: false,
                                        branch: null,
                                });
                                lastBeatMS = beatMS;
                        }

                        const notes = Array.isArray(measure && measure.notes) ? measure.notes : [];
                        notes.forEach((note, noteIndex) => {
                                const offset = coerceNumber(note && note.at, 0) || 0;
                                const absolute = startMs + offset;
                                const type = noteTypeFromEntry(note);
                                if(!type){
                                        return;
                                }
                                circleId++;
                                const circleCfg = buildCircleConfig({
                                        id: circleId,
                                        ms: absolute,
                                        type: type,
                                        speed: speed,
                                        endTime: absolute,
                                        requiredHits: 0,
                                        beatMS: beatMS,
                                        section: noteIndex === 0 && offset === 0,
                                });
                                const circle = new global.Circle(circleCfg);
                                circles.push(circle);
                                if(earliestMs === null || circle.ms < earliestMs){
                                        earliestMs = circle.ms;
                                }
                        });

                        const longs = Array.isArray(measure && measure.longs) ? measure.longs : [];
                        longs.forEach(longEntry => {
                                const offset = coerceNumber(longEntry && longEntry.at, 0) || 0;
                                const length = coerceNumber(longEntry && longEntry.len_ms, 0) || 0;
                                const endAt = coerceNumber(longEntry && longEntry.end_at, null);
                                const absolute = startMs + Math.max(0, offset);
                                const endMs = endAt !== null ? startMs + Math.max(endAt, offset) : absolute + Math.max(0, length);
                                const type = longTypeFromEntry(longEntry);
                                if(!type){
                                        return;
                                }
                                circleId++;
                                const circleCfg = buildCircleConfig({
                                        id: circleId,
                                        ms: absolute,
                                        type: type,
                                        speed: speed,
                                        endTime: endMs,
                                        requiredHits: coerceNumber(longEntry && (longEntry.hits || longEntry.required_hits), 1) || 1,
                                        beatMS: beatMS,
                                        section: offset === 0 && notes.length === 0,
                                });
                                const circle = new global.Circle(circleCfg);
                                circle.endTime = endMs;
                                circle.originalEndTime = endMs;
                                circles.push(circle);
                                if(earliestMs === null || circle.ms < earliestMs){
                                        earliestMs = circle.ms;
                                }
                        });
                });

                circles.sort((a, b) => a.ms - b.ms);

                if(earliestMs !== null && earliestMs < 0){
                        const offset = earliestMs;
                        circles.forEach(circle => {
                                circle.ms -= offset;
                                circle.originalMS = circle.ms;
                                circle.endTime -= offset;
                                circle.originalEndTime = circle.endTime;
                        });
                        parsedMeasures.forEach(measure => {
                                measure.ms -= offset;
                                measure.originalMS = measure.ms;
                        });
                        events.forEach(event => {
                                event.ms -= offset;
                        });
                }

                const beatInterval = circles.length ? (circles[0].beatMS || 600) : (firstBeatMS || 600);

                let durationMs = coerceNumber(chartData.duration_ms, 0);
                if(!durationMs || durationMs < 0){
                        const lastCircle = circles[circles.length - 1];
                        if(lastCircle){
                                durationMs = Math.max(lastCircle.endTime, lastCircle.ms);
                        }else if(parsedMeasures.length){
                                const lastMeasure = parsedMeasures[parsedMeasures.length - 1];
                                durationMs = lastMeasure.ms;
                        }else{
                                durationMs = 0;
                        }
                }

                const parsedChart = {
                        circles: circles,
                        measures: parsedMeasures,
                        events: events,
                        branches: null,
                        beatInfo: {beatInterval: beatInterval},
                        soundOffset: earliestMs && earliestMs < 0 ? earliestMs : 0,
                };

                const selection = context && context.selection ? context.selection : {};
                const difficulty = selection.difficulty || "oni";
                const stars = selection.stars || 0;
                if(typeof global.AutoScore === "function"){
                        const autoscore = new global.AutoScore(difficulty, stars, 2, circles);
                        parsedChart.scoremode = 2;
                        parsedChart.scoreinit = autoscore.ScoreInit;
                        parsedChart.scorediff = autoscore.ScoreDiff;
                }else{
                        parsedChart.scoremode = 2;
                        parsedChart.scoreinit = 0;
                        parsedChart.scorediff = 0;
                }

                return {
                        chart: parsedChart,
                        durationMs: durationMs,
                        meta: {
                                mode: context ? context.modeKey : null,
                                course: chartData.course || selection.difficulty || null,
                                totalNotes: chartData.total_notes || circles.length,
                        },
                };
        }

        function loadNotesForSong(songMeta, selection){
                const song = songMeta || {};
                const currentSelection = Object.assign({}, selection);
                if(!currentSelection.title){
                        currentSelection.title = song.title || song.originalTitle || song.id || "";
                }
                const resolver = global.modesHelper ? global.modesHelper.resolveSongMode(song, currentSelection) : {modeKey: "standard", definition: {notes_source: {type: "builtin", format: "engine-v1"}}};
                const modeKey = resolver.modeKey || "standard";
                const source = resolver.definition && resolver.definition.notes_source ? resolver.definition.notes_source : {type: "builtin", format: "engine-v1"};

                if(!source || source.type === "builtin" || source.type === "engine" || source.type === "internal"){
                        return {
                                type: "builtin",
                                modeKey: modeKey,
                                source: source,
                        };
                }

                if(source.type === "rest"){
                        const plan = buildRestUrl(source, song, currentSelection, modeKey);
                        const promise = fetchWithCache(plan.url, REST_CACHE_TTL).then(json => {
                                return convertMeasuresToParsedChart(json, {modeKey: modeKey, selection: currentSelection});
                        });
                        return {
                                type: "rest",
                                modeKey: modeKey,
                                source: source,
                                requestUrl: plan.url,
                                promise: promise,
                        };
                }

                return {
                        type: "builtin",
                        modeKey: modeKey,
                        source: source,
                };
        }

        global.loadNotesForSong = loadNotesForSong;
})(this);
