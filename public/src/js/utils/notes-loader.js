;(function(global){
        const DEFAULT_MODE_KEY = "standard";
        const REST_CACHE_TTL = 12000;
        const httpCache = new Map();
        const notesCache = new Map();

        function normaliseToken(value){
                if(typeof value !== "string"){
                        return "";
                }
                return value.trim().toLowerCase();
        }

        function canonicalModeKey(raw){
                const token = normaliseToken(raw);
                if(!token){
                        return DEFAULT_MODE_KEY;
                }
                if(token === "dan" || token === "dojo" || token === "dandojo"){
                        return "dandojo";
                }
                if(token === "tower" || token === "towers"){
                        return "tower";
                }
                if(token === "std" || token === "default"){
                        return DEFAULT_MODE_KEY;
                }
                return token;
        }

        function buildCategoryIndex(manifest){
                const index = {};
                if(!manifest || typeof manifest !== "object"){
                        return index;
                }
                const modes = Array.isArray(manifest.modes) ? manifest.modes : [];
                modes.forEach(entry => {
                        if(!entry || typeof entry !== "object"){
                                return;
                        }
                        const key = canonicalModeKey(entry.key || entry.mode);
                        if(!key){
                                return;
                        }
                        const categories = Array.isArray(entry.categories) ? entry.categories : [];
                        categories.forEach(category => {
                                if(typeof category === "string" && category.trim()){
                                        index[normaliseToken(category)] = key;
                                }
                        });
                });
                return index;
        }

        function getModesStore(){
                if(typeof window === "undefined"){
                        return null;
                }
                const store = window.__modes__;
                if(store && typeof store === "object" && store.manifest && !store.categoryIndex){
                        store.categoryIndex = buildCategoryIndex(store.manifest);
                }
                return store || null;
        }

        function getCategoryIndex(){
                const store = getModesStore();
                if(store && store.categoryIndex){
                        return store.categoryIndex;
                }
                if(global.modesHelper && global.modesHelper.manifest){
                        const manifest = global.modesHelper.manifest;
                        if(!manifest.__categoryIndex){
                                manifest.__categoryIndex = buildCategoryIndex(manifest);
                        }
                        return manifest.__categoryIndex;
                }
                return {};
        }

        function detectModeForSong(songMeta){
                const song = songMeta || {};
                const canonicalModes = [];
                const seen = new Set();
                const rawModes = Array.isArray(song.modes) ? song.modes : [];
                rawModes.forEach(mode => {
                        const canonical = canonicalModeKey(mode);
                        if(canonical && !seen.has(canonical)){
                                canonicalModes.push(canonical);
                                seen.add(canonical);
                        }
                });
                const defaultMode = canonicalModeKey(song.default_mode);
                if(canonicalModes.length){
                        if(defaultMode && seen.has(defaultMode)){
                                return defaultMode;
                        }
                        return canonicalModes[0];
                }
                if(defaultMode && defaultMode !== DEFAULT_MODE_KEY){
                        return defaultMode;
                }
                const fallbackMode = canonicalModeKey(song.mode);
                if(fallbackMode && fallbackMode !== DEFAULT_MODE_KEY){
                        return fallbackMode;
                }
                const category = song.category || song.category_title;
                if(category){
                        const mapped = getCategoryIndex()[normaliseToken(category)];
                        if(mapped){
                                return canonicalModeKey(mapped);
                        }
                }
                return DEFAULT_MODE_KEY;
        }

        function coerceNumber(value, fallback){
                if(value === null || value === undefined){
                        return fallback;
                }
                const number = Number(value);
                if(Number.isFinite(number)){
                        return number;
                }
                return fallback;
        }

        function fetchJsonWithCache(url){
                const now = Date.now();
                const existing = httpCache.get(url);
                if(existing && existing.expires > now){
                        return existing.promise;
                }
                const promise = fetch(url, {credentials: "same-origin"}).then(response => {
                        if(!response.ok){
                                throw new Error(url + " (" + response.status + ")");
                        }
                        return response.json();
                });
                httpCache.set(url, {promise: promise, expires: now + REST_CACHE_TTL});
                promise.catch(() => {
                        httpCache.delete(url);
                });
                return promise;
        }

        function noteKindFromEntry(note){
                if(!note || typeof note !== "object"){
                        return null;
                }
                const explicit = coerceNumber(note.kind, null);
                if(explicit === 1 || explicit === 2){
                        return explicit;
                }
                const typeToken = normaliseToken(note.type);
                if(typeToken === "ka" || typeToken === "katsu" || typeToken === "kat"){
                        return 2;
                }
                if(typeToken === "don" || typeToken === "dong" || typeToken === "do"){
                        return 1;
                }
                return null;
        }

        function transformMeasuresToEvents(measures, durationHint){
                const list = Array.isArray(measures) ? measures : [];
                const events = [];
                let durationMs = coerceNumber(durationHint, 0) || 0;
                let maxTime = durationMs;

                list.forEach(measure => {
                        const startMs = coerceNumber(measure && measure.start_ms, 0) || 0;
                        const duration = coerceNumber(measure && measure.duration_ms, 0) || 0;
                        if(startMs + duration > maxTime){
                                maxTime = startMs + duration;
                        }
                        const notes = Array.isArray(measure && measure.notes) ? measure.notes : [];
                        notes.forEach(entry => {
                                const offsetValue = entry && Object.prototype.hasOwnProperty.call(entry, "at") ? entry.at : entry && entry.offset;
                                const offset = coerceNumber(offsetValue, 0) || 0;
                                const absolute = startMs + Math.max(0, offset);
                                const kind = noteKindFromEntry(entry);
                                const resolvedKind = kind === 2 ? 2 : 1;
                                events.push({timeMs: Math.round(absolute), kind: resolvedKind});
                                if(absolute > maxTime){
                                        maxTime = absolute;
                                }
                        });
                });

                events.sort((a, b) => {
                        if(a.timeMs === b.timeMs){
                                return a.kind - b.kind;
                        }
                        return a.timeMs - b.timeMs;
                });

                if(maxTime > durationMs){
                        durationMs = Math.round(maxTime);
                }
                if(durationMs < 0){
                        durationMs = 0;
                }

                return {notes: events, durationMs: durationMs};
        }

        function noteTypeFromEntry(entry){
                const rawType = normaliseToken(entry && entry.type ? String(entry.type) : "");
                const kind = coerceNumber(entry && entry.kind, null);
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
                if(!entry || typeof entry !== "object"){
                        return null;
                }
                const rawKind = entry.kind != null ? String(entry.kind) : "";
                const rawType = entry.type != null ? String(entry.type) : "";
                const sizeToken = normaliseToken(entry.size);
                let token = normaliseToken(rawType) || normaliseToken(rawKind);
                token = token.replace(/\s+/g, "");
                let isBig = entry.big === true || sizeToken === "big";
                if(token.startsWith("dai")){
                        isBig = true;
                        token = token.slice(3);
                }
                if(token.startsWith("big")){
                        isBig = true;
                        token = token.slice(3);
                }
                if(token.endsWith("big")){
                        isBig = true;
                        token = token.slice(0, -3);
                }
                if(token === "balloon" || token === "balloons" || token === "balloonnote"){
                        return "balloon";
                }
                if(token === "drumroll" || token === "roll" || token === "renda" || token === "ren" || token === "drum"){
                        return isBig ? "daiDrumroll" : "drumroll";
                }
                if(token === "taiko" && isBig){
                        return "daiDrumroll";
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

        function createCircleFromConfig(config){
                if(typeof global.Circle === "function"){
                        const circle = new global.Circle(config);
                        return circle;
                }
                return {
                        id: config.id,
                        start: config.start,
                        ms: config.start,
                        originalMS: config.start,
                        type: config.type,
                        txt: config.txt,
                        speed: config.speed,
                        gogoTime: config.gogoTime,
                        endTime: config.endTime,
                        originalEndTime: config.endTime,
                        requiredHits: config.requiredHits,
                        beatMS: config.beatMS,
                        section: config.section,
                        branch: null,
                };
        }

        function convertMeasuresToParsedChart(payload, context){
                if(!payload || typeof payload !== "object"){
                        return null;
                }
                const chartData = payload.chart_data || {};
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
                                const offsetValue = note && Object.prototype.hasOwnProperty.call(note, "at") ? note.at : note && note.offset;
                                const offset = coerceNumber(offsetValue, 0) || 0;
                                const absolute = startMs + Math.max(0, offset);
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
                                const circle = createCircleFromConfig(circleCfg);
                                circles.push(circle);
                                if(circle.ms === undefined){
                                        circle.ms = circle.start;
                                }
                                if(circle.originalMS === undefined){
                                        circle.originalMS = circle.ms;
                                }
                                if(circle.originalEndTime === undefined){
                                        circle.originalEndTime = circle.endTime;
                                }
                                if(earliestMs === null || circle.ms < earliestMs){
                                        earliestMs = circle.ms;
                                }
                        });

                        const longs = Array.isArray(measure && measure.longs) ? measure.longs : [];
                        longs.forEach(longEntry => {
                                const offsetSource = longEntry && Object.prototype.hasOwnProperty.call(longEntry, "at") ? longEntry.at : longEntry && longEntry.offset;
                                const offset = coerceNumber(offsetSource, 0) || 0;
                                const length = coerceNumber(longEntry && longEntry.len_ms, 0) || 0;
                                const endAt = coerceNumber(longEntry && longEntry.end_at, null);
                                const absolute = startMs + Math.max(0, offset);
                                const endMs = endAt !== null ? startMs + Math.max(endAt, offset) : absolute + Math.max(0, length);
                                const type = longTypeFromEntry(longEntry);
                                if(!type){
                                        return;
                                }
                                circleId++;
                                const requiredHitsValue = coerceNumber(longEntry && (longEntry.hits || longEntry.required_hits), type === "balloon" ? 1 : 0) || 0;
                                const circleCfg = buildCircleConfig({
                                        id: circleId,
                                        ms: absolute,
                                        type: type,
                                        speed: speed,
                                        endTime: endMs,
                                        requiredHits: type === "balloon" ? Math.max(1, requiredHitsValue) : Math.max(0, requiredHitsValue),
                                        beatMS: beatMS,
                                        section: offset === 0 && notes.length === 0,
                                });
                                const circle = createCircleFromConfig(circleCfg);
                                circles.push(circle);
                                if(circle.ms === undefined){
                                        circle.ms = circle.start;
                                }
                                if(circle.originalMS === undefined){
                                        circle.originalMS = circle.ms;
                                }
                                if(circle.originalEndTime === undefined){
                                        circle.originalEndTime = circle.endTime;
                                }
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

                let durationMs = coerceNumber(chartData.duration_ms, 0) || 0;
                if(durationMs <= 0){
                        const lastCircle = circles[circles.length - 1];
                        if(lastCircle){
                                durationMs = Math.max(lastCircle.endTime || lastCircle.ms, lastCircle.ms);
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
                const difficulty = selection.difficulty || selection.course || "oni";
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
                                course: chartData.course || selection.difficulty || selection.course || null,
                                totalNotes: chartData.total_notes || circles.length,
                        },
                };
        }

        function buildFallbackParsedChart(events){
                if(!Array.isArray(events)){
                        return null;
                }
                const circles = [];
                let id = 0;
                events.forEach(event => {
                        if(!event || typeof event.timeMs !== "number"){
                                return;
                        }
                        const kind = event.kind === 2 ? 2 : 1;
                        const type = kind === 2 ? "ka" : "don";
                        id++;
                        const circleCfg = buildCircleConfig({
                                id: id,
                                ms: event.timeMs,
                                type: type,
                                speed: 1,
                                endTime: event.timeMs,
                                requiredHits: 0,
                                beatMS: 600,
                                section: id === 1,
                        });
                        const circle = createCircleFromConfig(circleCfg);
                        if(circle.ms === undefined){
                                circle.ms = circle.start;
                                circle.originalMS = circle.start;
                        }
                        if(circle.originalEndTime === undefined){
                                circle.originalEndTime = circle.endTime;
                        }
                        circles.push(circle);
                });
                circles.sort((a, b) => a.ms - b.ms);
                return {
                        circles: circles,
                        measures: [],
                        events: [],
                        branches: null,
                        beatInfo: {beatInterval: 600},
                        soundOffset: 0,
                        scoremode: 2,
                        scoreinit: 0,
                        scorediff: 0,
                };
        }

        function buildRestUrl(modeKey, song, selection){
                const params = new URLSearchParams();
                const title = selection.title || song.title || song.originalTitle || song.id || "";
                if(title){
                        params.set("title", title);
                }
                if(modeKey === "tower"){
                        const course = selection.course || selection.difficulty || "oni";
                        params.set("course", course);
                        params.set("mode", "tower");
                        return {url: "/api/tower/chart?" + params.toString()};
                }
                if(modeKey === "dandojo"){
                        const rank = selection.rank || selection.dan || 1;
                        params.set("rank", String(rank));
                        params.set("mode", "dan");
                        return {url: "/api/dan/chart?" + params.toString()};
                }
                return {url: ""};
        }

        function loadNotesForSong(songMeta, selection){
                const song = songMeta || {};
                const currentSelection = Object.assign({}, selection || {});
                if(!currentSelection.title){
                        currentSelection.title = song.title || song.originalTitle || song.id || "";
                }
                const modeKey = detectModeForSong(song);
                if(modeKey === DEFAULT_MODE_KEY){
                        return Promise.resolve(null);
                }
                const plan = buildRestUrl(modeKey, song, currentSelection);
                if(!plan.url){
                        return Promise.resolve(null);
                }
                const now = Date.now();
                const cached = notesCache.get(plan.url);
                if(cached && cached.expires > now){
                        return cached.promise;
                }

                const promise = fetchJsonWithCache(plan.url).then(json => {
                        if(!json || typeof json !== "object" || json.status !== "ok"){
                                const status = json && json.status ? json.status : "error";
                                throw new Error("status_" + status);
                        }
                        const chartData = json.chart_data || {};
                        const measures = Array.isArray(chartData.measures) ? chartData.measures : [];
                        const transformed = transformMeasuresToEvents(measures, chartData.duration_ms);
                        const result = {
                                modeKey: modeKey,
                                notes: transformed.notes,
                                durationMs: transformed.durationMs,
                        };
                        const parsed = convertMeasuresToParsedChart(json, {modeKey: modeKey, selection: currentSelection}) || {};
                        if(parsed.chart){
                                result.parsedChart = parsed.chart;
                                if(parsed.durationMs && (!result.durationMs || result.durationMs < parsed.durationMs)){
                                        result.durationMs = parsed.durationMs;
                                }
                                if(parsed.meta){
                                        result.meta = parsed.meta;
                                }
                        }else{
                                const fallbackChart = buildFallbackParsedChart(transformed.notes);
                                if(fallbackChart){
                                        result.parsedChart = fallbackChart;
                                }
                                result.meta = {
                                        mode: modeKey,
                                        course: chartData.course || currentSelection.course || currentSelection.rank || null,
                                        totalNotes: transformed.notes.length,
                                };
                        }
                        if(!result.meta){
                                result.meta = {
                                        mode: modeKey,
                                        course: chartData.course || currentSelection.course || currentSelection.rank || null,
                                        totalNotes: chartData.total_notes || transformed.notes.length,
                                };
                        }
                        if(result.durationMs < 0){
                                result.durationMs = 0;
                        }
                        console.info("notes-loader:", modeKey, result.notes.length, plan.url);
                        return result;
                }).catch(error => {
                        console.warn("notes-loader:", modeKey, "fallback", plan.url, error && error.message ? error.message : error);
                        return null;
                });

                notesCache.set(plan.url, {promise: promise, expires: now + REST_CACHE_TTL});
                promise.then(value => {
                        if(value === null){
                                notesCache.delete(plan.url);
                        }
                });
                return promise;
        }

        global.loadNotesForSong = loadNotesForSong;
        global.notesLoader = {
                loadNotesForSong: loadNotesForSong,
                detectModeForSong: detectModeForSong,
                transformMeasuresToEvents: transformMeasuresToEvents,
        };
})(this);
