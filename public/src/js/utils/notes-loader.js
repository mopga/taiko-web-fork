const globalObject = typeof window !== "undefined" ? window : (typeof globalThis !== "undefined" ? globalThis : this);

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
        if(globalObject.modesHelper && globalObject.modesHelper.manifest){
                const manifest = globalObject.modesHelper.manifest;
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

function resolveModeKey(song, selection){
        const raw = (selection && selection.mode ? String(selection.mode) : "").toLowerCase();
        if(raw === "tower" || raw === "dandojo"){
                return raw;
        }

        const charts = Array.isArray(song && song.charts) ? song.charts : [];
        for(let i = 0; i < charts.length; i++){
                const chart = charts[i] || {};
                const m = (chart.mode || chart.display_course || "").toLowerCase();
                if(m === "tower"){
                        return "tower";
                }
                if(m === "dandojo" || m === "dan"){
                        return "dandojo";
                }
        }

        const metaMode = (song && (song.default_mode || song.mode) ? String(song.default_mode || song.mode) : "").toLowerCase();
        if(metaMode === "tower"){
                return "tower";
        }
        if(metaMode === "dandojo" || metaMode === "dan"){
                return "dandojo";
        }

        const idx = (typeof window !== "undefined" && window.__modes__ && window.__modes__.categoryIndex) ? window.__modes__.categoryIndex : getCategoryIndex();
        const catTitle = (song && song.category_title ? String(song.category_title) : "").toLowerCase().trim();
        if(idx[catTitle] === "tower"){
                return "tower";
        }
        if(idx[catTitle] === "dandojo"){
                return "dandojo";
        }

        const cat = (selection && selection.category ? String(selection.category) : song && song.category ? String(song.category) : "").toLowerCase().trim();
        if(idx[cat] === "tower"){
                return "tower";
        }
        if(idx[cat] === "dandojo"){
                return "dandojo";
        }

        const detected = detectModeForSong(song);
        if(detected === "tower" || detected === "dandojo"){
                return detected;
        }

        return "standard";
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

function convertMeasuresToEngineEvents(measures){
        const events = [];
        const list = Array.isArray(measures) ? measures : [];
        let acc = 0;

        for(let i = 0; i < list.length; i++){
                const measure = list[i] || {};
                const measureStart = typeof measure.start_ms === "number" ? measure.start_ms : acc;
                const baseStart = Number.isFinite(measureStart) ? measureStart : acc;
                const notes = Array.isArray(measure.notes) ? measure.notes : [];
                const longs = Array.isArray(measure.longs) ? measure.longs : [];
                const balloons = Array.isArray(measure.balloons) ? measure.balloons : [];
                const scroll = measure.scroll;
                const gogo = Object.prototype.hasOwnProperty.call(measure, "gogotime") ? measure.gogotime : measure.gogo;

                for(let j = 0; j < notes.length; j++){
                        const note = notes[j] || {};
                        const offsetValue = Object.prototype.hasOwnProperty.call(note, "at") ? note.at : note.offset;
                        const offset = coerceNumber(offsetValue, 0) || 0;
                        const time = baseStart + Math.max(0, offset);
                        const rawType = note.type != null ? String(note.type).toLowerCase() : "";
                        const rawKind = note.kind != null ? String(note.kind).toLowerCase() : "";
                        const numericKind = note.kind;
                        const isBalloon = rawType === "balloon" || rawType === "balloons" || rawType === "balloonnote";
                        if(isBalloon){
                                const hitsValue = note.hits != null ? note.hits : (note.target != null ? note.target : note.required_hits);
                                const hits = coerceNumber(hitsValue, 5) || 5;
                                const balloonEvent = {type: "balloon", time: time, hits: Math.max(1, Math.round(hits))};
                                if(scroll != null){
                                        balloonEvent.scroll = scroll;
                                }
                                if(gogo != null){
                                        balloonEvent.gogotime = !!gogo;
                                }
                                events.push(balloonEvent);
                                continue;
                        }
                        const kindToken = rawType || rawKind;
                        const isKa = numericKind === 2 || kindToken === "ka" || kindToken === "katsu" || kindToken === "kat";
                        const shortEvent = {type: isKa ? "ka" : "don", time: time};
                        if(scroll != null){
                                shortEvent.scroll = scroll;
                        }
                        if(gogo != null){
                                shortEvent.gogotime = !!gogo;
                        }
                        events.push(shortEvent);
                }

                for(let j = 0; j < longs.length; j++){
                        const longNote = longs[j] || {};
                        const offsetValue = Object.prototype.hasOwnProperty.call(longNote, "at") ? longNote.at : longNote.offset;
                        const offset = coerceNumber(offsetValue, 0) || 0;
                        const startTime = baseStart + Math.max(0, offset);
                        const endAt = longNote.end_at != null ? coerceNumber(longNote.end_at, 0) : null;
                        const len = longNote.len_ms != null ? coerceNumber(longNote.len_ms, 0) : (longNote.len != null ? coerceNumber(longNote.len, 0) : coerceNumber(longNote.length_ms, 0));
                        const endTime = endAt != null ? baseStart + Math.max(endAt, offset) : startTime + Math.max(0, len);
                        const hitsValue = longNote.hits != null ? longNote.hits : (longNote.required_hits != null ? longNote.required_hits : null);
                        const longTypeTokenRaw = longNote.type != null ? String(longNote.type) : (longNote.kind != null ? String(longNote.kind) : "");
                        const longTypeToken = longTypeTokenRaw.toLowerCase().replace(/\s+/g, "");
                        if(longTypeToken === "balloon" || longTypeToken === "balloons" || longTypeToken === "balloonnote"){
                                const balloonEvent = {
                                        type: "balloon",
                                        time: startTime,
                                        hits: Math.max(1, Math.round(coerceNumber(hitsValue, 5) || 5)),
                                };
                                if(scroll != null){
                                        balloonEvent.scroll = scroll;
                                }
                                if(gogo != null){
                                        balloonEvent.gogotime = !!gogo;
                                }
                                events.push(balloonEvent);
                                continue;
                        }
                        const rollEvent = {
                                type: "roll",
                                time: startTime,
                                endTime: endTime,
                        };
                        if(hitsValue != null){
                                rollEvent.hits = Math.max(0, Math.round(coerceNumber(hitsValue, 0)));
                        }
                        if(scroll != null){
                                rollEvent.scroll = scroll;
                        }
                        if(gogo != null){
                                rollEvent.gogotime = !!gogo;
                        }
                        events.push(rollEvent);
                }

                for(let j = 0; j < balloons.length; j++){
                        const balloon = balloons[j] || {};
                        const offsetValue = Object.prototype.hasOwnProperty.call(balloon, "at") ? balloon.at : balloon.offset;
                        const offset = coerceNumber(offsetValue, 0) || 0;
                        const time = baseStart + Math.max(0, offset);
                        const hitsValue = balloon.hits != null ? balloon.hits : (balloon.target != null ? balloon.target : balloon.required_hits);
                        const balloonEvent = {
                                type: "balloon",
                                time: time,
                                hits: Math.max(1, Math.round(coerceNumber(hitsValue, 5) || 5)),
                        };
                        if(scroll != null){
                                balloonEvent.scroll = scroll;
                        }
                        if(gogo != null){
                                balloonEvent.gogotime = !!gogo;
                        }
                        events.push(balloonEvent);
                }

                const duration = coerceNumber(measure.duration_ms, 0) || 0;
                acc = baseStart + duration;
        }

        events.sort((a, b) => {
                const aTime = a.time != null ? a.time : (a.timeMs != null ? a.timeMs : 0);
                const bTime = b.time != null ? b.time : (b.timeMs != null ? b.timeMs : 0);
                if(aTime === bTime){
                        return 0;
                }
                return aTime - bTime;
        });

        return events;
}

function transformMeasuresToEvents(measures, durationHint){
        const events = convertMeasuresToEngineEvents(measures);
        let durationMs = coerceNumber(durationHint, null);
        if(durationMs === null || durationMs === undefined || !Number.isFinite(durationMs) || durationMs < 0){
                durationMs = computeDurationFromEvents(events);
        }
        return {notes: events, durationMs: durationMs};
}

function computeDurationFromEvents(events){
        if(!Array.isArray(events) || !events.length){
                return 0;
        }
        let maxTime = 0;
        for(let i = 0; i < events.length; i++){
                const entry = events[i];
                if(!entry || typeof entry !== "object"){
                        continue;
                }
                const start = typeof entry.time === "number" ? entry.time : (typeof entry.timeMs === "number" ? entry.timeMs : null);
                const end = typeof entry.endTime === "number" ? entry.endTime : null;
                if(start !== null && Number.isFinite(start) && start > maxTime){
                        maxTime = start;
                }
                if(end !== null && Number.isFinite(end) && end > maxTime){
                        maxTime = end;
                }
        }
        return maxTime >= 0 ? Math.max(0, Math.round(maxTime)) : 0;
}

function computeTotalNotes(events){
        if(!Array.isArray(events) || !events.length){
                return 0;
        }
        let total = 0;
        for(let i = 0; i < events.length; i++){
                const entry = events[i];
                if(!entry || typeof entry !== "object"){
                        continue;
                }
                const type = entry.type || null;
                if(type === "don" || type === "ka" || type === "daiDon" || type === "daiKa"){
                        total++;
                }else if(type === undefined && typeof entry.timeMs === "number"){
                        total++;
                }
        }
        return total;
}

function applyResultToContext(result, context){
        if(!result){
                return result;
        }
        if(context && context !== globalObject && typeof context === "object"){
                const events = Array.isArray(result.notes) ? result.notes : [];
                let durationMs = typeof result.durationMs === "number" ? result.durationMs : computeDurationFromEvents(events);
                if(!Number.isFinite(durationMs) || durationMs < 0){
                        durationMs = 0;
                }
                let totalNotes = result.meta && result.meta.totalNotes != null ? result.meta.totalNotes : computeTotalNotes(events);
                if(!Number.isFinite(totalNotes) || totalNotes < 0){
                        totalNotes = events.length;
                }
                context.songData = events;
                context.durationMs = durationMs;
                context.totalNotes = totalNotes;
        }
        return result;
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
                txt: (globalObject.strings && globalObject.strings.note && globalObject.strings.note[config.type]) || config.type,
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
        if(typeof globalObject.Circle === "function"){
                const circle = new globalObject.Circle(config);
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
                isPlayed: 0,
                animating: false,
                animT: 0,
                score: 0,
                lastFrame: config.start + 100,
                animationEnded: false,
                timesHit: 0,
                timesKa: 0,
                rendaPlayed: false,
                gogoChecked: false,
                fixedPos: false,
                animate(ms){
                        this.animating = true;
                        this.animT = ms;
                },
                played(score, big){
                        this.score = score;
                        this.isPlayed = score <= 0 ? score - 1 : (big ? 2 : 1);
                },
                hit(keysKa){
                        this.timesHit++;
                        if(keysKa){
                                this.timesKa++;
                        }
                },
        };
}

function convertMeasuresToParsedChart(payload, context){
        if(!payload || typeof payload !== "object"){
                return null;
        }
        const chartData = payload && typeof payload.chart_data === "object" ? payload.chart_data : payload;
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
                        const length = coerceNumber(longEntry && (longEntry.len_ms || longEntry.length_ms), 0) || 0;
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
        if(typeof globalObject.AutoScore === "function"){
                const autoscore = new globalObject.AutoScore(difficulty, stars, 2, circles);
                parsedChart.scoremode = 2;
                parsedChart.scoreinit = autoscore.ScoreInit;
                parsedChart.scorediff = autoscore.ScoreDiff;
        }else{
                parsedChart.scoremode = 2;
                parsedChart.scoreinit = 0;
                parsedChart.scorediff = 0;
        }

        const meta = {
                mode: context ? context.modeKey : null,
                course: chartData.course || selection.difficulty || selection.course || null,
                totalNotes: chartData.total_notes || circles.length,
        };

        return {
                chart: parsedChart,
                durationMs: durationMs,
                meta: meta,
        };
}

function buildFallbackParsedChart(events){
        if(!Array.isArray(events)){
                return null;
        }
        const circles = [];
        let id = 0;
        events.forEach(event => {
                if(!event || typeof event !== "object"){
                        return;
                }
                const time = typeof event.time === "number" ? event.time : (typeof event.timeMs === "number" ? event.timeMs : null);
                if(time === null){
                        return;
                }
                const kind = event.kind === 2 || event.type === "ka" || event.type === "daiKa" ? 2 : 1;
                const type = kind === 2 ? "ka" : "don";
                id++;
                const circleCfg = buildCircleConfig({
                        id: id,
                        ms: time,
                        type: type,
                        speed: 1,
                        endTime: time,
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
        const qp = new URLSearchParams();
        const title = selection && selection.title ? selection.title : song && song.title ? song.title : "";
        if(!title){
                return "";
        }

        qp.set("title", title);

        if(modeKey === "tower"){
                const charts = Array.isArray(song && song.charts) ? song.charts : [];
                const courseFromCharts = charts.find(chart => {
                        const token = (chart && (chart.mode || chart.display_course) ? String(chart.mode || chart.display_course) : "").toLowerCase();
                        return token.indexOf("tower") !== -1 || token === "tower";
                });
                const course = selection && selection.course ? selection.course : (courseFromCharts && (courseFromCharts.course || courseFromCharts.difficulty || courseFromCharts.level || courseFromCharts.rank)) || "oni";
                qp.set("course", String(course));
                qp.set("mode", "tower");
                return "/api/tower/chart?" + qp.toString();
        }

        if(modeKey === "dandojo"){
                const charts = Array.isArray(song && song.charts) ? song.charts : [];
                const danChart = charts.find(chart => {
                        const token = (chart && (chart.mode || chart.display_course) ? String(chart.mode || chart.display_course) : "").toLowerCase();
                        return token === "dandojo" || token === "dan";
                });
                let rank = selection && selection.rank !== undefined ? selection.rank : undefined;
                if(rank === undefined || rank === null || String(rank).trim() === ""){
                        rank = song && song.rank !== undefined ? song.rank : undefined;
                }
                if((rank === undefined || rank === null || String(rank).trim() === "") && danChart){
                        rank = danChart.rank;
                }
                if(rank === undefined || rank === null || String(rank).trim() === ""){
                        return "";
                }
                qp.set("rank", String(rank));
                qp.set("mode", "dandojo");
                return "/api/dan/chart?" + qp.toString();
        }

        return "";
}

function normalizeChartResponse(resp){
        const cd = resp && typeof resp === "object" && resp.chart_data ? resp.chart_data : resp;
        if(!cd || typeof cd !== "object" || !Array.isArray(cd.measures)){
                return null;
        }
        return {
                measures: cd.measures,
                duration_ms: cd.duration_ms !== undefined ? cd.duration_ms : null,
                total_notes: cd.total_notes !== undefined ? cd.total_notes : null,
                course: cd.course !== undefined ? cd.course : null,
        };
}

function registerRestNotesLoader(Loader){
        if(!Loader || !Loader.prototype){
                console.warn("[notes-loader] Loader is not ready");
                return;
        }

        const loadNotesForSong = async function loadNotesForSong(songMeta, selection){
                const song = songMeta || {};
                const currentSelection = Object.assign({}, selection || {});
                const context = this && typeof this === "object" ? this : null;
                if(!currentSelection.title){
                        currentSelection.title = song.title || song.originalTitle || song.id || "";
                }

                const modeKey = resolveModeKey(song, currentSelection);
                if(modeKey !== "tower" && modeKey !== "dandojo"){
                        return null;
                }

                currentSelection.mode = modeKey;

                const charts = Array.isArray(song && song.charts) ? song.charts : [];
                if(modeKey === "tower"){
                        let course = currentSelection.course || currentSelection.difficulty || null;
                        if(course === null || course === undefined || String(course).trim() === ""){
                                const towerChart = charts.find(chart => {
                                        const token = (chart && (chart.mode || chart.display_course) ? String(chart.mode || chart.display_course) : "").toLowerCase();
                                        return token === "tower" || token.indexOf("tower") !== -1;
                                });
                                if(towerChart){
                                        course = towerChart.course || towerChart.difficulty || towerChart.level || towerChart.rank || null;
                                }
                        }
                        if(course === null || course === undefined || String(course).trim() === ""){
                                course = "oni";
                        }
                        currentSelection.course = course;
                        if(!currentSelection.difficulty){
                                currentSelection.difficulty = course;
                        }
                }else{
                        let rank = currentSelection.rank;
                        if(rank === undefined || rank === null || String(rank).trim() === ""){
                                rank = song.rank;
                        }
                        if((rank === undefined || rank === null || String(rank).trim() === "") && charts.length){
                                const danChart = charts.find(chart => {
                                        const token = (chart && (chart.mode || chart.display_course) ? String(chart.mode || chart.display_course) : "").toLowerCase();
                                        return token === "dandojo" || token === "dan";
                                });
                                if(danChart && danChart.rank !== undefined && danChart.rank !== null && String(danChart.rank).trim() !== ""){
                                        rank = danChart.rank;
                                }
                        }
                        if(rank !== undefined && rank !== null && String(rank).trim() !== ""){
                                currentSelection.rank = rank;
                        }
                }

                const url = buildRestUrl(modeKey, song, currentSelection);
                if(!url){
                        return null;
                }

                const now = Date.now();
                const cached = notesCache.get(url);
                if(cached && cached.expires > now){
                        return cached.promise.then(result => applyResultToContext(result, context));
                }

                const promise = fetchJsonWithCache(url).then(json => {
                        const normalized = normalizeChartResponse(json);
                        if(!normalized){
                                return null;
                        }

                        const events = convertMeasuresToEngineEvents(normalized.measures);
                        if(!events.length){
                                return null;
                        }

                        let durationMs = coerceNumber(normalized.duration_ms, null);
                        if(durationMs === null){
                                durationMs = computeDurationFromEvents(events);
                        }

                        let totalNotes = coerceNumber(normalized.total_notes, null);
                        if(totalNotes === null){
                                totalNotes = computeTotalNotes(events);
                        }

                        const result = {
                                modeKey: modeKey,
                                notes: events,
                                durationMs: durationMs,
                                rest: true,
                                meta: {
                                        mode: modeKey,
                                        totalNotes: totalNotes,
                                        course: normalized.course || currentSelection.course || currentSelection.rank || null,
                                },
                        };

                        const parserPayload = {chart_data: {measures: normalized.measures, duration_ms: normalized.duration_ms, total_notes: normalized.total_notes, course: normalized.course}};
                        const parsed = convertMeasuresToParsedChart(parserPayload, {modeKey: modeKey, selection: currentSelection});
                        if(parsed && parsed.chart){
                                result.parsedChart = parsed.chart;
                                if(parsed.durationMs && (!result.durationMs || result.durationMs < parsed.durationMs)){
                                        result.durationMs = parsed.durationMs;
                                }
                                if(parsed.meta){
                                        result.meta = Object.assign({}, result.meta, parsed.meta);
                                        if(result.meta.mode == null){
                                                result.meta.mode = modeKey;
                                        }
                                }
                        }else{
                                const fallbackChart = buildFallbackParsedChart(events);
                                if(fallbackChart){
                                        result.parsedChart = fallbackChart;
                                }
                        }

                        if(result.meta.totalNotes == null){
                                result.meta.totalNotes = computeTotalNotes(events);
                        }
                        if(result.meta.course == null){
                                result.meta.course = normalized.course || currentSelection.course || currentSelection.rank || null;
                        }

                        if(result.durationMs == null){
                                result.durationMs = computeDurationFromEvents(events);
                        }

                        if(context && context !== globalObject && typeof context === "object"){
                                context.songData = events;
                                context.durationMs = result.durationMs;
                                context.totalNotes = result.meta.totalNotes;
                        }

                        return result;
                }).catch(error => {
                        console.debug("[notes] REST fetch failed", {
                                mode: modeKey,
                                url: url,
                                error: error && error.message ? error.message : error,
                        });
                        return null;
                });

                notesCache.set(url, {promise: promise, expires: now + REST_CACHE_TTL});
                promise.then(value => {
                        if(value === null){
                                notesCache.delete(url);
                        }
                });

                return promise.then(result => applyResultToContext(result, context));
        };

        Loader.prototype.loadNotesForSong = loadNotesForSong;

        if(globalObject){
                globalObject.loadNotesForSong = loadNotesForSong;
                globalObject.notesLoader = {
                        loadNotesForSong: loadNotesForSong,
                        detectModeForSong: detectModeForSong,
                        transformMeasuresToEvents: transformMeasuresToEvents,
                        convertMeasuresToEngineEvents: convertMeasuresToEngineEvents,
                };
        }
}

if(globalObject){
        globalObject.registerRestNotesLoader = registerRestNotesLoader;
        const queue = globalObject.__restNotesLoaderRegistrations__;
        if(Array.isArray(queue)){
                while(queue.length){
                        const fn = queue.shift();
                        try{
                                fn();
                        }catch(error){
                                console.warn("[notes-loader] deferred registration failed", error);
                        }
                }
        }
}
