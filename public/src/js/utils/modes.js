;(function(global){
        const DEFAULT_MODE_KEY = "standard";
        const DEFAULT_MODE_DEFINITION = {
                key: DEFAULT_MODE_KEY,
                label: "Standard",
                notes_source: {type: "builtin", format: "engine-v1"},
        };

        const fallbackModesByKey = {
                [DEFAULT_MODE_KEY]: DEFAULT_MODE_DEFINITION,
        };

        function normaliseToken(value){
                if(typeof value !== "string"){
                        return "";
                }
                return value.trim().toLowerCase();
        }

        function canonicalModeKey(rawKey, modesByKey){
                const token = normaliseToken(rawKey);
                if(!token){
                        return DEFAULT_MODE_KEY;
                }
                if(modesByKey && modesByKey[token]){
                        return modesByKey[token].key;
                }
                if(token === "dan" || token === "dojo"){
                        return modesByKey && modesByKey["dandojo"] ? modesByKey["dandojo"].key : "dandojo";
                }
                if(token === "towers"){
                        return modesByKey && modesByKey["tower"] ? modesByKey["tower"].key : "tower";
                }
                return token;
        }

        const registry = {
                manifest: null,
                modesByKey: Object.assign({}, fallbackModesByKey),
                categoryModes: {},
        };

        function rebuildCategoryMapFromManifest(){
                registry.categoryModes = registry.categoryModes || {};
                if(!registry.manifest || !Array.isArray(registry.manifest.modes)){
                        return;
                }
                registry.manifest.modes.forEach(entry => {
                        if(!entry || typeof entry !== "object"){
                                return;
                        }
                        const modeKey = canonicalModeKey(entry.key || entry.mode, registry.modesByKey);
                        const categories = Array.isArray(entry.categories) ? entry.categories : [];
                        categories.forEach(title => {
                                if(typeof title === "string" && title.trim()){
                                        registry.categoryModes[normaliseToken(title)] = modeKey;
                                }
                        });
                });
        }

        function updateManifest(manifest){
                if(
                        !manifest ||
                        typeof manifest !== "object" ||
                        (manifest.status && manifest.status !== "ok")
                ){
                        registry.manifest = null;
                        registry.modesByKey = Object.assign({}, fallbackModesByKey);
                        registry.categoryModes = {};
                        return;
                }
                registry.manifest = manifest;
                registry.modesByKey = Object.assign({}, fallbackModesByKey);
                if(Array.isArray(manifest.modes)){
                        manifest.modes.forEach(entry => {
                                if(!entry || typeof entry !== "object"){
                                        return;
                                }
                                const key = canonicalModeKey(entry.key || entry.mode, registry.modesByKey);
                                registry.modesByKey[normaliseToken(key)] = Object.assign({}, entry, {key});
                        });
                }
                rebuildCategoryMapFromManifest();
        }

        function registerCategories(categories){
                if(!Array.isArray(categories)){
                        return;
                }
                categories.forEach(category => {
                        if(!category || typeof category !== "object"){
                                return;
                        }
                        const title = typeof category.title === "string" && category.title.trim() ? category.title.trim() : (typeof category.name === "string" ? category.name.trim() : "");
                        if(!title){
                                return;
                        }
                        const modeTokens = [];
                        if(category.mode){
                                modeTokens.push(category.mode);
                        }
                        if(category.mode_key){
                                modeTokens.push(category.mode_key);
                        }
                        if(Array.isArray(category.modes)){
                                category.modes.forEach(value => modeTokens.push(value));
                        }
                        const manifestMode = registry.categoryModes[normaliseToken(title)];
                        if(manifestMode && !modeTokens.length){
                                registry.categoryModes[normaliseToken(title)] = manifestMode;
                                return;
                        }
                        const chosen = modeTokens.map(token => canonicalModeKey(token, registry.modesByKey)).find(Boolean);
                        if(chosen){
                                registry.categoryModes[normaliseToken(title)] = chosen;
                        }
                });
        }

        function getModeDefinition(modeKey){
                const key = canonicalModeKey(modeKey, registry.modesByKey);
                const definition = registry.modesByKey[normaliseToken(key)];
                if(definition){
                        return definition;
                }
                return fallbackModesByKey[DEFAULT_MODE_KEY];
        }

        function modeForCategory(title){
                if(!title){
                        return DEFAULT_MODE_KEY;
                }
                const mapped = registry.categoryModes[normaliseToken(title)];
                if(mapped){
                        return canonicalModeKey(mapped, registry.modesByKey);
                }
                return DEFAULT_MODE_KEY;
        }

        function resolveSongMode(songMeta, selection){
                const song = songMeta || {};
                const pickList = [];
                if(selection && selection.mode){
                        pickList.push(selection.mode);
                }
                if(song.selectedMode){
                        pickList.push(song.selectedMode);
                }
                if(song.default_mode){
                        pickList.push(song.default_mode);
                }
                if(song.mode){
                        pickList.push(song.mode);
                }
                if(Array.isArray(song.modes)){
                        song.modes.forEach(value => pickList.push(value));
                }
                if(song.category){
                        pickList.push(modeForCategory(song.category));
                }

                let resolvedKey = null;
                for(let i = 0; i < pickList.length; i++){
                        const candidate = canonicalModeKey(pickList[i], registry.modesByKey);
                        if(candidate){
                                resolvedKey = candidate;
                                break;
                        }
                }
                if(!resolvedKey){
                        resolvedKey = DEFAULT_MODE_KEY;
                }
                const definition = getModeDefinition(resolvedKey);
                return {
                        modeKey: definition.key,
                        definition: definition,
                };
        }

        function enrichSongMetadata(song){
                if(!song || typeof song !== "object"){
                        return song;
                }
                const modes = Array.isArray(song.modes) ? song.modes.slice() : [];
                if(song.mode && !modes.length){
                        modes.push(song.mode);
                }
                if(song.default_mode && !modes.length){
                        modes.push(song.default_mode);
                }
                if(song.category){
                        const categoryMode = modeForCategory(song.category);
                        if(categoryMode){
                                modes.push(categoryMode);
                        }
                }
                if(!modes.length){
                        modes.push(DEFAULT_MODE_KEY);
                }
                const canonicalModes = [];
                const seen = new Set();
                modes.forEach(rawMode => {
                        const key = canonicalModeKey(rawMode, registry.modesByKey);
                        if(key && !seen.has(key)){
                                canonicalModes.push(key);
                                seen.add(key);
                        }
                });
                if(!canonicalModes.length){
                        canonicalModes.push(DEFAULT_MODE_KEY);
                }
                const defaultMode = canonicalModeKey(song.default_mode || canonicalModes[0], registry.modesByKey);
                song.modes = canonicalModes;
                song.default_mode = defaultMode;
                song.mode = defaultMode;
                return song;
        }

        global.modesHelper = {
                updateManifest,
                registerCategories,
                resolveSongMode,
                getModeDefinition,
                modeForCategory,
                enrichSongMetadata,
                get manifest(){
                        return registry.manifest;
                },
        };
})(this);
