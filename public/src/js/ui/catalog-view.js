(function(global){
        "use strict";

        var DEFAULT_ESTIMATED_HEIGHT = 132;
        var DEFAULT_BUFFER_SCREENS = 12;
        var DEFAULT_MAX_RENDER = 80;

        function clamp(value, min, max){
                return Math.min(Math.max(value, min), max);
        }

        function formatDuration(ms){
                if(!Number.isFinite(ms) || ms <= 0){
                        return "";
                }
                var totalSeconds = Math.round(ms / 1000);
                var minutes = Math.floor(totalSeconds / 60);
                var seconds = totalSeconds % 60;
                return minutes + ":" + String(seconds).padStart(2, "0");
        }

        function createDifficultyList(detail){
                var list = document.createElement("ul");
                list.className = "catalog-card__difficulties";
                var courses = detail && detail.courses;
                if(!courses || typeof courses !== "object"){
                        return list;
                }
                ["easy", "normal", "hard", "oni", "ura"].forEach(function(course){
                        var data = courses[course];
                        var item = document.createElement("li");
                        item.className = "catalog-card__difficulty";
                        item.dataset.difficulty = course;
                        if(data && typeof data === "object"){
                                var stars = Number(data.stars);
                                if(Number.isFinite(stars) && stars > 0){
                                        item.textContent = course + ": " + stars + "★";
                                }else{
                                        item.textContent = course;
                                }
                        }else{
                                item.textContent = course;
                                item.classList.add("catalog-card__difficulty--missing");
                        }
                        list.appendChild(item);
                });
                return list;
        }

        function SongCatalogCard(item){
                this.id = item ? item.id : null;
                this.detailLoaded = false;
                this.element = document.createElement("article");
                this.element.className = "catalog-card";
                this.element.dataset.songId = this.id || "";

                this.header = document.createElement("header");
                this.header.className = "catalog-card__header";

                this.titleEl = document.createElement("h3");
                this.titleEl.className = "catalog-card__title";

                this.subtitleEl = document.createElement("div");
                this.subtitleEl.className = "catalog-card__subtitle";

                this.metaEl = document.createElement("div");
                this.metaEl.className = "catalog-card__meta";

                this.previewIndicator = document.createElement("span");
                this.previewIndicator.className = "catalog-card__preview";
                this.previewIndicator.title = "Preview";

                this.durationEl = document.createElement("span");
                this.durationEl.className = "catalog-card__duration";

                this.metaEl.appendChild(this.previewIndicator);
                this.metaEl.appendChild(this.durationEl);

                this.bodyEl = document.createElement("div");
                this.bodyEl.className = "catalog-card__body";

                this.difficultiesHost = document.createElement("div");
                this.difficultiesHost.className = "catalog-card__difficulties-host";

                this.bodyEl.appendChild(this.difficultiesHost);

                this.footer = document.createElement("footer");
                this.footer.className = "catalog-card__footer";

                this.categoryEl = document.createElement("span");
                this.categoryEl.className = "catalog-card__category";

                this.footer.appendChild(this.categoryEl);

                this.element.appendChild(this.header);
                this.element.appendChild(this.bodyEl);
                this.element.appendChild(this.footer);

                this.header.appendChild(this.titleEl);
                this.header.appendChild(this.subtitleEl);
                this.header.appendChild(this.metaEl);

                this.updateSummary(item);
        }

        SongCatalogCard.prototype.updateSummary = function(item){
                if(!item || typeof item !== "object"){
                        return;
                }
                this.id = item.id;
                this.element.dataset.songId = this.id || "";
                var title = item.title || "";
                this.titleEl.textContent = title;
                var subtitle = item.subtitle || "";
                if(subtitle){
                        this.subtitleEl.textContent = subtitle;
                        this.subtitleEl.style.display = "block";
                }else{
                        this.subtitleEl.textContent = "";
                        this.subtitleEl.style.display = "none";
                }
                var previewAvailable = item.preview_available === true;
                this.previewIndicator.dataset.available = previewAvailable ? "true" : "false";
                this.previewIndicator.textContent = previewAvailable ? "🎧" : "";
                var duration = formatDuration(item.duration_ms);
                this.durationEl.textContent = duration;
                this.categoryEl.textContent = item.category || "";
        };

        SongCatalogCard.prototype.applyDetail = function(detail){
                if(!detail || typeof detail !== "object"){
                        return;
                }
                this.detailLoaded = true;
                var previewAvailable = detail.preview_available === true;
                this.previewIndicator.dataset.available = previewAvailable ? "true" : "false";
                this.previewIndicator.textContent = previewAvailable ? "🎧" : "";
                var duration = formatDuration(detail.duration_ms);
                if(duration){
                        this.durationEl.textContent = duration;
                }
                while(this.difficultiesHost.firstChild){
                        this.difficultiesHost.removeChild(this.difficultiesHost.firstChild);
                }
                this.difficultiesHost.appendChild(createDifficultyList(detail));
        };

        SongCatalogCard.prototype.getHeight = function(){
                return this.element.getBoundingClientRect().height;
        };

        function SongCatalogView(options){
                options = options || {};
                this.container = options.container;
                this.dataSource = options.dataSource;
                this.onNeedMore = typeof options.onNeedMore === "function" ? options.onNeedMore : function(){};
                this.onDetailLoaded = typeof options.onDetailLoaded === "function" ? options.onDetailLoaded : function(){};
                this.estimatedHeight = Number.isFinite(options.estimatedItemHeight) && options.estimatedItemHeight > 0 ? options.estimatedItemHeight : DEFAULT_ESTIMATED_HEIGHT;
                this.bufferScreens = Number.isFinite(options.bufferScreens) && options.bufferScreens > 0 ? options.bufferScreens : DEFAULT_BUFFER_SCREENS;
                this.maxRender = Number.isFinite(options.maxRender) && options.maxRender > 0 ? options.maxRender : DEFAULT_MAX_RENDER;

                this.scroller = this.container.querySelector(".catalog-scroll");
                this.topSpacer = this.container.querySelector(".catalog-spacer--top");
                this.itemsHost = this.container.querySelector(".catalog-items");
                this.bottomSpacer = this.container.querySelector(".catalog-spacer--bottom");
                this.loadingEl = this.container.querySelector(".catalog-loading");
                this.errorEl = this.container.querySelector(".catalog-error");
                this.emptyEl = this.container.querySelector(".catalog-empty");

                this.items = [];
                this.renderedRange = {start: 0, end: 0};
                this.cardCache = new Map();
                this.activeCards = new Map();
                this.pendingLoadMore = false;

                this.handleScroll = this.handleScroll.bind(this);
                this.handleResize = this.handleResize.bind(this);
                this.handleIntersection = this.handleIntersection.bind(this);

                this.observer = new IntersectionObserver(this.handleIntersection, {
                        root: this.scroller,
                        threshold: 0.1,
                });

                this.scroller.addEventListener("scroll", this.handleScroll);
                window.addEventListener("resize", this.handleResize);
        }

        SongCatalogView.prototype.setLoading = function(isLoading){
                if(this.loadingEl){
                        this.loadingEl.style.display = isLoading ? "flex" : "none";
                }
        };

        SongCatalogView.prototype.setError = function(message){
                if(!this.errorEl){
                        return;
                }
                if(message){
                        this.errorEl.textContent = message;
                        this.errorEl.style.display = "block";
                }else{
                        this.errorEl.style.display = "none";
                }
        };

        SongCatalogView.prototype.setEmpty = function(isEmpty){
                if(this.emptyEl){
                        this.emptyEl.style.display = isEmpty ? "block" : "none";
                }
        };

        SongCatalogView.prototype.ensureCard = function(item){
                if(!item || typeof item !== "object"){
                        return null;
                }
                var id = item.id;
                var card = this.cardCache.get(id);
                if(card){
                        card.updateSummary(item);
                        return card;
                }
                card = new SongCatalogCard(item);
                this.cardCache.set(id, card);
                var detail = this.dataSource ? this.dataSource.getDetailFromCache(id) : null;
                if(detail){
                        card.applyDetail(detail);
                }
                return card;
        };

        SongCatalogView.prototype.handleIntersection = function(entries){
                var _this = this;
                entries.forEach(function(entry){
                        if(!entry.isIntersecting){
                                return;
                        }
                        var target = entry.target;
                        var id = target && target.dataset ? target.dataset.songId : null;
                        if(!id){
                                return;
                        }
                        if(!_this.dataSource || typeof _this.dataSource.queueDetail !== "function"){
                                return;
                        }
                        _this.dataSource.queueDetail(id).then(function(detail){
                                if(!detail){
                                        return;
                                }
                                var card = _this.cardCache.get(id);
                                if(card){
                                        card.applyDetail(detail);
                                }
                                _this.onDetailLoaded(detail);
                        }).catch(function(){
                                // ignore detail errors for UI purposes
                        });
                });
        };

        SongCatalogView.prototype.updateItems = function(items){
                if(!Array.isArray(items)){
                        items = [];
                }
                this.items = items.slice();
                this.render(true);
                this.setEmpty(this.items.length === 0);
        };

        SongCatalogView.prototype.appendItems = function(items){
                if(!Array.isArray(items) || !items.length){
                        this.render(false);
                        return;
                }
                Array.prototype.push.apply(this.items, items);
                this.render(false);
                this.setEmpty(this.items.length === 0);
        };

        SongCatalogView.prototype.handleResize = function(){
                this.render(false);
        };

        SongCatalogView.prototype.handleScroll = function(){
                this.render(false);
                if(this.shouldLoadMore()){
                        this.onNeedMore();
                }
        };

        SongCatalogView.prototype.shouldLoadMore = function(){
                if(!this.dataSource || typeof this.dataSource.hasMore !== "function"){
                        return false;
                }
                if(!this.dataSource.hasMore()){
                        return false;
                }
                var scrollTop = this.scroller.scrollTop;
                var viewportHeight = this.scroller.clientHeight;
                var totalHeight = this.items.length * this.estimatedHeight;
                if(totalHeight <= 0){
                        return false;
                }
                return scrollTop + viewportHeight >= totalHeight - this.estimatedHeight * 2;
        };

        SongCatalogView.prototype.render = function(force){
                var viewportHeight = this.scroller.clientHeight;
                if(viewportHeight <= 0){
                        viewportHeight = this.estimatedHeight * 6;
                }
                var visibleCount = Math.ceil(viewportHeight / this.estimatedHeight);
                var buffer = Math.max(visibleCount * this.bufferScreens, visibleCount * 2);
                var scrollTop = this.scroller.scrollTop;
                var startIndex = Math.floor(scrollTop / this.estimatedHeight) - buffer;
                var endIndex = Math.ceil((scrollTop + viewportHeight) / this.estimatedHeight) + buffer;
                startIndex = clamp(startIndex, 0, this.items.length);
                endIndex = clamp(endIndex, 0, this.items.length);
                if(endIndex - startIndex > this.maxRender){
                        endIndex = startIndex + this.maxRender;
                }
                if(!force && startIndex === this.renderedRange.start && endIndex === this.renderedRange.end){
                        return;
                }
                this.renderRange(startIndex, endIndex);
        };

        SongCatalogView.prototype.renderRange = function(start, end){
                var _this = this;
                this.renderedRange = {start: start, end: end};
                var fragment = document.createDocumentFragment();
                var activeKeys = new Set();

                for(var i = start; i < end; i++){
                        var item = this.items[i];
                        if(!item){
                                continue;
                        }
                        var card = this.ensureCard(item);
                        if(!card){
                                continue;
                        }
                        activeKeys.add(item.id);
                        fragment.appendChild(card.element);
                        this.observer.observe(card.element);
                }

                this.itemsHost.innerHTML = "";
                this.itemsHost.appendChild(fragment);

                var topHeight = start * this.estimatedHeight;
                var bottomHeight = Math.max((this.items.length - end) * this.estimatedHeight, 0);
                this.topSpacer.style.height = topHeight + "px";
                this.bottomSpacer.style.height = bottomHeight + "px";

                this.activeCards.forEach(function(card, key){
                        if(!activeKeys.has(key)){
                                _this.observer.unobserve(card.element);
                                _this.activeCards.delete(key);
                        }
                });
                activeKeys.forEach(function(key){
                        var card = _this.cardCache.get(key);
                        if(card){
                                _this.activeCards.set(key, card);
                        }
                });

                this.updateEstimatedHeight();
        };

        SongCatalogView.prototype.updateEstimatedHeight = function(){
                if(!this.itemsHost.children.length){
                        return;
                }
                var total = 0;
                var count = 0;
                for(var i = 0; i < this.itemsHost.children.length; i++){
                        var child = this.itemsHost.children[i];
                        var height = child.getBoundingClientRect().height;
                        if(Number.isFinite(height) && height > 0){
                                total += height;
                                count++;
                        }
                }
                if(count){
                        var average = total / count;
                        if(Number.isFinite(average) && average > 0){
                                this.estimatedHeight = Math.round(average);
                        }
                }
        };

        SongCatalogView.prototype.destroy = function(){
                this.scroller.removeEventListener("scroll", this.handleScroll);
                window.removeEventListener("resize", this.handleResize);
                this.observer.disconnect();
        };

        global.SongCatalogView = SongCatalogView;
})(this);
