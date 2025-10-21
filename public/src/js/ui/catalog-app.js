(function(global){
        "use strict";

        function initialise(){
                if(!global.document){
                        return;
                }
                var container = global.document.querySelector(".catalog-app");
                if(!container){
                        return;
                }
                var dataSource = new global.SongCatalogDataSource({
                        pageSize: 120,
                        maxConcurrency: 6,
                        detailBatchSize: 50,
                        hardPageCap: 5,
                        minPageCap: 5,
                });
                var view = new global.SongCatalogView({
                        container: container,
                        dataSource: dataSource,
                        estimatedItemHeight: 140,
                        bufferScreens: 12,
                        maxRender: 80,
                        onNeedMore: function(){
                                if(dataSource.hasMore()){
                                        fetchNextPage();
                                }
                        },
                });

                function fetchNextPage(){
                        if(!dataSource.hasMore()){
                                view.setLoading(false);
                                return;
                        }
                        view.setError(null);
                        view.setLoading(true);
                        dataSource.loadNextPage().then(function(items){
                                if(view.items && view.items.length){
                                        view.appendItems(items);
                                }else{
                                        view.updateItems(dataSource.getItems());
                                }
                        }).catch(function(error){
                                console.error("Failed to load songs page", error);
                                view.setError("Failed to load songs: " + (error && error.message ? error.message : "unknown error"));
                        }).finally(function(){
                                view.setLoading(false);
                        });
                }

                fetchNextPage();
        }

        if(document.readyState === "loading"){
                document.addEventListener("DOMContentLoaded", initialise);
        }else{
                initialise();
        }
})(this);
