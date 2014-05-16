var request = require('request');
var lastScrollId = null;
var totalSearchResults = 0;

var elasticsearchScan = function (parent, baseUrl) {
    this.baseUrl = baseUrl;
    this.parent = parent;
}

// accept callback
// return (error, arr) where arr is an array of objects
elasticsearchScan.prototype.get = function (limit, offset, callback) {
    var searchBody, searchRequest, self;
    self = this;

    if (offset >= totalSearchResults && totalSearchResults != 0) {
        callback(null, []);
        return;
    }

    if (lastScrollId !== null) {
        scrollResultSet(self, callback);
    } else {

        searchBody = {
            "query": {
                "match_all": {}
            },
            "size": limit
        };

        searchRequest = {
            "uri": self.baseUrl + "/" + "_search?search_type=scan&scroll=" + self.parent.options.scrollTime,
            "method": "GET",
            "body": JSON.stringify(searchBody)
        };

        request.get(searchRequest, function requestResonse (err, response) {
            if(response.statusCode != 200 && err == null){
                err = new Error(response.body);
                callback(err, []);
                return;
            }

            var body = JSON.parse(response.body);
            lastScrollId = body._scroll_id;
            totalSearchResults = body.hits.total;

            scrollResultSet(self, callback);
        });
    }
}

// accept arr, callback where arr is an array of objects
// return (error, writes)
elasticsearchScan.prototype.set = function (data, limit, offset, callback) {
    var self = this;
    var error = null;

    callback(error);
}

exports.elasticsearchScan = elasticsearchScan;

/**
 * Posts requests to the _search api to fetch the latest
 * scan result with scroll id
 * @param callback
 */
function scrollResultSet(that, callback) {
    var self = that;

    scrollRequest = {
        "uri": self.parent.options.searchUrl + "/scroll?scroll=" + self.parent.options.scrollTime,
        "method": "POST",
        "body": lastScrollId
    };

    request.get(scrollRequest, function requestResonse (err, response) {
        if(response.statusCode != 200 && err == null){
            err = new Error(response.body);
            callback(err, []);
            return;
        }

        var body = JSON.parse(response.body);
        lastScrollId = body._scroll_id;
        var hits = body.hits.hits;

        if(self.parent.options.delete === true && hits.length > 0){
            var stated = 0;
            hits.forEach(function(elem){
                stated++;
                self.del(elem, function(){
                    if(stated === hits.length){
                        self.reindex(function(err){
                            callback(err, hits);
                        });
                    }
                });
            });
        } else {
            callback(err, hits);
        }
    });
}


elasticsearchScan.prototype.del = function(elem, callback){
    var self = this;
    var thisUrl = self.baseUrl + "/" + encodeURIComponent(elem._type) + "/" + encodeURIComponent(elem._id);

    self.parent.emit('debug', 'deleteUrl: ' + thisUrl);

    request.del(thisUrl, function(err, response, body){
        if(typeof callback == 'function'){ callback(err, response, body); }
    });
}

elasticsearchScan.prototype.reindex = function(callback){
    var self = this;
    request.post(self.baseUrl + "/_refresh", function(err, response){
        callback(err, response);
    });
}