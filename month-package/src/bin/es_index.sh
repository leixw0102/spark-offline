#!/bin/sh
currentDay=`date +%Y-%m-%d`
esIp=10.150.27.248
createIndexAndType(){
curl -XPOST ${esIp}:9200/path_of_often_index -d '{
    "settings" : { "number_of_shards" : 5,"number_of_replicas" : 2 },
    "mappings" : {
            "heze${currentDay}" : {
                     "properties" : {
                            "id" : { "type" : "string", "index" : "not_analyzed" } ,
                             "numb" : { "type" : "string", "index" : "not_analyzed" } ,
                             "plate_type" : { "type" : "integer", "index" : "not_analyzed" } ,
                             "cids" : { "type" : "string", "index" : "not_analyzed" } ,
                             "start" : { "type" : "string", "index" : "not_analyzed" } ,
                             "end" : { "type" : "string", "index" : "not_analyzed" } ,
                             "num" : { "type" : "integer", "index" : "not_analyzed" }
                            }
                     }
             }
     }'
}

createType(){
    curl -XPOST ${esIp}:9200/path_of_often_index/heze${currentDay}/_mapping -d '{
            "heze'${currentDay}'" : {
                 "properties" : {
                        "id" : { "type" : "string", "index" : "not_analyzed" } ,
                         "numb" : { "type" : "string", "index" : "not_analyzed" } ,
                         "plate_type" : { "type" : "integer", "index" : "not_analyzed" } ,
                         "cids" : { "type" : "string", "index" : "not_analyzed" } ,
                         "start" : { "type" : "string", "index" : "not_analyzed" } ,
                         "end" : { "type" : "string", "index" : "not_analyzed" } ,
                         "num" : { "type" : "integer", "index" : "not_analyzed" }
                 }
            }
     }'
}
