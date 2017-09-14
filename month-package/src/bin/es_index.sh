#!/bin/sh
yesterday=`date -d '-1 day' +%Y-%m-%d`
esIp=11.110.248.31
createIndexAndType(){
curl -XPOST ${esIp}:9200/path_of_often_index${yesterday} -d '{
    "settings" : { "number_of_shards" : 10,"number_of_replicas" : 2 },
    "mappings" : {
            "heze" : {
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
     curl -XPOST http://${esIp}:9200/_aliases -d '
{
    "actions" : [
        { "add" : { "index" : "path_of_often_index'${yesterday}'", "alias" : "path_index" } }
    ]
}'
}

createType(){
    curl -XPOST ${esIp}:9200/path_of_often_index/heze${yesterday}/_mapping -d '{
            "heze'${yesterday}'" : {
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
beforeYesterday=`date -d '-2 day' +%Y-%m-%d`
deleteType(){
curl -XDELETE http://${esIp}:9200/path_of_often_index${beforeYesterday}
}
case $1 in
    createIndexAndType)
        createIndexAndType
        ;;
    createType)
        createType
        ;;
    deleteType)
        deleteType
        ;;
    *)
        echo $"Usage: $0 {createIndexAndType|createType|deleteType}"
        exit 1
esac

exit 0

