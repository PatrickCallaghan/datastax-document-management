create keyspace if not exists tika WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use tika; 

create table if not exists metadata (
documentid text PRIMARY KEY,
created_date timestamp,
last_modified timestamp,
version double, 
encrypted boolean,
content_type text,
tags list<text>,
link text, 
type text,
content text,
md_ map<text,text>
);

CREATE SEARCH INDEX ON tika.metadata;

ALTER SEARCH INDEX SCHEMA ON tika.metadata ADD types.fieldtype[@class='org.apache.solr.schema.TextField', @name='MyTextField'] WITH '{"analyzer":{"tokenizer":{"class":"solr.StandardFilterFactory"}, "filter":[{"class": "solr.LowerCaseFilterFactory"}]}}';

ALTER SEARCH INDEX SCHEMA ON tika.metadata SET field[@name='content']@type='MyTextField';

RELOAD SEARCH INDEX ON tika.metadata ;

REBUILD SEARCH INDEX ON tika.metadata ;