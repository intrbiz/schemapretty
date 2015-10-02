SchemaPretty
==========
A quick a dirty pretty PostgreSQL schema dumper

SchemaPretty will dump (the important bits) of a PostgreSQL schema 
into a pretty directory structure, which is nice for using with 
SCM.

The idea is to dump each entity as a separate file, which is then 
really easy to manage changes using SCMs such as git.

Usage
-----

You can run schemapretty as follows:

    java -jar schemapretty-0.0.1.app '<out_dir>' '<url>' '<username>' '<password>'


Author
------
Chris Ellis

Twitter: @intrbiz

Web: intrbiz.com

Copyright (c) Chris Ellis 2015