/*
Package search implements a database text search engine appropriate to IMQS data.

Concepts

In a nutshell, the indexer reads strings from the tables that are being indexed. These strings
are then split into tokens. The tokens are inserted into one giant search index table.
When searching, we run the same tokenizer on the search query. These tokens that are produced
out of the search query are then used to perform a prefix match against all of the tokens in
the index. Because our tokens are fairly unique, we maintain reasonably logarithmic growth
in query time, as our index grows.

Query Strings

The "Find" API accepts a single string. That string is tokenized, and those tokens are
searched for inside our index. Generally, a search query is just a plain old string. However,
there are two special instructions that you can embed inside this string. Those instructions
are (by example)

	regular search terms <fields:db1.roads.streetname,db2.addresses.address>

The first example, above, illustrates using a "fields:" instruction to restrict the fields that are searched.

The lists within the special instructions are separated by a comma. A pipe character (|) can be
used as an escape character. You need to escape commas and the greater than symbol. An instruction may
appear only once - ie you may not have more than one <fields:...> instruction.

The 'fields' restriction limits the source fields that you want to search in. If you don't
specify a 'fields' restriction, then all fields are searched.

The second instruction available is the "pairs" instruction:

	regular search terms <pairs:db1.pipes.type=COPPER,db1.pipes.type=BRASS,db2.sealer.material=COPPER>

In the above example, we have specified three key-value pairs. Key-value pairs are matched in an OR manner.
In other words, if any of the key-value pairs match, then the record is considered a legal result.
Key-value pairs must match the entire string (ie not just a prefix match), but they are case insensitive.

To perform a 'pairs' prefix match instruction use '=~':

	regular search terms <pairs:db1.pipes.type=~COPP,db1.pipes.type=~BRA,db2.sealer.material=COPPER>

In the above example, we have specified three key-value pairs, the first two of which allow prefix matching.
I.e. db1.pipes.type fields will match the pattern COPP% and db1.pipes.type will match BRA% while
db2.sealer.material must match COPPER exactly. Note again that all matches are case insensitive.

Ambiguity caused by 'fields' and 'pairs' instructions:

There is inherent ambiguity if one specifies both 'fields' and 'pairs' instructions. For example, consider
the following table:

	diameter  type   length
	     150    FH       30
	     100     V       30
	     110     V       50

Now imagine this search query

	110 <pairs:db1.pipes.type=V> <fields:db1.pipes.diameter>

The user is looking for the string "110", provided the record found has the property "type=V". The query string
explicitly says to look inside the diameter field, where we find 110, so all is well.

Now consider this query

	110 <pairs:db1.pipes.type=V>

The user said that he wants to search for the string "110", provided the record found has the property "type=V".
However, this IMPLIES that he wants to limit his search to the table "pipes", because the pair constraint can
only be satisfied by records from the pipes table. So, the implication of this is that the user wants to search
in all indexed fields inside the pipes table. But now, if he specifies additional fields to be searched in, do
we still want to search through all fields in the pipes table? What if he specifies fields from other tables,
but no fields from "pipes"? Does that mean that he wants to search in all fields of pipes, or in no fields of
pipes? There doesn't seem to be an obvious answer.

How do we treat this ambiguity?
The rules are as follows:

	For each 'pairs' instruction:
	    If there is at least one 'fields' instruction for a field
	     belonging to the same table as the 'pairs'
	     instruction, then do not add any new 'fields' instructions.
	    Else (ie there are zero 'fields' instructions for that table),
	     add all fields of that table, as though they had been
	     explicitly specified with 'fields' instructions.

One more thing that should be said about pairs: If a search contains only <pairs> instructions, and no tokens,
then the result set contains all records that match the <pairs> constraints. No result ranking is performed
in this case, because there is nothing on which to rank.

Table Relationships

This system supports joining tables together by specifying relationships between them. For example,
imagine a table called Sausages, and another table called Farms. Assume every sausage is made with meat from
only one farm. Every farm has properties such as "organic", "free range". If the user wants to search for
"free range pork", then our system needs to read "free range" from the Farms table, and "pork" from the Sausages
table, and somehow combine that information to produce a higher score for all sausages which are pork, and
which contain meat from free range farms. This implies that the ranking comes not from a single table, but
from multiple tables, after joining their records together.

Table Relationship Internals

To illustrate how the table joining system works, we can start by describing how one configures related tables.
Firstly, we express relationships between tables by saying that a table is related to another table with a
OneToOne, OneToMany, or ManyToOne relationship. OneToOne is it's own opposite. If table A is related
to table B with OneToOne, then B is also related to A with OneToOne. The opposite of OneToMany is ManyToOne.
We do not require configuration to specify both ends of a relationship. We automatically infer
them after parsing the config. All OneToOnes get their opposite ends set to OneToOne, and likewise for the other
relationship types. The system refuses to load the configuration if relationships are in conflict.

Tables are only allowed to relate to each other through a single key. In graph terms, there may be at most one
edge between any two nodes.

Now that we have the inter-table relationships, we can think of them in terms of a graph structure.

For the remainer of this chapter, we will talk in terms of the following example graph:

	(A) --- (B)
	 |     /
	 |   /
	 | /
	(C) *--- (E)

In the above example, there are three tables A,B,C, which are all related to one another with a OneToOne relationship.
This kind of pattern appears in a database where you've taken one logical table and split it up into three
different tables, purely for the sake of keeping table widths down.

In addition, table C is related to E with a ManyToOne relationship. Think of E here as a lookup table. For example,
C might contain a Pipe Material code, and E is a lookup table that describes those materials. C is typically a table
with a lot of rows, and E has only a small number of rows.

One thing that we need to do here, before we can proceed, is to create some kind of hierarchy within this graph.
The reason we need to do this is for efficiency. When the user executes a find operation, and we scan our index,
it is pointless to have table A reference B and C, and B reference A and C, etc. Instead, all that is necessary
is that A references B and C, and C references E. This decision affects the design of the code that runs after
we have queried tokens out of the database. This code must manually join up the results, so that a keyword matched
from table A and a different keyword matched from table B both end up counting towards a final result. This brings
us to the question of how exactly we match records up during a find operation.

Firstly, it is important to note here that we have two different ID fields that we need to deal with. There is
the 'srcrow' field (usually 'rowid' in the database), which is a unique 64-bit integer field that must be present
in order for a table to be indexed. Secondly, there is the key field that is used by the relationship between
tables. These key fields can be strings, integers, blobs, or UUIDs. We do not wish to store these key fields in
the index, because they would bloat the index significantly. In addition, even if we did store them, we would be
reliant on many BTree lookups to find the actual records, from their keys. This kind of "random access" into the
database would completely break the kind of performance that we're looking for in this search engine.

The solution to this problem, is to compute all of the table joins up front, and store them inside the index.
So what exactly do we store? We store a tuple of 'srcrow' values. The first 'srcrow' is always the srcrow of
the table from which the token originated. Subsequent srcrow values in the tuple, are the rows onto which
this original row is joined. For any given table graph, we can compute a stable tuple set for every table,
so that the tuples stored inside the index are as compact as possible.

The tuple ordering is computed as follows.

Firstly, we need to form a hierarchy of tables. In a set of OneToOne relationships, like the A,B,C triangle
above, there is no clear "primary", "secondary", "tertiary" table. So we arbitrarily assign this hierarchy. We
do so in a manner that tries to produce stable results, so that if one makes a change to the search config,
which does not affect some other part of the index, then we don't need to rebuild that unaffected part of the
index. What we end up with is a strict ordering of tables. One could also refer to this as a "total ordering".
In our example above, imagine that table A has order 1, table B has order 2, table C has order 3, and
table E has order 4. In reality the order will be effectively random, but to keep our example simple,
we make our ordering here simple.

The srcrow tuples of any given table are:

	* The primary table that is being indexed
	* All related tables, in descending order, but only if:
	    * The relationship is OneToOne, and their order is higher than the primary table's order, or...
	    * The relationship is ManyToOne

The tuple set for the above example is:

	* A: A, B, C    B and C have higher order than A, so they are both included
	* B: B, C       C has higher order than B, so it is included
	* C: C, E       C to E is ManyToOne, to E is included
	* E: E          E to C is OneToMany, so C is not included

This 'tuple set' ends up in the search_index table, inside the 'srcrows' field, which is a binary blob
field, capable of storing any number of srcrow values. We encode the srcrow values using the
varint encoding (see https://developers.google.com/protocol-buffers/docs/encoding#varints), which
will encode values under 2 million in 3 bytes, and values below 260 million in 4 bytes. Before adding
support for relationships, we used a BIGINT (fixed 64-bit) field type, so adding support for joins
actually ended up shrinking typical index sizes, for the case where no relationships are specified.
<VERIFY THIS INDEX SIZE STATEMENT BEFORE COMMITTING>

When performing a find operation, we can quickly accumulate (aka join) the results from related tables.
We only need to walk down the tuple order. For example, if we have some results in A, and some in C,
then we walk the results of A, and every 'C' record that we find, we inspect, and merge it's token matches
into A's token matches. Likewise for C, we walk it's results, and add in matches from E. However, we
do not need to walk C, and match it's results for A, because this is symmetrical to the match that
we already did, when we walked A and matched up with C. With a single pass, we can match all relationships
that are directly related. With two passes, we can find relationships two steps away.
We could perform as many passes as we like, with each one increasing the distance of indirection, but
we need to see how far along the relationship tree it is practically meaningful to walk.

NOTE - The above description is no longer entirely true. When we store the tuples in search_index, we actually
store more than the strict set mentioned above. The rule that is relaxed here is that all OneToOne
records store all references to their counterparts. In other words, the restriction of "their order is
higher than the primary table's order" is removed. This is necessary in order to send back the full
list of records are related records from our API, regardless of whether the related records played a
part in the search ranking.

Autovacuum

We are forced by our history to run some servers on spinning disc hard drives. When the autovacuum decides to run
on a hard drive, it makes the server unusable. For this reason, we disable autovacuum, and instead
run vacuum on a 24-hour schedule, at night time. This won't be good enough forever, but for now
it's OK, since our server installations serve a particular demographic. Hopefully by the time
"nightly" runs are no longer sufficient, we will have moved off hard drives completely.

Postgres Table Statistics

Occasionally we see that Postgres table statistics are reset. It's not clear how this happens. The Postgres
documention mentions nothing about statistics being automatically reset, but there certainly are functions that
one can run manually to reset these statistics.

The result of the statistics being reset, is that our indexer will rebuild it's indexes for the tables
that have had their statistics reset. This is typically not a problem when our servers are running on solid
state drives, but it can produce a surprise system slowdown if it occurs on a hard drive machine.

Our best bet for why this happens occasionally is due to people backing up and restoring Postgres databases.

Single Digit Tokens

Normally, we enforce a minimum length on tokens, and this minimum length is 2 characters. Single character
tokens are discarded, and never end up in the index. However, when adding support for addresses, we realized
that street addresses are often single digits, and we need to support this. What we did here, was add support
into our config specification, which allows one to specify the minimum token length. For address fields,
we set this to 1. This works, but it's a bad solution, because it causes the index to bloat with gigantic
sets of numeric street number tokens.

What should we do instead?

A much better solution is to build domain-specific knowledge into our indexer and finder. Let's consider
the indexing of addresses as an example.

Firstly, instead of running addresses through the regular token parser, we build a special parser for
addresses. This parser comprehends all types of address formats, rewrites them in a canonical form,
and then adds them to the index.

When making this change, one would also want to introduce a new level into the index key. At present,
index keys are the following tuple: (token, srcfield, srcrow). We should change this, by adding a new
element to the front of the list, so that the new tuple is: (type, token, srcfield, srcrow). This would
allow one to cordon off searches so that they only hit the type that you're interested in. When receiving
a query, if we have any reason to believe it may be an address query, then we run it through the address
parser, and fetch results from the index.

Remember that our address parser does not store individual tokens. The address parser would probably store
tokens that look like "5 omega", "113 bluecrane", etc. This makes searches much more efficient, because
we are now indexing far more unique things, than if we were indexing "5", "113", etc.

One thing that we would need to do for something like address searches is spelling correction. Street names
could be just another thing that we index.

Performance

2015/09/15 Jeremy -
ExcludeEntireStringFromResult Functionality: This is used when inserting fields into the searchIndex table
Its value is read out of the search.json file(default is false). So what this does is(say we use value "I-74E"),
so previously the tokenized record would look like this : [i 74e]. With the ExcludeEntireStringFromResult = false
will include the entire value in the tokenized result looking like this : [i-74e i 74e] . The reason why we want
to do is so we can find records which contains single charaters in the database. This enable us search for
"i-74e" and "74e" and it will return result "I-74E" where it previously didnt.

See the stats below on what the impact will be on the searchIndex table(Sample data).

ExcludeEntireStringFromResult : true
Table Size			84 MB
Indexes Size		159 MB

ExcludeEntireStringFromResult : false
Table Size			126 MB
Indexes Size		236 MB

See the stats below for the search query(Sample data):

ExcludeEntireStringFromResult : true
Find(i-74e): 10 results in 93 ms true
Find(i-74e): 10 results in 91 ms true
Find(i-74e): 10 results in 95 ms true

ExcludeEntireStringFromResult : false
Find(i-74e): 10 results in 93 ms false
Find(i-74e): 10 results in 96 ms false
Find(i-74e): 10 results in 99 ms false
*/
package search
